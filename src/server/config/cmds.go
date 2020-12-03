package cmds

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sort"

	"github.com/pachyderm/pachyderm/src/client/pkg/config"
	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
	"github.com/pachyderm/pachyderm/src/client/pkg/grpcutil"
	"github.com/pachyderm/pachyderm/src/server/cmd/pachctl/shell"
	"github.com/pachyderm/pachyderm/src/server/pkg/cmdutil"

	prompt "github.com/c-bata/go-prompt"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/spf13/cobra"
)

const (
	listContextHeader = "ACTIVE\tNAME"
)

// Cmds returns a slice containing admin commands.
func Cmds() []*cobra.Command {
	marshaller := &jsonpb.Marshaler{
		Indent:   "  ",
		OrigName: true,
	}

	var commands []*cobra.Command

	getMetrics := &cobra.Command{
		Short: "Gets whether metrics are enabled.",
		Long:  "Gets whether metrics are enabled.",
		Run: cmdutil.Run(func(args []string) (retErr error) {
			cfg, err := config.Read(false)
			if err != nil {
				return err
			}
			fmt.Printf("%v\n", cfg.V2.Metrics)
			return nil
		}),
	}
	commands = append(commands, cmdutil.CreateAlias(getMetrics, "config get metrics"))

	setMetrics := &cobra.Command{
		Short: "Sets whether metrics are enabled.",
		Long:  "Sets whether metrics are enabled.",
		Run: cmdutil.RunFixedArgs(1, func(args []string) (retErr error) {
			metrics := true
			if args[0] == "false" {
				metrics = false
			} else if args[0] != "true" {
				return errors.New("invalid argument; use either `true` or `false`")
			}

			cfg, err := config.Read(false)
			if err != nil {
				return err
			}

			cfg.V2.Metrics = metrics
			return cfg.Write()
		}),
	}
	commands = append(commands, cmdutil.CreateAlias(setMetrics, "config set metrics"))

	getActiveContext := &cobra.Command{
		Short: "Gets the currently active context.",
		Long:  "Gets the currently active context.",
		Run: cmdutil.Run(func(args []string) (retErr error) {
			cfg, err := config.Read(false)
			if err != nil {
				return err
			}
			activeContext, _, err := cfg.ActiveContext(false)
			if err != nil {
				return err
			}
			if activeContext == "" {
				fmt.Println("NONE")
			} else {
				fmt.Printf("%s\n", activeContext)
			}
			return nil
		}),
	}
	commands = append(commands, cmdutil.CreateAlias(getActiveContext, "config get active-context"))

	setActiveContext := &cobra.Command{
		Short: "Sets the currently active context.",
		Long:  "Sets the currently active context.",
		Run: cmdutil.RunFixedArgs(1, func(args []string) (retErr error) {
			cfg, err := config.Read(false)
			if err != nil {
				return err
			}
			if _, ok := cfg.V2.Contexts[args[0]]; !ok {
				return errors.Errorf("context does not exist: %s", args[0])
			}
			cfg.V2.ActiveContext = args[0]
			return cfg.Write()
		}),
	}
	shell.RegisterCompletionFunc(setActiveContext, contextCompletion)
	commands = append(commands, cmdutil.CreateAlias(setActiveContext, "config set active-context"))

	getContext := &cobra.Command{
		Short: "Gets a context.",
		Long:  "Gets the config of a context by its name.",
		Run: cmdutil.RunFixedArgs(1, func(args []string) (retErr error) {
			cfg, err := config.Read(false)
			if err != nil {
				return err
			}

			context, ok := cfg.V2.Contexts[args[0]]
			if !ok {
				return errors.Errorf("context does not exist: %s", args[0])
			}

			if err = marshaller.Marshal(os.Stdout, context); err != nil {
				return err
			}

			fmt.Println()
			return nil
		}),
	}
	shell.RegisterCompletionFunc(getContext, contextCompletion)
	commands = append(commands, cmdutil.CreateAlias(getContext, "config get context"))

	var overwrite bool
	var kubeContextName string
	setContext := &cobra.Command{
		Short: "Set a context.",
		Long:  "Set a context config from a given name and either JSON stdin, or a given kubernetes context.",
		Run: cmdutil.RunFixedArgs(1, func(args []string) (retErr error) {
			name := args[0]

			cfg, err := config.Read(false)
			if err != nil {
				return err
			}

			if !overwrite {
				if _, ok := cfg.V2.Contexts[name]; ok {
					return errors.Errorf("context '%s' already exists, use `--overwrite` if you wish to replace it", args[0])
				}
			}

			var context config.Context
			if kubeContextName != "" {
				kubeConfig, err := config.RawKubeConfig()
				if err != nil {
					return err
				}

				kubeContext := kubeConfig.Contexts[kubeContextName]
				if kubeContext == nil {
					return errors.Errorf("kubernetes context does not exist: %s", kubeContextName)
				}

				context = config.Context{
					Source:      config.ContextSource_IMPORTED,
					ClusterName: kubeContext.Cluster,
					AuthInfo:    kubeContext.AuthInfo,
					Namespace:   kubeContext.Namespace,
				}
			} else {
				fmt.Println("Reading from stdin.")

				var buf bytes.Buffer
				var decoder *json.Decoder

				contextReader := io.TeeReader(os.Stdin, &buf)
				decoder = json.NewDecoder(contextReader)

				if err := jsonpb.UnmarshalNext(decoder, &context); err != nil {
					if errors.Is(err, io.EOF) {
						return errors.New("unexpected EOF")
					}
					return errors.Wrapf(err, "malformed context")
				}

				pachdAddress, err := grpcutil.ParsePachdAddress(context.PachdAddress)
				if err != nil {
					if !errors.Is(err, grpcutil.ErrNoPachdAddress) {
						return err
					}
				} else {
					context.PachdAddress = pachdAddress.Qualified()
				}
			}

			cfg.V2.Contexts[name] = &context
			return cfg.Write()
		}),
	}
	setContext.Flags().BoolVar(&overwrite, "overwrite", false, "Overwrite a context if it already exists.")
	setContext.Flags().StringVarP(&kubeContextName, "kubernetes", "k", "", "Import a given kubernetes context's values into the Pachyderm context.")
	shell.RegisterCompletionFunc(setContext, contextCompletion)
	commands = append(commands, cmdutil.CreateAlias(setContext, "config set context"))

	var pachdAddress string
	var clusterName string
	var authInfo string
	var serverCAs string
	var namespace string
	var removeClusterDeploymentID bool
	var updateContext *cobra.Command // standalone declaration so Run() can refer
	updateContext = &cobra.Command{
		Short: "Updates a context.",
		Long: "Updates an existing context config from a given name (or the " +
			"currently-active context, if no name is given).",
		Use: "{{alias}} [<context>]",
		Run: cmdutil.RunBoundedArgs(0, 1, func(args []string) (retErr error) {
			cfg, err := config.Read(false)
			if err != nil {
				return err
			}

			var context *config.Context
			if len(args) > 0 {
				var ok bool
				context, ok = cfg.V2.Contexts[args[0]]
				if !ok {
					return errors.Errorf("context does not exist: %s", args[0])
				}
			} else {
				var name string
				var err error
				name, context, err = cfg.ActiveContext(true)
				if err != nil {
					return err
				}
				fmt.Printf("editing the currently active context %q\n", name)
			}

			// Use this method since we want to differentiate between no
			// flag being set (the value shouldn't be changed) vs the flag
			// being an empty string (meaning we want to set the value to an
			// empty string)
			if updateContext.Flags().Changed("pachd-address") {
				parsedPachdAddress, err := grpcutil.ParsePachdAddress(pachdAddress)
				if err != nil {
					if errors.Is(err, grpcutil.ErrNoPachdAddress) {
						context.PachdAddress = ""
					} else {
						return err
					}
				} else {
					context.PachdAddress = parsedPachdAddress.Qualified()
				}
			}
			if updateContext.Flags().Changed("cluster-name") {
				context.ClusterName = clusterName
			}
			if updateContext.Flags().Changed("auth-info") {
				context.AuthInfo = authInfo
			}
			if updateContext.Flags().Changed("server-cas") {
				context.ServerCAs = serverCAs
			}
			if updateContext.Flags().Changed("namespace") {
				context.Namespace = namespace
			}
			if removeClusterDeploymentID {
				context.ClusterDeploymentID = ""
			}

			return cfg.Write()
		}),
	}
	updateContext.Flags().StringVar(&pachdAddress, "pachd-address", "", "Set a new name pachd address.")
	updateContext.Flags().StringVar(&clusterName, "cluster-name", "", "Set a new cluster name.")
	updateContext.Flags().StringVar(&authInfo, "auth-info", "", "Set a new k8s auth info.")
	updateContext.Flags().StringVar(&serverCAs, "server-cas", "", "Set new trusted CA certs.")
	updateContext.Flags().StringVar(&namespace, "namespace", "", "Set a new namespace.")
	updateContext.Flags().BoolVar(&removeClusterDeploymentID, "remove-cluster-deployment-id", false, "Remove the cluster deployment ID field, which will be repopulated on the next `pachctl` call using this context.")
	shell.RegisterCompletionFunc(updateContext, contextCompletion)
	commands = append(commands, cmdutil.CreateAlias(updateContext, "config update context"))

	deleteContext := &cobra.Command{
		Short: "Deletes a context.",
		Long:  "Deletes a context.",
		Run: cmdutil.RunFixedArgs(1, func(args []string) (retErr error) {
			cfg, err := config.Read(false)
			if err != nil {
				return err
			}
			if _, ok := cfg.V2.Contexts[args[0]]; !ok {
				return errors.Errorf("context does not exist: %s", args[0])
			}
			if cfg.V2.ActiveContext == args[0] {
				return errors.New("cannot delete an active context")
			}
			delete(cfg.V2.Contexts, args[0])
			return cfg.Write()
		}),
	}
	shell.RegisterCompletionFunc(deleteContext, contextCompletion)
	commands = append(commands, cmdutil.CreateAlias(deleteContext, "config delete context"))

	listContext := &cobra.Command{
		Short: "Lists contexts.",
		Long:  "Lists contexts.",
		Run: cmdutil.Run(func(args []string) (retErr error) {
			cfg, err := config.Read(false)
			if err != nil {
				return err
			}

			keys := make([]string, len(cfg.V2.Contexts))
			i := 0
			for key := range cfg.V2.Contexts {
				keys[i] = key
				i++
			}
			sort.Strings(keys)

			activeContext, _, err := cfg.ActiveContext(false)
			if err != nil {
				return err
			}

			fmt.Println(listContextHeader)
			for _, key := range keys {
				if key == activeContext {
					fmt.Printf("*\t%s\n", key)
				} else {
					fmt.Printf("\t%s\n", key)
				}
			}
			return nil
		}),
	}
	commands = append(commands, cmdutil.CreateAlias(listContext, "config list context"))
	commands = append(commands, cmdutil.CreateAlias(listContext, "config list contexts"))

	configDocs := &cobra.Command{
		Short: "Manages the pachyderm config.",
		Long:  "Gets/sets pachyderm config values.",
	}
	commands = append(commands, cmdutil.CreateDocsAlias(configDocs, "config", "^pachctl config "))

	configGetRoot := &cobra.Command{
		Short: "Commands for getting pachyderm config values",
		Long:  "Commands for getting pachyderm config values",
	}
	commands = append(commands, cmdutil.CreateAlias(configGetRoot, "config get"))

	configSetRoot := &cobra.Command{
		Short: "Commands for setting pachyderm config values",
		Long:  "Commands for setting pachyderm config values",
	}
	commands = append(commands, cmdutil.CreateAlias(configSetRoot, "config set"))

	configUpdateRoot := &cobra.Command{
		Short: "Commands for updating pachyderm config values",
		Long:  "Commands for updating pachyderm config values",
	}
	commands = append(commands, cmdutil.CreateAlias(configUpdateRoot, "config update"))

	configDeleteRoot := &cobra.Command{
		Short: "Commands for deleting pachyderm config values",
		Long:  "Commands for deleting pachyderm config values",
	}
	commands = append(commands, cmdutil.CreateAlias(configDeleteRoot, "config delete"))

	configListRoot := &cobra.Command{
		Short: "Commands for listing pachyderm config values",
		Long:  "Commands for listing pachyderm config values",
	}
	commands = append(commands, cmdutil.CreateAlias(configListRoot, "config list"))

	return commands
}

func contextCompletion(_, text string, maxCompletions int64) ([]prompt.Suggest, shell.CacheFunc) {
	cfg, err := config.Read(false)
	if err != nil {
		log.Fatal(err)
	}
	activeContext, _, err := cfg.ActiveContext(false)
	if err != nil {
		log.Fatal(err)
	}
	var result []prompt.Suggest
	for name, ctx := range cfg.V2.Contexts {
		desc := ctx.PachdAddress
		if name == activeContext {
			desc += " (active)"
		}
		result = append(result, prompt.Suggest{
			Text:        name,
			Description: desc,
		})
	}
	sort.Slice(result, func(i, j int) bool {
		switch {
		case result[i].Text == activeContext:
			return true
		case result[j].Text == activeContext:
			return false
		default:
			return result[i].Text < result[j].Text
		}
	})
	return result, shell.CacheAll
}
