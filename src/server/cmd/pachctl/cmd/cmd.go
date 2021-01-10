package cmd

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/signal"
	"sort"
	"strings"
	"text/template"
	"time"
	"unicode"

	"github.com/pachyderm/pachyderm/src/client"
	"github.com/pachyderm/pachyderm/src/client/pkg/config"
	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
	"github.com/pachyderm/pachyderm/src/client/pps"
	"github.com/pachyderm/pachyderm/src/client/version"
	"github.com/pachyderm/pachyderm/src/client/version/versionpb"
	admincmds "github.com/pachyderm/pachyderm/src/server/admin/cmds"
	authcmds "github.com/pachyderm/pachyderm/src/server/auth/cmds"
	"github.com/pachyderm/pachyderm/src/server/cmd/pachctl/shell"
	configcmds "github.com/pachyderm/pachyderm/src/server/config"
	debugcmds "github.com/pachyderm/pachyderm/src/server/debug/cmds"
	enterprisecmds "github.com/pachyderm/pachyderm/src/server/enterprise/cmds"
	pfscmds "github.com/pachyderm/pachyderm/src/server/pfs/cmds"
	"github.com/pachyderm/pachyderm/src/server/pkg/cmdutil"
	deploycmds "github.com/pachyderm/pachyderm/src/server/pkg/deploy/cmds"
	logutil "github.com/pachyderm/pachyderm/src/server/pkg/log"
	"github.com/pachyderm/pachyderm/src/server/pkg/metrics"
	ppscmds "github.com/pachyderm/pachyderm/src/server/pps/cmds"
	txncmds "github.com/pachyderm/pachyderm/src/server/transaction/cmds"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/fatih/color"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/types"
	"github.com/juju/ansiterm"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	prefixed "github.com/x-cray/logrus-prefixed-formatter"
	"golang.org/x/net/context"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/status"
)

const (
	bashCompletionFunc = `
__pachctl_get_object() {
	if pachctl_output=$(eval pachctl $1 2>/dev/null); then
		local out=($(echo "${pachctl_output}" | awk -v c=$2 'NR > 1 {print $c}'))
		COMPREPLY+=($(compgen -P "${__pachctl_prefix}" -S "${__pachctl_suffix}" -W "${out[*]}" "$cur"))
	fi
}

__pachctl_get_repo() {
	__pachctl_get_object "list repo" 1
}

# $1: repo name
__pachctl_get_commit() {
	if [[ -z $1 ]]; then
		return
	fi
	__pachctl_get_object "list commit $1" 2
	__pachctl_get_object "list branch $1" 1
}

# Performs completion of the standard format <repo>@<branch-or-commit>
__pachctl_get_repo_commit() {
	if [[ ${cur} != *@* ]]; then
		compopt -o nospace
		local __pachctl_suffix="@"
		__pachctl_get_repo
	else
		local repo=$(__parse_repo ${cur})
		local cur=$(__parse_commit ${cur})
		local __pachctl_prefix="${repo}@"
		__pachctl_get_commit ${repo}
	fi
}

# Performs completion of the standard format <repo>@<branch> (will not match commits)
__pachctl_get_repo_branch() {
	if [[ ${cur} != *@* ]]; then
		compopt -o nospace
		local __pachctl_suffix="@"
		__pachctl_get_repo
	else
		local repo=$(__parse_repo ${cur})
		local cur=$(__parse_commit ${cur})
		local __pachctl_prefix="${repo}@"
		__pachctl_get_branch ${repo}
	fi
}

# Performs completion of the standard format <repo>@<branch-or-commit>:<path>
__pachctl_get_repo_commit_path() {
	# completion treats ':' as its own argument and an argument-break
	if [[ ${#nouns[@]} -ge 1 ]] && [[ ${cur} == : ]]; then
		local repo=$(__parse_repo ${nouns[-1]})
		local commit=$(__parse_commit ${nouns[-1]})
		local cur=
		__pachctl_get_path ${repo} ${commit}
	elif [[ ${#nouns[@]} -ge 2 ]] && [[ ${nouns[-1]} == : ]]; then
		local repo=$(__parse_repo ${nouns[-2]})
		local commit=$(__parse_commit ${nouns[-2]})
		__pachctl_get_path ${repo} ${commit}
	elif [[ ${cur} != *@* ]]; then
		__pachctl_get_repo_commit
	else
		compopt -o nospace
		local __pachctl_suffix=":"
		__pachctl_get_repo_commit
	fi
}

# $1: repo name
# $2: branch name or commit id
__pachctl_get_path() {
	if [[ -z $1 ]] || [[ -z $2 ]]; then
		return
	fi

	__pachctl_get_object "glob file \"$1@$2:${cur}**\"" 2
}

# $1: repo name
__pachctl_get_branch() {
	if [[ -z $1 ]]; then
		return
	fi
	__pachctl_get_object "list branch $1" 1
}

__pachctl_get_job() {
	__pachctl_get_object "list job" 1
}

__pachctl_get_pipeline() {
	__pachctl_get_object "list pipeline" 1
}

__pachctl_get_datum() {
	__pachctl_get_object "list datum $2" 1
}

# Parses repo from the format <repo>@<branch-or-commit>
__parse_repo() {
	echo $1 | cut -f 1 -d "@"
}

# Parses commit from the format <repo>@<branch-or-commit>
__parse_commit() {
	echo $1 | cut -f 2 -d "@" -s
}

__pachctl_auth_scope() {
	local out=(none reader writer owner)
	COMPREPLY+=($(compgen -W "${out[*]}" "$cur"))
}

# $*: an integer corresponding to a positional argument index (zero-based)
# Checks if the argument being completed is a positional argument at one of the
# specified indices.
__is_active_arg() {
	for predicate in $*; do
		if [[ $predicate == ${#nouns[@]} ]]; then
			return 0
		fi
	done
	return 1
}

__custom_func() {
	case ${last_command} in
		pachctl_auth_check)
		  if __is_active_arg 0; then
				__pachctl_auth_scope
			elif __is_active_arg 1; then
				__pachctl_get_repo
			fi
			;;
		pachctl_auth_get)
			if __is_active_arg 0 1; then
				__pachctl_get_repo
			fi
			;;
		pachctl_auth_set)
		  if __is_active_arg 1; then
			  __pachctl_auth_scope
			elif __is_active_arg 2; then
				__pachctl_get_repo
			fi
			;;
		pachctl_update_repo | pachctl_inspect_repo | pachctl_delete_repo | pachctl_list_branch | pachctl_list_commit)
			if __is_active_arg 0; then
				__pachctl_get_repo
			fi
			;;
		pachctl_delete_branch | pachctl_subscribe_commit)
			if __is_active_arg 0; then
				__pachctl_get_repo_branch
			fi
			;;
		pachctl_finish_commit | pachctl_inspect_commit | pachctl_delete_commit | pachctl_create_branch | pachctl_start_commit)
			if __is_active_arg 0; then
				__pachctl_get_repo_commit
			fi
			;;
		pachctl_get_file | pachctl_inspect_file | pachctl_list_file | pachctl_delete_file | pachctl_glob_file | pachctl_put_file)
			# completion splits the ':' character into its own argument
			if __is_active_arg 0 1 2; then
				__pachctl_get_repo_commit_path
			fi
			;;
		pachctl_copy_file | pachctl_diff_file)
			__pachctl_get_repo_commit_path
			;;
		pachctl_inspect_job | pachctl_delete_job | pachctl_stop_job | pachctl_list_datum | pachctl_restart_datum)
			if __is_active_arg 0; then
				__pachctl_get_job
			fi
			;;
		pachctl_inspect_datum)
			if __is_active_arg 0; then
				__pachctl_get_job
			elif __is_active_arg 1; then
				__pachctl_get_datum ${nouns[0]}
			fi
			;;
		pachctl_inspect_pipeline | pachctl_delete_pipeline | pachctl_start_pipeline | pachctl_stop_pipeline | pachctl_extract_pipeline | pachctl_edit_pipeline)
			if __is_active_arg 0; then
				__pachctl_get_pipeline
			fi
			;;
		pachctl_flush_job | pachctl_flush_commit)
			__pachctl_get_repo_commit
			;;
		# Deprecated v1.8 commands - remove later
		pachctl_update-repo | pachctl_inspect-repo | pachctl_delete-repo | pachctl_list-branch | pachctl_list-commit)
			if __is_active_arg 0; then
				__pachctl_get_repo
			fi
			;;
		pachctl_delete-branch | pachctl_subscribe-commit)
			if __is_active_arg 0; then
				__pachctl_get_repo
			elif __is_active_arg 1; then
				__pachctl_get_branch ${nouns[0]}
			fi
			;;
		pachctl_finish-commit | pachctl_inspect-commit | pachctl_delete-commit | pachctl_create-branch | pachctl_start-commit)
			if __is_active_arg 0; then
				__pachctl_get_repo
			elif __is_active_arg 1; then
				__pachctl_get_commit ${nouns[0]}
			fi
			;;
		pachctl_get-file | pachctl_inspect-file | pachctl_list-file | pachctl_delete-file | pachctl_glob-file | pachctl_put-file)
			if __is_active_arg 0; then
				__pachctl_get_repo
			elif __is_active_arg 1; then
				__pachctl_get_commit ${nouns[0]}
			elif __is_active_arg 2; then
			  __pachctl_get_path ${nouns[0]} ${nouns[1]}
			fi
			;;
		pachctl_copy-file | pachctl_diff-file)
			if __is_active_arg 0 3; then
				__pachctl_get_repo
			elif __is_active_arg 1 4; then
				__pachctl_get_commit ${nouns[0]}
			elif __is_active_arg 2 5; then
			  __pachctl_get_path ${nouns[0]} ${nouns[1]}
			fi
			;;
		pachctl_inspect-job | pachctl_delete-job | pachctl_stop-job | pachctl_list-datum | pachctl_restart-datum)
			if __is_active_arg 0; then
				__pachctl_get_job
			fi
			;;
		pachctl_inspect-datum)
			if __is_active_arg 0; then
				__pachctl_get_job
			elif __is_active_arg 1; then
				__pachctl_get_datum ${nouns[0]}
			fi
			;;
		pachctl_inspect-pipeline | pachctl_delete-pipeline | pachctl_start-pipeline | pachctl_stop-pipeline | pachctl_extract-pipeline | pachctl_edit-pipeline)
			if __is_active_arg 0; then
				__pachctl_get_pipeline
			fi
			;;
		pachctl_flush-job | pachctl_flush-commit)
			__pachctl_get_repo_slash_commit
			;;
		*)
			;;
	esac
}`
)

// PachctlCmd creates a cobra.Command which can deploy pachyderm clusters and
// interact with them (it implements the pachctl binary).
func PachctlCmd() *cobra.Command {
	var verbose bool

	raw := false
	rawFlags := pflag.NewFlagSet("", pflag.ContinueOnError)
	rawFlags.BoolVar(&raw, "raw", false, "disable pretty printing, print raw json")

	marshaller := &jsonpb.Marshaler{Indent: "  "}

	rootCmd := &cobra.Command{
		Use: os.Args[0],
		Long: `Access the Pachyderm API.

Environment variables:
  PACH_CONFIG=<path>, the path where pachctl will attempt to load your config.
  JAEGER_ENDPOINT=<host>:<port>, the Jaeger server to connect to, if PACH_TRACE
    is set
  PACH_TRACE={true,false}, if true, and JAEGER_ENDPOINT is set, attach a Jaeger
    trace to any outgoing RPCs.
  PACH_TRACE_DURATION=<duration>, the amount of time for which PPS should trace
    a pipeline after 'pachctl create-pipeline' (PACH_TRACE must also be set).
`,
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			log.SetFormatter(new(prefixed.TextFormatter))

			if !verbose {
				log.SetLevel(log.ErrorLevel)
				// Silence grpc logs
				grpclog.SetLoggerV2(grpclog.NewLoggerV2(ioutil.Discard, ioutil.Discard, ioutil.Discard))
			} else {
				log.SetLevel(log.DebugLevel)
				// etcd overrides grpc's logs--there's no way to enable one without
				// enabling both.
				// Error and warning logs are discarded because they will be
				// redundantly sent to the info logger. See:
				// https://godoc.org/google.golang.org/grpc/grpclog#NewLoggerV2
				logger := log.StandardLogger()
				etcd.SetLogger(grpclog.NewLoggerV2(
					logutil.NewGRPCLogWriter(logger, "etcd/grpc"),
					ioutil.Discard,
					ioutil.Discard,
				))
				cmdutil.PrintErrorStacks = true
			}
		},
		BashCompletionFunction: bashCompletionFunc,
	}
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "Output verbose logs")
	rootCmd.PersistentFlags().BoolVar(&color.NoColor, "no-color", false, "Turn off colors.")

	var subcommands []*cobra.Command

	var clientOnly bool
	var timeoutFlag string
	versionCmd := &cobra.Command{
		Short: "Print Pachyderm version information.",
		Long:  "Print Pachyderm version information.",
		Run: cmdutil.RunFixedArgs(0, func(args []string) (retErr error) {
			if clientOnly {
				if raw {
					if err := marshaller.Marshal(os.Stdout, version.Version); err != nil {
						return err
					}
				} else {
					fmt.Println(version.PrettyPrintVersion(version.Version))
				}
				return nil
			}

			start := time.Now()
			startMetricsWait := metrics.StartReportAndFlushUserAction("Version", start)
			defer startMetricsWait()
			defer func() {
				finishMetricsWait := metrics.FinishReportAndFlushUserAction("Version", retErr, start)
				finishMetricsWait()
			}()

			// Print header + client version
			writer := ansiterm.NewTabWriter(os.Stdout, 20, 1, 3, ' ', 0)
			if raw {
				if err := marshaller.Marshal(os.Stdout, version.Version); err != nil {
					return err
				}
			} else {
				printVersionHeader(writer)
				printVersion(writer, "pachctl", version.Version)
				if err := writer.Flush(); err != nil {
					return err
				}
			}

			// Dial pachd & get server version
			var pachClient *client.APIClient
			var err error
			if timeoutFlag != "default" {
				var timeout time.Duration
				timeout, err = time.ParseDuration(timeoutFlag)
				if err != nil {
					return errors.Wrapf(err, "could not parse timeout duration %q", timeout)
				}
				pachClient, err = client.NewOnUserMachine("user", client.WithDialTimeout(timeout))
			} else {
				pachClient, err = client.NewOnUserMachine("user")
			}
			if err != nil {
				return err
			}
			defer pachClient.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			version, err := pachClient.GetVersion(ctx, &types.Empty{})

			if err != nil {
				buf := bytes.NewBufferString("")
				errWriter := ansiterm.NewTabWriter(buf, 20, 1, 3, ' ', 0)
				fmt.Fprintf(errWriter, "pachd\t(version unknown) : error connecting to pachd server at address (%v): %v\n\nplease make sure pachd is up (`kubectl get all`) and portforwarding is enabled\n", pachClient.GetAddress(), status.Convert(err).Message())
				errWriter.Flush()
				return errors.New(buf.String())
			}

			// print server version
			if raw {
				if err := marshaller.Marshal(os.Stdout, version); err != nil {
					return err
				}
			} else {
				printVersion(writer, "pachd", version)
				if err := writer.Flush(); err != nil {
					return err
				}
			}
			return nil
		}),
	}
	versionCmd.Flags().BoolVar(&clientOnly, "client-only", false, "If set, "+
		"only print pachctl's version, but don't make any RPCs to pachd. Useful "+
		"if pachd is unavailable")
	versionCmd.Flags().StringVar(&timeoutFlag, "timeout", "default", "If set, "+
		"'pachctl version' will timeout after the given duration (formatted as a "+
		"golang time duration--a number followed by ns, us, ms, s, m, or h). If "+
		"--client-only is set, this flag is ignored. If unset, pachctl will use a "+
		"default timeout; if set to 0s, the call will never time out.")
	versionCmd.Flags().AddFlagSet(rawFlags)
	subcommands = append(subcommands, cmdutil.CreateAlias(versionCmd, "version"))
	exitCmd := &cobra.Command{
		Short: "Exit the pachctl shell.",
		Long:  "Exit the pachctl shell.",
		Run:   cmdutil.RunFixedArgs(0, func(args []string) error { return nil }),
	}
	subcommands = append(subcommands, cmdutil.CreateAlias(exitCmd, "exit"))

	var maxCompletions int64
	shellCmd := &cobra.Command{
		Short: "Run the pachyderm shell.",
		Long:  "Run the pachyderm shell.",
		Run: cmdutil.RunFixedArgs(0, func(args []string) (retErr error) {
			cfg, err := config.Read(true, false)
			if err != nil {
				return err
			}
			if maxCompletions == 0 {
				maxCompletions = cfg.V2.MaxShellCompletions
			}
			shell.Run(rootCmd, maxCompletions) // never returns
			return nil
		}),
	}
	shellCmd.Flags().Int64Var(&maxCompletions, "max-completions", 0, "The maximum number of completions to show in the shell, defaults to 64.")
	subcommands = append(subcommands, cmdutil.CreateAlias(shellCmd, "shell"))

	deleteAll := &cobra.Command{
		Short: "Delete everything.",
		Long: `Delete all repos, commits, files, pipelines and jobs.
This resets the cluster to its initial state.`,
		Run: cmdutil.RunFixedArgs(0, func(args []string) error {
			client, err := client.NewOnUserMachine("user")
			if err != nil {
				return err
			}
			defer client.Close()
			red := color.New(color.FgRed).SprintFunc()
			var repos, pipelines []string
			repoInfos, err := client.ListRepo()
			if err != nil {
				return err
			}
			for _, ri := range repoInfos {
				repos = append(repos, red(ri.Repo.Name))
			}
			resp, err := client.PpsAPIClient.ListPipeline(client.Ctx(), &pps.ListPipelineRequest{AllowIncomplete: true})
			if err != nil {
				return err
			}
			for _, pi := range resp.PipelineInfo {
				pipelines = append(pipelines, red(pi.Pipeline.Name))
			}
			fmt.Println("All ACLs, repos, commits, files, pipelines and jobs will be deleted.")
			if len(repos) > 0 {
				fmt.Printf("Repos to delete: %s\n", strings.Join(repos, ", "))
			}
			if len(pipelines) > 0 {
				fmt.Printf("Pipelines to delete: %s\n", strings.Join(pipelines, ", "))
			}
			if ok, err := cmdutil.InteractiveConfirm(); err != nil {
				return err
			} else if !ok {
				return nil
			}

			if err := client.DeleteAll(); err != nil {
				return err
			}
			return txncmds.ClearActiveTransaction()
		}),
	}
	subcommands = append(subcommands, cmdutil.CreateAlias(deleteAll, "delete all"))

	var port uint16
	var remotePort uint16
	var samlPort uint16
	var remoteSamlPort uint16
	var oidcPort uint16
	var remoteOidcPort uint16
	var uiPort uint16
	var uiWebsocketPort uint16
	var pfsPort uint16
	var s3gatewayPort uint16
	var namespace string
	portForward := &cobra.Command{
		Short: "Forward a port on the local machine to pachd. This command blocks.",
		Long:  "Forward a port on the local machine to pachd. This command blocks.",
		Run: cmdutil.RunFixedArgs(0, func(args []string) error {
			// TODO(ys): remove the `--namespace` flag here eventually
			if namespace != "" {
				fmt.Printf("WARNING: The `--namespace` flag is deprecated and will be removed in a future version. Please set the namespace in the pachyderm context instead: pachctl config update context `pachctl config get active-context` --namespace '%s'\n", namespace)
			}

			cfg, err := config.Read(false, false)
			if err != nil {
				return err
			}
			contextName, context, err := cfg.ActiveContext(true)
			if err != nil {
				return err
			}
			if context.PortForwarders != nil && len(context.PortForwarders) > 0 {
				return errors.New("port forwarding appears to already be running for this context")
			}

			fw, err := client.NewPortForwarder(context, namespace)
			if err != nil {
				return err
			}
			defer fw.Close()

			context.PortForwarders = map[string]uint32{}
			successCount := 0

			fmt.Println("Forwarding the pachd (Pachyderm daemon) port...")
			port, err := fw.RunForDaemon(port, remotePort)
			if err != nil {
				fmt.Printf("port forwarding failed: %v\n", err)
			} else {
				fmt.Printf("listening on port %d\n", port)
				context.PortForwarders["pachd"] = uint32(port)
				successCount++
			}

			fmt.Println("Forwarding the SAML ACS port...")
			port, err = fw.RunForSAMLACS(samlPort, remoteSamlPort)
			if err != nil {
				fmt.Printf("port forwarding failed: %v\n", err)
			} else {
				fmt.Printf("listening on port %d\n", port)
				context.PortForwarders["saml-acs"] = uint32(port)
				successCount++
			}

			fmt.Println("Forwarding the OIDC ACS port...")
			port, err = fw.RunForOIDCACS(oidcPort, remoteOidcPort)
			if err != nil {
				fmt.Printf("port forwarding failed: %v\n", err)
			} else {
				fmt.Printf("listening on port %d\n", port)
				context.PortForwarders["oidc-acs"] = uint32(port)
				successCount++
			}

			fmt.Printf("Forwarding the dash (Pachyderm dashboard) UI port to http://localhost:%v...\n", uiPort)
			port, err = fw.RunForDashUI(uiPort)
			if err != nil {
				fmt.Printf("port forwarding failed: %v\n", err)
			} else {
				fmt.Printf("listening on port %d\n", port)
				context.PortForwarders["dash-ui"] = uint32(port)
				successCount++
			}

			fmt.Println("Forwarding the dash (Pachyderm dashboard) websocket port...")
			port, err = fw.RunForDashWebSocket(uiWebsocketPort)
			if err != nil {
				fmt.Printf("port forwarding failed: %v\n", err)
			} else {
				fmt.Printf("listening on port %d\n", port)
				context.PortForwarders["dash-ws"] = uint32(port)
				successCount++
			}

			fmt.Println("Forwarding the PFS port...")
			port, err = fw.RunForPFS(pfsPort)
			if err != nil {
				fmt.Printf("port forwarding failed: %v\n", err)
			} else {
				fmt.Printf("listening on port %d\n", port)
				context.PortForwarders["pfs-over-http"] = uint32(port)
				successCount++
			}

			fmt.Println("Forwarding the s3gateway port...")
			port, err = fw.RunForS3Gateway(s3gatewayPort)
			if err != nil {
				fmt.Printf("port forwarding failed: %v\n", err)
			} else {
				fmt.Printf("listening on port %d\n", port)
				context.PortForwarders["s3g"] = uint32(port)
				successCount++
			}

			if successCount == 0 {
				return errors.New("failed to start port forwarders")
			}

			if err = cfg.Write(); err != nil {
				return err
			}

			defer func() {
				// reload config in case changes have happened since the
				// config was last read
				cfg, err := config.Read(true, false)
				if err != nil {
					fmt.Fprintf(os.Stderr, "failed to read config file: %v\n", err)
					return
				}
				context, ok := cfg.V2.Contexts[contextName]
				if ok {
					context.PortForwarders = nil
					if err := cfg.Write(); err != nil {
						fmt.Fprintf(os.Stderr, "failed to write config file: %v\n", err)
					}
				}
			}()

			fmt.Println("CTRL-C to exit")
			ch := make(chan os.Signal, 1)
			signal.Notify(ch, os.Interrupt)
			<-ch

			return nil
		}),
	}
	portForward.Flags().Uint16VarP(&port, "port", "p", 30650, "The local port to bind pachd to.")
	portForward.Flags().Uint16Var(&remotePort, "remote-port", 650, "The remote port that pachd is bound to in the cluster.")
	portForward.Flags().Uint16Var(&samlPort, "saml-port", 30654, "The local port to bind pachd's SAML ACS to.")
	portForward.Flags().Uint16Var(&remoteSamlPort, "remote-saml-port", 654, "The remote port that SAML ACS is bound to in the cluster.")
	portForward.Flags().Uint16Var(&oidcPort, "oidc-port", 30657, "The local port to bind pachd's OIDC ACS to.")
	portForward.Flags().Uint16Var(&remoteOidcPort, "remote-oidc-port", 657, "The remote port that OIDC ACS is bound to in the cluster.")
	portForward.Flags().Uint16VarP(&uiPort, "ui-port", "u", 30080, "The local port to bind Pachyderm's dash service to.")
	portForward.Flags().Uint16VarP(&uiWebsocketPort, "proxy-port", "x", 30081, "The local port to bind Pachyderm's dash proxy service to.")
	portForward.Flags().Uint16VarP(&pfsPort, "pfs-port", "f", 30652, "The local port to bind PFS over HTTP to.")
	portForward.Flags().Uint16VarP(&s3gatewayPort, "s3gateway-port", "s", 30600, "The local port to bind the s3gateway to.")
	portForward.Flags().StringVar(&namespace, "namespace", "", "Kubernetes namespace Pachyderm is deployed in.")
	subcommands = append(subcommands, cmdutil.CreateAlias(portForward, "port-forward"))

	var install bool
	var installPathBash string
	completionBash := &cobra.Command{
		Short: "Print or install the bash completion code.",
		Long:  "Print or install the bash completion code.",
		Run: cmdutil.RunFixedArgs(0, func(args []string) (retErr error) {
			return createCompletions(rootCmd, install, installPathBash, rootCmd.GenBashCompletion)
		}),
	}
	completionBash.Flags().BoolVar(&install, "install", false, "Install the completion.")
	completionBash.Flags().StringVar(&installPathBash, "path", "/etc/bash_completion.d/pachctl", "Path to install the completions to.")
	subcommands = append(subcommands, cmdutil.CreateAlias(completionBash, "completion bash"))

	var installPathZsh string
	completionZsh := &cobra.Command{
		Short: "Print or install the zsh completion code.",
		Long:  "Print or install the zsh completion code.",
		Run: cmdutil.RunFixedArgs(0, func(args []string) (retErr error) {
			return createCompletions(rootCmd, install, installPathZsh, rootCmd.GenZshCompletion)
		}),
	}
	completionZsh.Flags().BoolVar(&install, "install", false, "Install the completion.")
	completionZsh.Flags().StringVar(&installPathZsh, "path", "_pachctl", "Path to install the completions to.")
	subcommands = append(subcommands, cmdutil.CreateAlias(completionZsh, "completion zsh"))

	// Logical commands for grouping commands by verb (no run functions)
	completionDocs := &cobra.Command{
		Short: "Print or install terminal completion code.",
		Long:  "Print or install terminal completion code.",
	}
	subcommands = append(subcommands, cmdutil.CreateAlias(completionDocs, "completion"))

	deleteDocs := &cobra.Command{
		Short: "Delete an existing Pachyderm resource.",
		Long:  "Delete an existing Pachyderm resource.",
	}
	subcommands = append(subcommands, cmdutil.CreateAlias(deleteDocs, "delete"))

	createDocs := &cobra.Command{
		Short: "Create a new instance of a Pachyderm resource.",
		Long:  "Create a new instance of a Pachyderm resource.",
	}
	subcommands = append(subcommands, cmdutil.CreateAlias(createDocs, "create"))

	updateDocs := &cobra.Command{
		Short: "Change the properties of an existing Pachyderm resource.",
		Long:  "Change the properties of an existing Pachyderm resource.",
	}
	subcommands = append(subcommands, cmdutil.CreateAlias(updateDocs, "update"))

	inspectDocs := &cobra.Command{
		Short: "Show detailed information about a Pachyderm resource.",
		Long:  "Show detailed information about a Pachyderm resource.",
	}
	subcommands = append(subcommands, cmdutil.CreateAlias(inspectDocs, "inspect"))

	listDocs := &cobra.Command{
		Short: "Print a list of Pachyderm resources of a specific type.",
		Long:  "Print a list of Pachyderm resources of a specific type.",
	}
	subcommands = append(subcommands, cmdutil.CreateAlias(listDocs, "list"))

	startDocs := &cobra.Command{
		Short: "Start a Pachyderm resource.",
		Long:  "Start a Pachyderm resource.",
	}
	subcommands = append(subcommands, cmdutil.CreateAlias(startDocs, "start"))

	finishDocs := &cobra.Command{
		Short: "Finish a Pachyderm resource.",
		Long:  "Finish a Pachyderm resource.",
	}
	subcommands = append(subcommands, cmdutil.CreateAlias(finishDocs, "finish"))

	flushDocs := &cobra.Command{
		Short: "Wait for the side-effects of a Pachyderm resource to propagate.",
		Long:  "Wait for the side-effects of a Pachyderm resource to propagate.",
	}
	subcommands = append(subcommands, cmdutil.CreateAlias(flushDocs, "flush"))

	subscribeDocs := &cobra.Command{
		Short: "Wait for notifications of changes to a Pachyderm resource.",
		Long:  "Wait for notifications of changes to a Pachyderm resource.",
	}
	subcommands = append(subcommands, cmdutil.CreateAlias(subscribeDocs, "subscribe"))

	putDocs := &cobra.Command{
		Short: "Insert data into Pachyderm.",
		Long:  "Insert data into Pachyderm.",
	}
	subcommands = append(subcommands, cmdutil.CreateAlias(putDocs, "put"))

	copyDocs := &cobra.Command{
		Short: "Copy a Pachyderm resource.",
		Long:  "Copy a Pachyderm resource.",
	}
	subcommands = append(subcommands, cmdutil.CreateAlias(copyDocs, "copy"))

	getDocs := &cobra.Command{
		Short: "Get the raw data represented by a Pachyderm resource.",
		Long:  "Get the raw data represented by a Pachyderm resource.",
	}
	subcommands = append(subcommands, cmdutil.CreateAlias(getDocs, "get"))

	globDocs := &cobra.Command{
		Short: "Print a list of Pachyderm resources matching a glob pattern.",
		Long:  "Print a list of Pachyderm resources matching a glob pattern.",
	}
	subcommands = append(subcommands, cmdutil.CreateAlias(globDocs, "glob"))

	diffDocs := &cobra.Command{
		Short: "Show the differences between two Pachyderm resources.",
		Long:  "Show the differences between two Pachyderm resources.",
	}
	subcommands = append(subcommands, cmdutil.CreateAlias(diffDocs, "diff"))

	stopDocs := &cobra.Command{
		Short: "Cancel an ongoing task.",
		Long:  "Cancel an ongoing task.",
	}
	subcommands = append(subcommands, cmdutil.CreateAlias(stopDocs, "stop"))

	restartDocs := &cobra.Command{
		Short: "Cancel and restart an ongoing task.",
		Long:  "Cancel and restart an ongoing task.",
	}
	subcommands = append(subcommands, cmdutil.CreateAlias(restartDocs, "restart"))

	resumeDocs := &cobra.Command{
		Short: "Resume a stopped task.",
		Long:  "Resume a stopped task.",
	}
	subcommands = append(subcommands, cmdutil.CreateAlias(resumeDocs, "resume"))

	runDocs := &cobra.Command{
		Short: "Manually run a Pachyderm resource.",
		Long:  "Manually run a Pachyderm resource.",
	}
	subcommands = append(subcommands, cmdutil.CreateAlias(runDocs, "run"))

	editDocs := &cobra.Command{
		Short: "Edit the value of an existing Pachyderm resource.",
		Long:  "Edit the value of an existing Pachyderm resource.",
	}
	subcommands = append(subcommands, cmdutil.CreateAlias(editDocs, "edit"))

	subcommands = append(subcommands, pfscmds.Cmds()...)
	subcommands = append(subcommands, ppscmds.Cmds()...)
	subcommands = append(subcommands, deploycmds.Cmds()...)
	subcommands = append(subcommands, authcmds.Cmds()...)
	subcommands = append(subcommands, enterprisecmds.Cmds()...)
	subcommands = append(subcommands, admincmds.Cmds()...)
	subcommands = append(subcommands, debugcmds.Cmds()...)
	subcommands = append(subcommands, txncmds.Cmds()...)
	subcommands = append(subcommands, configcmds.Cmds()...)

	cmdutil.MergeCommands(rootCmd, subcommands)
	applyRootUsageFunc(rootCmd)
	return rootCmd
}

func printVersionHeader(w io.Writer) {
	fmt.Fprintf(w, "COMPONENT\tVERSION\t\n")
}

func printVersion(w io.Writer, component string, v *versionpb.Version) {
	fmt.Fprintf(w, "%s\t%s\t\n", component, version.PrettyPrintVersion(v))
}

func applyRootUsageFunc(rootCmd *cobra.Command) {
	// Partition subcommands by category
	var admin []*cobra.Command
	var actions []*cobra.Command
	var other []*cobra.Command

	for _, subcmd := range rootCmd.Commands() {
		switch subcmd.Name() {
		case
			"branch",
			"commit",
			"datum",
			"file",
			"job",
			"object",
			"pipeline",
			"repo",
			"tag":
			// These are ignored - they will show up in the help topics section
		case
			"copy",
			"create",
			"delete",
			"diff",
			"edit",
			"finish",
			"flush",
			"get",
			"glob",
			"inspect",
			"list",
			"put",
			"restart",
			"start",
			"stop",
			"subscribe",
			"update":
			actions = append(actions, subcmd)
		case
			"deploy",
			"undeploy",
			"extract",
			"restore",
			"garbage-collect",
			"update-dash",
			"auth",
			"enterprise":
			admin = append(admin, subcmd)
		default:
			other = append(other, subcmd)
		}
	}

	sortGroup := func(group []*cobra.Command) {
		sort.Slice(group, func(i, j int) bool {
			return group[i].Name() < group[j].Name()
		})
	}

	sortGroup(admin)
	sortGroup(actions)
	sortGroup(other)

	// Template environment copied from cobra templates
	templateFuncs := template.FuncMap{
		"trimRightSpace": func(s string) string {
			return strings.TrimRightFunc(s, unicode.IsSpace)
		},
		"rpad": func(s string, padding int) string {
			format := fmt.Sprintf("%%-%ds", padding)
			return fmt.Sprintf(format, s)
		},
		"admin": func() []*cobra.Command {
			return admin
		},
		"actions": func() []*cobra.Command {
			return actions
		},
		"other": func() []*cobra.Command {
			return other
		},
	}

	// template modified from default cobra template
	text := `Usage:
  {{.CommandPath}} [command]

Administration Commands:{{range admin}}{{if .IsAvailableCommand}}
  {{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}

Commands by Action:{{range actions}}{{if .IsAvailableCommand}}
  {{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}

Other Commands:{{range other}}{{if .IsAvailableCommand}}
  {{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}{{if .HasAvailableLocalFlags}}

Additional help topics:{{range .Commands}}{{if .IsHelpCommand}}
  {{rpad .Name .NamePadding}} {{.Short}}{{end}}{{end}}{{end}}{{if .HasAvailableSubCommands}}

Use "{{.CommandPath}} [command] --help" for more information about a command.{{end}}
`

	originalUsageFunc := rootCmd.UsageFunc()
	rootCmd.SetUsageFunc(func(cmd *cobra.Command) error {
		if cmd != rootCmd {
			return originalUsageFunc(cmd)
		}

		t := template.New("top")
		t.Funcs(templateFuncs)
		template.Must(t.Parse(text))
		return t.Execute(cmd.OutOrStderr(), cmd)
	})
}

func createCompletions(rootCmd *cobra.Command, install bool, installPath string, f func(io.Writer) error) (retErr error) {
	var dest io.Writer

	if install {
		f, err := os.Create(installPath)
		if err != nil {
			if os.IsPermission(err) {
				return errors.New("could not install completions due to permissions - rerun this command with sudo")
			}
			return errors.Wrapf(err, "could not install completions")
		}

		defer func() {
			if err := f.Close(); err != nil && retErr == nil {
				retErr = err
			} else {
				fmt.Printf("Completions installed in %q, you must restart your terminal to enable them.\n", installPath)
			}
		}()

		dest = f
	} else {
		dest = os.Stdout
	}

	// Remove 'hidden' flag from all commands so we can get completions for them as well
	var unhide func(*cobra.Command)
	unhide = func(cmd *cobra.Command) {
		cmd.Hidden = false
		for _, subcmd := range cmd.Commands() {
			unhide(subcmd)
		}
	}
	unhide(rootCmd)

	return f(dest)
}
