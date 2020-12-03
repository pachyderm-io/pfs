package cmds

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	pachdclient "github.com/pachyderm/pachyderm/src/client"
	"github.com/pachyderm/pachyderm/src/client/pfs"
	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
	"github.com/pachyderm/pachyderm/src/client/pkg/grpcutil"
	"github.com/pachyderm/pachyderm/src/client/pkg/tracing/extended"
	ppsclient "github.com/pachyderm/pachyderm/src/client/pps"
	"github.com/pachyderm/pachyderm/src/client/version"
	"github.com/pachyderm/pachyderm/src/server/cmd/pachctl/shell"
	"github.com/pachyderm/pachyderm/src/server/pkg/cmdutil"
	"github.com/pachyderm/pachyderm/src/server/pkg/pager"
	"github.com/pachyderm/pachyderm/src/server/pkg/ppsutil"
	"github.com/pachyderm/pachyderm/src/server/pkg/progress"
	"github.com/pachyderm/pachyderm/src/server/pkg/serde"
	"github.com/pachyderm/pachyderm/src/server/pkg/tabwriter"
	"github.com/pachyderm/pachyderm/src/server/pkg/uuid"
	"github.com/pachyderm/pachyderm/src/server/pps/pretty"

	prompt "github.com/c-bata/go-prompt"
	units "github.com/docker/go-units"
	"github.com/fatih/color"
	docker "github.com/fsouza/go-dockerclient"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/itchyny/gojq"
	glob "github.com/pachyderm/ohmyglob"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"golang.org/x/net/context"
)

// encoder creates an encoder that writes data structures to w[0] (or os.Stdout
// if no 'w' is passed) in the serialization format 'format'. If more than one
// writer is passed, all writers after the first are silently ignored (rather
// than returning an error), and if the 'format' passed is unrecognized
// (currently, 'format' must be 'json' or 'yaml') then pachctl exits
// immediately. Ignoring errors or crashing simplifies the type signature of
// 'encoder' and allows it to be used inline.
func encoder(format string, w ...io.Writer) serde.Encoder {
	format = strings.ToLower(format)
	if format == "" {
		format = "json"
	}
	var output io.Writer = os.Stdout
	if len(w) > 0 {
		output = w[0]
	}
	e, err := serde.GetEncoder(format, output,
		serde.WithIndent(2),
		serde.WithOrigName(true),
	)
	if err != nil {
		cmdutil.ErrorAndExit(err.Error())
	}
	return e
}

// Cmds returns a slice containing pps commands.
func Cmds() []*cobra.Command {
	var commands []*cobra.Command

	raw := false
	var output string
	outputFlags := pflag.NewFlagSet("", pflag.ExitOnError)
	outputFlags.BoolVar(&raw, "raw", false, "Disable pretty printing; serialize data structures to an encoding such as json or yaml")
	// --output is empty by default, so that we can print an error if a user
	// explicitly sets --output without --raw, but the effective default is set in
	// encode(), which assumes "json" if 'format' is empty.
	// Note: because of how spf13/flags works, no other StringVarP that sets
	// 'output' can have a default value either
	outputFlags.StringVarP(&output, "output", "o", "", "Output format when --raw is set: \"json\" or \"yaml\" (default \"json\")")

	fullTimestamps := false
	fullTimestampsFlags := pflag.NewFlagSet("", pflag.ContinueOnError)
	fullTimestampsFlags.BoolVar(&fullTimestamps, "full-timestamps", false, "Return absolute timestamps (as opposed to the default, relative timestamps).")

	noPager := false
	noPagerFlags := pflag.NewFlagSet("", pflag.ContinueOnError)
	noPagerFlags.BoolVar(&noPager, "no-pager", false, "Don't pipe output into a pager (i.e. less).")

	jobDocs := &cobra.Command{
		Short: "Docs for jobs.",
		Long: `Jobs are the basic units of computation in Pachyderm.

Jobs run a containerized workload over a set of finished input commits. Jobs are
created by pipelines and will write output to a commit in the pipeline's output
repo. A job can have multiple datums, each processed independently and the
results will be merged together at the end.

If the job fails, the output commit will not be populated with data.`,
	}
	commands = append(commands, cmdutil.CreateDocsAlias(jobDocs, "job", " job$"))

	var block bool
	inspectJob := &cobra.Command{
		Use:   "{{alias}} <job>",
		Short: "Return info about a job.",
		Long:  "Return info about a job.",
		Run: cmdutil.RunFixedArgs(1, func(args []string) error {
			client, err := pachdclient.NewOnUserMachine("user")
			if err != nil {
				return err
			}
			defer client.Close()
			jobInfo, err := client.InspectJob(args[0], block, true)
			if err != nil {
				cmdutil.ErrorAndExit("error from InspectJob: %s", err.Error())
			}
			if jobInfo == nil {
				cmdutil.ErrorAndExit("job %s not found.", args[0])
			}
			if raw {
				return encoder(output).EncodeProto(jobInfo)
			} else if output != "" {
				cmdutil.ErrorAndExit("cannot set --output (-o) without --raw")
			}
			ji := &pretty.PrintableJobInfo{
				JobInfo:        jobInfo,
				FullTimestamps: fullTimestamps,
			}
			return pretty.PrintDetailedJobInfo(ji)
		}),
	}
	inspectJob.Flags().BoolVarP(&block, "block", "b", false, "block until the job has either succeeded or failed")
	inspectJob.Flags().AddFlagSet(outputFlags)
	inspectJob.Flags().AddFlagSet(fullTimestampsFlags)
	shell.RegisterCompletionFunc(inspectJob, shell.JobCompletion)
	commands = append(commands, cmdutil.CreateAlias(inspectJob, "inspect job"))

	var pipelineName string
	var outputCommitStr string
	var inputCommitStrs []string
	var history string
	var stateStrs []string
	listJob := &cobra.Command{
		Short: "Return info about jobs.",
		Long:  "Return info about jobs.",
		Example: `
# Return all jobs
$ {{alias}}

# Return all jobs from the most recent version of pipeline "foo"
$ {{alias}} -p foo

# Return all jobs from all versions of pipeline "foo"
$ {{alias}} -p foo --history all

# Return all jobs whose input commits include foo@XXX and bar@YYY
$ {{alias}} -i foo@XXX -i bar@YYY

# Return all jobs in pipeline foo and whose input commits include bar@YYY
$ {{alias}} -p foo -i bar@YYY`,
		Run: cmdutil.RunFixedArgs(0, func(args []string) error {
			commits, err := cmdutil.ParseCommits(inputCommitStrs)
			if err != nil {
				return err
			}
			history, err := cmdutil.ParseHistory(history)
			if err != nil {
				return errors.Wrapf(err, "error parsing history flag")
			}
			var outputCommit *pfs.Commit
			if outputCommitStr != "" {
				outputCommit, err = cmdutil.ParseCommit(outputCommitStr)
				if err != nil {
					return err
				}
			}
			var filter string
			if len(stateStrs) > 0 {
				filter, err = ParseJobStates(stateStrs)
				if err != nil {
					return errors.Wrap(err, "error parsing state")
				}
			}

			client, err := pachdclient.NewOnUserMachine("user")
			if err != nil {
				return err
			}
			defer client.Close()

			return pager.Page(noPager, os.Stdout, func(w io.Writer) error {
				if raw {
					e := encoder(output)
					return client.ListJobFilterF(pipelineName, commits, outputCommit, history, true, filter, func(ji *ppsclient.JobInfo) error {
						return e.EncodeProto(ji)
					})
				} else if output != "" {
					cmdutil.ErrorAndExit("cannot set --output (-o) without --raw")
				}
				writer := tabwriter.NewWriter(w, pretty.JobHeader)
				if err := client.ListJobFilterF(pipelineName, commits, outputCommit, history, false, filter, func(ji *ppsclient.JobInfo) error {
					pretty.PrintJobInfo(writer, ji, fullTimestamps)
					return nil
				}); err != nil {
					return err
				}
				return writer.Flush()
			})
		}),
	}
	listJob.Flags().StringVarP(&pipelineName, "pipeline", "p", "", "Limit to jobs made by pipeline.")
	listJob.MarkFlagCustom("pipeline", "__pachctl_get_pipeline")
	listJob.Flags().StringVarP(&outputCommitStr, "output", "o", "", "List jobs with a specific output commit. format: <repo>@<branch-or-commit>")
	listJob.MarkFlagCustom("output", "__pachctl_get_repo_commit")
	listJob.Flags().StringSliceVarP(&inputCommitStrs, "input", "i", []string{}, "List jobs with a specific set of input commits. format: <repo>@<branch-or-commit>")
	listJob.MarkFlagCustom("input", "__pachctl_get_repo_commit")
	listJob.Flags().AddFlagSet(outputFlags)
	listJob.Flags().AddFlagSet(fullTimestampsFlags)
	listJob.Flags().AddFlagSet(noPagerFlags)
	listJob.Flags().StringVar(&history, "history", "none", "Return jobs from historical versions of pipelines.")
	listJob.Flags().StringArrayVar(&stateStrs, "state", []string{}, "Return only jobs with the specified state. Can be repeated to include multiple states")
	shell.RegisterCompletionFunc(listJob,
		func(flag, text string, maxCompletions int64) ([]prompt.Suggest, shell.CacheFunc) {
			if flag == "-p" || flag == "--pipeline" {
				cs, cf := shell.PipelineCompletion(flag, text, maxCompletions)
				return cs, shell.AndCacheFunc(cf, shell.SameFlag(flag))
			}
			return nil, shell.SameFlag(flag)
		})
	commands = append(commands, cmdutil.CreateAlias(listJob, "list job"))
	commands = append(commands, cmdutil.Hide(cmdutil.CreateAlias(listJob, "list jobs")))

	var pipelines cmdutil.RepeatedStringArg
	flushJob := &cobra.Command{
		Use:   "{{alias}} <repo>@<branch-or-commit> ...",
		Short: "Wait for all jobs caused by the specified commits to finish and return them.",
		Long:  "Wait for all jobs caused by the specified commits to finish and return them.",
		Example: `
# Return jobs caused by foo@XXX and bar@YYY.
$ {{alias}} foo@XXX bar@YYY

# Return jobs caused by foo@XXX leading to pipelines bar and baz.
$ {{alias}} foo@XXX -p bar -p baz`,
		Run: cmdutil.Run(func(args []string) error {
			if output != "" && !raw {
				cmdutil.ErrorAndExit("cannot set --output (-o) without --raw")
			}
			commits, err := cmdutil.ParseCommits(args)
			if err != nil {
				return err
			}

			c, err := pachdclient.NewOnUserMachine("user")
			if err != nil {
				return err
			}
			defer c.Close()
			var writer *tabwriter.Writer
			if !raw {
				writer = tabwriter.NewWriter(os.Stdout, pretty.JobHeader)
			}
			e := encoder(output)
			if err := c.FlushJob(commits, pipelines, func(ji *ppsclient.JobInfo) error {
				if raw {
					if err := e.EncodeProto(ji); err != nil {
						return err
					}
					return nil
				}
				pretty.PrintJobInfo(writer, ji, fullTimestamps)
				return nil
			}); err != nil {
				return err
			}
			if !raw {
				return writer.Flush()
			}
			return nil
		}),
	}
	flushJob.Flags().VarP(&pipelines, "pipeline", "p", "Wait only for jobs leading to a specific set of pipelines")
	flushJob.MarkFlagCustom("pipeline", "__pachctl_get_pipeline")
	flushJob.Flags().AddFlagSet(outputFlags)
	flushJob.Flags().AddFlagSet(fullTimestampsFlags)
	shell.RegisterCompletionFunc(flushJob,
		func(flag, text string, maxCompletions int64) ([]prompt.Suggest, shell.CacheFunc) {
			if flag == "--pipeline" || flag == "-p" {
				cs, cf := shell.PipelineCompletion(flag, text, maxCompletions)
				return cs, shell.AndCacheFunc(cf, shell.SameFlag(flag))
			}
			cs, cf := shell.BranchCompletion(flag, text, maxCompletions)
			return cs, shell.AndCacheFunc(cf, shell.SameFlag(flag))
		})
	commands = append(commands, cmdutil.CreateAlias(flushJob, "flush job"))
	commands = append(commands, cmdutil.Hide(cmdutil.CreateAlias(flushJob, "flush jobs")))

	deleteJob := &cobra.Command{
		Use:   "{{alias}} <job>",
		Short: "Delete a job.",
		Long:  "Delete a job.",
		Run: cmdutil.RunFixedArgs(1, func(args []string) error {
			client, err := pachdclient.NewOnUserMachine("user")
			if err != nil {
				return err
			}
			defer client.Close()
			if err := client.DeleteJob(args[0]); err != nil {
				cmdutil.ErrorAndExit("error from DeleteJob: %s", err.Error())
			}
			return nil
		}),
	}
	shell.RegisterCompletionFunc(deleteJob, shell.JobCompletion)
	commands = append(commands, cmdutil.CreateAlias(deleteJob, "delete job"))

	stopJob := &cobra.Command{
		Use:   "{{alias}} <job>",
		Short: "Stop a job.",
		Long:  "Stop a job.  The job will be stopped immediately.",
		Run: cmdutil.RunFixedArgs(1, func(args []string) error {
			client, err := pachdclient.NewOnUserMachine("user")
			if err != nil {
				return err
			}
			defer client.Close()
			if err := client.StopJob(args[0]); err != nil {
				cmdutil.ErrorAndExit("error from StopJob: %s", err.Error())
			}
			return nil
		}),
	}
	shell.RegisterCompletionFunc(stopJob, shell.JobCompletion)
	commands = append(commands, cmdutil.CreateAlias(stopJob, "stop job"))

	datumDocs := &cobra.Command{
		Short: "Docs for datums.",
		Long: `Datums are the small independent units of processing for Pachyderm jobs.

A datum is defined by applying a glob pattern (in the pipeline spec) to the file
paths in the input repo. A datum can include one or more files or directories.

Datums within a job will be processed independently, sometimes distributed
across separate workers.  A separate execution of user code will be run for
each datum.`,
	}
	commands = append(commands, cmdutil.CreateDocsAlias(datumDocs, "datum", " datum$"))

	restartDatum := &cobra.Command{
		Use:   "{{alias}} <job> <datum-path1>,<datum-path2>,...",
		Short: "Restart a datum.",
		Long:  "Restart a datum.",
		Run: cmdutil.RunFixedArgs(2, func(args []string) error {
			client, err := pachdclient.NewOnUserMachine("user")
			if err != nil {
				return err
			}
			defer client.Close()
			datumFilter := strings.Split(args[1], ",")
			for i := 0; i < len(datumFilter); {
				if len(datumFilter[i]) == 0 {
					if i+1 < len(datumFilter) {
						copy(datumFilter[i:], datumFilter[i+1:])
					}
					datumFilter = datumFilter[:len(datumFilter)-1]
				} else {
					i++
				}
			}
			return client.RestartDatum(args[0], datumFilter)
		}),
	}
	commands = append(commands, cmdutil.CreateAlias(restartDatum, "restart datum"))
	commands = append(commands, cmdutil.Hide(cmdutil.CreateAlias(restartDatum, "restart datums")))

	var pageSize int64
	var page int64
	var pipelineInputPath string
	listDatum := &cobra.Command{
		Use:   "{{alias}} <job>",
		Short: "Return the datums in a job.",
		Long:  "Return the datums in a job.",
		Run: cmdutil.RunBoundedArgs(0, 1, func(args []string) (retErr error) {
			client, err := pachdclient.NewOnUserMachine("user")
			if err != nil {
				return err
			}
			defer client.Close()
			if pageSize < 0 {
				return errors.Errorf("pageSize must be zero or positive")
			}
			if page < 0 {
				return errors.Errorf("page must be zero or positive")
			}
			var printF func(*ppsclient.DatumInfo) error
			if !raw {
				if output != "" {
					cmdutil.ErrorAndExit("cannot set --output (-o) without --raw")
				}
				writer := tabwriter.NewWriter(os.Stdout, pretty.DatumHeader)
				printF = func(di *ppsclient.DatumInfo) error {
					pretty.PrintDatumInfo(writer, di)
					return nil
				}
				defer func() {
					if err := writer.Flush(); retErr == nil {
						retErr = err
					}
				}()
			} else {
				e := encoder(output)
				printF = func(di *ppsclient.DatumInfo) error {
					return e.EncodeProto(di)
				}
			}
			if pipelineInputPath != "" && len(args) == 1 {
				return errors.Errorf("can't specify both a job and a pipeline spec")
			} else if pipelineInputPath != "" {
				pipelineReader, err := ppsutil.NewPipelineManifestReader(pipelineInputPath)
				if err != nil {
					return err
				}
				request, err := pipelineReader.NextCreatePipelineRequest()
				if err != nil {
					return err
				}
				return client.ListDatumInputF(request.Input, pageSize, page, printF)
			} else if len(args) == 1 {
				return client.ListDatumF(args[0], pageSize, page, printF)
			} else {
				return errors.Errorf("must specify either a job or a pipeline spec")
			}
		}),
	}
	listDatum.Flags().Int64Var(&pageSize, "pageSize", 0, "Specify the number of results sent back in a single page")
	listDatum.Flags().Int64Var(&page, "page", 0, "Specify the page of results to send")
	listDatum.Flags().StringVarP(&pipelineInputPath, "file", "f", "", "The JSON file containing the pipeline to list datums from, the pipeline need not exist")
	listDatum.Flags().AddFlagSet(outputFlags)
	shell.RegisterCompletionFunc(listDatum, shell.JobCompletion)
	commands = append(commands, cmdutil.CreateAlias(listDatum, "list datum"))
	commands = append(commands, cmdutil.Hide(cmdutil.CreateAlias(listDatum, "list datums")))

	inspectDatum := &cobra.Command{
		Use:   "{{alias}} <job> <datum>",
		Short: "Display detailed info about a single datum.",
		Long:  "Display detailed info about a single datum. Requires the pipeline to have stats enabled.",
		Run: cmdutil.RunFixedArgs(2, func(args []string) error {
			client, err := pachdclient.NewOnUserMachine("user")
			if err != nil {
				return err
			}
			defer client.Close()
			datumInfo, err := client.InspectDatum(args[0], args[1])
			if err != nil {
				return err
			}
			if raw {
				return encoder(output).EncodeProto(datumInfo)
			} else if output != "" {
				cmdutil.ErrorAndExit("cannot set --output (-o) without --raw")
			}
			pretty.PrintDetailedDatumInfo(os.Stdout, datumInfo)
			return nil
		}),
	}
	inspectDatum.Flags().AddFlagSet(outputFlags)
	commands = append(commands, cmdutil.CreateAlias(inspectDatum, "inspect datum"))

	var (
		jobID       string
		datumID     string
		commaInputs string // comma-separated list of input files of interest
		master      bool
		worker      bool
		follow      bool
		tail        int64
	)

	// prettyLogsPrinter helps to print the logs recieved in different colours
	prettyLogsPrinter := func(message string) {
		informationArray := strings.Split(message, " ")
		if len(informationArray) > 1 {
			debugString := informationArray[1]
			debugLevel := strings.ToLower(debugString)
			var debugLevelColoredString string
			if debugLevel == "info" {
				debugLevelColoredString = color.New(color.FgGreen).Sprint(debugString)
			} else if debugLevel == "warning" {
				debugLevelColoredString = color.New(color.FgYellow).Sprint(debugString)
			} else if debugLevel == "error" {
				debugLevelColoredString = color.New(color.FgRed).Sprint(debugString)
			} else {
				debugLevelColoredString = debugString
			}
			informationArray[1] = debugLevelColoredString
			coloredMessage := strings.Join(informationArray, " ")
			fmt.Println(coloredMessage)
		} else {
			fmt.Println(message)
		}

	}

	getLogs := &cobra.Command{
		Use:   "{{alias}} [--pipeline=<pipeline>|--job=<job>] [--datum=<datum>]",
		Short: "Return logs from a job.",
		Long:  "Return logs from a job.",
		Example: `
# Return logs emitted by recent jobs in the "filter" pipeline
$ {{alias}} --pipeline=filter

# Return logs emitted by the job aedfa12aedf
$ {{alias}} --job=aedfa12aedf

# Return logs emitted by the pipeline \"filter\" while processing /apple.txt and a file with the hash 123aef
$ {{alias}} --pipeline=filter --inputs=/apple.txt,123aef`,
		Run: cmdutil.RunFixedArgs(0, func(args []string) error {
			client, err := pachdclient.NewOnUserMachine("user")
			if err != nil {
				return errors.Wrapf(err, "error connecting to pachd")
			}
			defer client.Close()

			// Break up comma-separated input paths, and filter out empty entries
			data := strings.Split(commaInputs, ",")
			for i := 0; i < len(data); {
				if len(data[i]) == 0 {
					if i+1 < len(data) {
						copy(data[i:], data[i+1:])
					}
					data = data[:len(data)-1]
				} else {
					i++
				}
			}

			// Issue RPC
			iter := client.GetLogs(pipelineName, jobID, data, datumID, master, follow, tail)
			var buf bytes.Buffer
			encoder := json.NewEncoder(&buf)
			for iter.Next() {
				if raw {
					buf.Reset()
					if err := encoder.Encode(iter.Message()); err != nil {
						fmt.Fprintf(os.Stderr, "error marshalling \"%v\": %s\n", iter.Message(), err)
					}
					fmt.Println(buf.String())
				} else if iter.Message().User && !master && !worker {
					prettyLogsPrinter(iter.Message().Message)
				} else if iter.Message().Master && master {
					prettyLogsPrinter(iter.Message().Message)
				} else if !iter.Message().User && !iter.Message().Master && worker {
					prettyLogsPrinter(iter.Message().Message)
				} else if pipelineName == "" && jobID == "" {
					prettyLogsPrinter(iter.Message().Message)
				}
			}
			return iter.Err()
		}),
	}
	getLogs.Flags().StringVarP(&pipelineName, "pipeline", "p", "", "Filter the log "+
		"for lines from this pipeline (accepts pipeline name)")
	getLogs.MarkFlagCustom("pipeline", "__pachctl_get_pipeline")
	getLogs.Flags().StringVarP(&jobID, "job", "j", "", "Filter for log lines from "+
		"this job (accepts job ID)")
	getLogs.MarkFlagCustom("job", "__pachctl_get_job")
	getLogs.Flags().StringVar(&datumID, "datum", "", "Filter for log lines for this datum (accepts datum ID)")
	getLogs.Flags().StringVar(&commaInputs, "inputs", "", "Filter for log lines "+
		"generated while processing these files (accepts PFS paths or file hashes)")
	getLogs.Flags().BoolVar(&master, "master", false, "Return log messages from the master process (pipeline must be set).")
	getLogs.Flags().BoolVar(&worker, "worker", false, "Return log messages from the worker process.")
	getLogs.Flags().BoolVar(&raw, "raw", false, "Return log messages verbatim from server.")
	getLogs.Flags().BoolVarP(&follow, "follow", "f", false, "Follow logs as more are created.")
	getLogs.Flags().Int64VarP(&tail, "tail", "t", 0, "Lines of recent logs to display.")
	shell.RegisterCompletionFunc(getLogs,
		func(flag, text string, maxCompletions int64) ([]prompt.Suggest, shell.CacheFunc) {
			if flag == "--pipeline" || flag == "-p" {
				cs, cf := shell.PipelineCompletion(flag, text, maxCompletions)
				return cs, shell.AndCacheFunc(cf, shell.SameFlag(flag))
			}
			if flag == "--job" || flag == "-j" {
				cs, cf := shell.JobCompletion(flag, text, maxCompletions)
				return cs, shell.AndCacheFunc(cf, shell.SameFlag(flag))
			}
			return nil, shell.SameFlag(flag)
		})
	commands = append(commands, cmdutil.CreateAlias(getLogs, "logs"))

	pipelineDocs := &cobra.Command{
		Short: "Docs for pipelines.",
		Long: `Pipelines are a powerful abstraction for automating jobs.

Pipelines take a set of repos and branches as inputs and will write to a single
output repo of the same name. Pipelines then subscribe to commits on those repos
and launch a job to process each incoming commit.

All jobs created by a pipeline will create commits in the pipeline's output repo.`,
	}
	commands = append(commands, cmdutil.CreateDocsAlias(pipelineDocs, "pipeline", " pipeline$"))

	var build bool
	var pushImages bool
	var registry string
	var username string
	var pipelinePath string
	createPipeline := &cobra.Command{
		Short: "Create a new pipeline.",
		Long:  "Create a new pipeline from a pipeline specification. For details on the format, see http://docs.pachyderm.io/en/latest/reference/pipeline_spec.html.",
		Run: cmdutil.RunFixedArgs(0, func(args []string) (retErr error) {
			return pipelineHelper(false, build, pushImages, registry, username, pipelinePath, false)
		}),
	}
	createPipeline.Flags().StringVarP(&pipelinePath, "file", "f", "-", "The JSON file containing the pipeline, it can be a url or local file. - reads from stdin.")
	createPipeline.Flags().BoolVarP(&build, "build", "b", false, "If true, build and push local docker images into the docker registry.")
	createPipeline.Flags().BoolVarP(&pushImages, "push-images", "p", false, "If true, push local docker images into the docker registry.")
	createPipeline.Flags().StringVarP(&registry, "registry", "r", "index.docker.io", "The registry to push images to.")
	createPipeline.Flags().StringVarP(&username, "username", "u", "", "The username to push images as.")
	commands = append(commands, cmdutil.CreateAlias(createPipeline, "create pipeline"))
	commands = append(commands, cmdutil.Hide(cmdutil.CreateAlias(createPipeline, "create pipelines")))

	var reprocess bool
	updatePipeline := &cobra.Command{
		Short: "Update an existing Pachyderm pipeline.",
		Long:  "Update a Pachyderm pipeline with a new pipeline specification. For details on the format, see http://docs.pachyderm.io/en/latest/reference/pipeline_spec.html.",
		Run: cmdutil.RunFixedArgs(0, func(args []string) (retErr error) {
			return pipelineHelper(reprocess, build, pushImages, registry, username, pipelinePath, true)
		}),
	}
	updatePipeline.Flags().StringVarP(&pipelinePath, "file", "f", "-", "The JSON file containing the pipeline, it can be a url or local file. - reads from stdin.")
	updatePipeline.Flags().BoolVarP(&build, "build", "b", false, "If true, build and push local docker images into the docker registry.")
	updatePipeline.Flags().BoolVarP(&pushImages, "push-images", "p", false, "If true, push local docker images into the docker registry.")
	updatePipeline.Flags().StringVarP(&registry, "registry", "r", "index.docker.io", "The registry to push images to.")
	updatePipeline.Flags().StringVarP(&username, "username", "u", "", "The username to push images as.")
	updatePipeline.Flags().BoolVar(&reprocess, "reprocess", false, "If true, reprocess datums that were already processed by previous version of the pipeline.")
	commands = append(commands, cmdutil.CreateAlias(updatePipeline, "update pipeline"))
	commands = append(commands, cmdutil.Hide(cmdutil.CreateAlias(updatePipeline, "update pipelines")))

	runPipeline := &cobra.Command{
		Use:   "{{alias}} <pipeline> [<repo>@[<branch>|<commit>|<branch>=<commit>]...]",
		Short: "Run an existing Pachyderm pipeline on the specified commits-branch pairs.",
		Long:  "Run a Pachyderm pipeline on the datums from specific commit-branch pairs. If you only specify a branch, Pachyderm uses the HEAD commit to complete the pair. Similarly, if you only specify a commit, Pachyderm will try to use the branch the commit originated on. Note: Pipelines run automatically when data is committed to them. This command is for the case where you want to run the pipeline on a specific set of data, or if you want to rerun the pipeline. The datums that were successfully processed in previous runs will not be processed unless you specify the --reprocess flag.",
		Example: `
		# Rerun the latest job for the "filter" pipeline
		$ {{alias}} filter

		# Process the pipeline "filter" on the data from commit-branch pairs "repo1@A=a23e4" and "repo2@B=bf363"
		$ {{alias}} filter repo1@A=a23e4 repo2@B=bf363

		# Run the pipeline "filter" on the data from commit "167af5" on the "staging" branch on repo "repo1"
		$ {{alias}} filter repo1@staging=167af5

		# Run the pipeline "filter" on the HEAD commit of the "testing" branch on repo "repo1"
		$ {{alias}} filter repo1@testing

		# Run the pipeline "filter" on the commit "af159e which originated on the "master" branch on repo "repo1"
		$ {{alias}} filter repo1@af159`,

		Run: cmdutil.RunMinimumArgs(1, func(args []string) (retErr error) {
			client, err := pachdclient.NewOnUserMachine("user")
			if err != nil {
				return err
			}
			defer client.Close()
			prov, err := cmdutil.ParseCommitProvenances(args[1:])
			if err != nil {
				return err
			}
			err = client.RunPipeline(args[0], prov, jobID)
			if err != nil {
				return err
			}
			return nil
		}),
	}
	runPipeline.Flags().StringVar(&jobID, "job", "", "rerun the given job")
	commands = append(commands, cmdutil.CreateAlias(runPipeline, "run pipeline"))

	runCron := &cobra.Command{
		Use:   "{{alias}} <pipeline>",
		Short: "Run an existing Pachyderm cron pipeline now",
		Long:  "Run an existing Pachyderm cron pipeline now",
		Example: `
		# Run a cron pipeline "clock" now
		$ {{alias}} clock`,
		Run: cmdutil.RunMinimumArgs(1, func(args []string) (retErr error) {
			client, err := pachdclient.NewOnUserMachine("user")
			if err != nil {
				return err
			}
			defer client.Close()
			err = client.RunCron(args[0])
			if err != nil {
				return err
			}
			return nil
		}),
	}
	commands = append(commands, cmdutil.CreateAlias(runCron, "run cron"))

	inspectPipeline := &cobra.Command{
		Use:   "{{alias}} <pipeline>",
		Short: "Return info about a pipeline.",
		Long:  "Return info about a pipeline.",
		Run: cmdutil.RunFixedArgs(1, func(args []string) error {
			client, err := pachdclient.NewOnUserMachine("user")
			if err != nil {
				return err
			}
			defer client.Close()
			pipelineInfo, err := client.InspectPipeline(args[0])
			if err != nil {
				return err
			}
			if pipelineInfo == nil {
				return errors.Errorf("pipeline %s not found", args[0])
			}
			if raw {
				return encoder(output).EncodeProto(pipelineInfo)
			} else if output != "" {
				cmdutil.ErrorAndExit("cannot set --output (-o) without --raw")
			}
			pi := &pretty.PrintablePipelineInfo{
				PipelineInfo:   pipelineInfo,
				FullTimestamps: fullTimestamps,
			}
			return pretty.PrintDetailedPipelineInfo(os.Stdout, pi)
		}),
	}
	inspectPipeline.Flags().AddFlagSet(outputFlags)
	inspectPipeline.Flags().AddFlagSet(fullTimestampsFlags)
	commands = append(commands, cmdutil.CreateAlias(inspectPipeline, "inspect pipeline"))

	extractPipeline := &cobra.Command{
		Use:   "{{alias}} <pipeline>",
		Short: "Return the manifest used to create a pipeline.",
		Long:  "Return the manifest used to create a pipeline.",
		Run: cmdutil.RunFixedArgs(1, func(args []string) error {
			client, err := pachdclient.NewOnUserMachine("user")
			if err != nil {
				return err
			}
			defer client.Close()
			createPipelineRequest, err := client.ExtractPipeline(args[0])
			if err != nil {
				return err
			}
			return encoder(output).EncodeProto(createPipelineRequest)
		}),
	}
	extractPipeline.Flags().StringVarP(&output, "output", "o", "", "Output format: \"json\" or \"yaml\" (default \"json\")")
	commands = append(commands, cmdutil.CreateAlias(extractPipeline, "extract pipeline"))

	var editor string
	var editorArgs []string
	editPipeline := &cobra.Command{
		Use:   "{{alias}} <pipeline>",
		Short: "Edit the manifest for a pipeline in your text editor.",
		Long:  "Edit the manifest for a pipeline in your text editor.",
		Run: cmdutil.RunFixedArgs(1, func(args []string) (retErr error) {
			client, err := pachdclient.NewOnUserMachine("user")
			if err != nil {
				return err
			}
			defer client.Close()
			createPipelineRequest, err := client.ExtractPipeline(args[0])
			if err != nil {
				return err
			}
			f, err := ioutil.TempFile("", args[0])
			if err != nil {
				return err
			}
			if err := encoder(output, f).EncodeProto(createPipelineRequest); err != nil {
				return err
			}
			defer func() {
				if err := f.Close(); err != nil && retErr == nil {
					retErr = err
				}
			}()
			if editor == "" {
				editor = os.Getenv("EDITOR")
			}
			if editor == "" {
				editor = "vim"
			}
			editorArgs = strings.Split(editor, " ")
			editorArgs = append(editorArgs, f.Name())
			if err := cmdutil.RunIO(cmdutil.IO{
				Stdin:  os.Stdin,
				Stdout: os.Stdout,
				Stderr: os.Stderr,
			}, editorArgs...); err != nil {
				return err
			}
			pipelineReader, err := ppsutil.NewPipelineManifestReader(f.Name())
			if err != nil {
				return err
			}
			request, err := pipelineReader.NextCreatePipelineRequest()
			if err != nil {
				return err
			}
			if proto.Equal(createPipelineRequest, request) {
				fmt.Println("Pipeline unchanged, no update will be performed.")
				return nil
			}
			request.Update = true
			request.Reprocess = reprocess
			if _, err := client.PpsAPIClient.CreatePipeline(
				client.Ctx(),
				request,
			); err != nil {
				return grpcutil.ScrubGRPC(err)
			}
			return nil
		}),
	}
	editPipeline.Flags().BoolVar(&reprocess, "reprocess", false, "If true, reprocess datums that were already processed by previous version of the pipeline.")
	editPipeline.Flags().StringVar(&editor, "editor", "", "Editor to use for modifying the manifest.")
	editPipeline.Flags().StringVarP(&output, "output", "o", "", "Output format: \"json\" or \"yaml\" (default \"json\")")
	commands = append(commands, cmdutil.CreateAlias(editPipeline, "edit pipeline"))

	var spec bool
	listPipeline := &cobra.Command{
		Use:   "{{alias}} [<pipeline>]",
		Short: "Return info about all pipelines.",
		Long:  "Return info about all pipelines.",
		Run: cmdutil.RunBoundedArgs(0, 1, func(args []string) error {
			// validate flags
			if raw && spec {
				return errors.Errorf("cannot set both --raw and --spec")
			} else if !raw && !spec && output != "" {
				cmdutil.ErrorAndExit("cannot set --output (-o) without --raw or --spec")
			}
			history, err := cmdutil.ParseHistory(history)
			if err != nil {
				return errors.Wrapf(err, "error parsing history flag")
			}
			var filter string
			if len(stateStrs) > 0 {
				filter, err = ParsePipelineStates(stateStrs)
				if err != nil {
					return errors.Wrap(err, "error parsing state")
				}
			}
			// init client & get pipeline info
			client, err := pachdclient.NewOnUserMachine("user")
			if err != nil {
				return errors.Wrapf(err, "error connecting to pachd")
			}
			defer client.Close()
			var pipeline string
			if len(args) > 0 {
				pipeline = args[0]
			}
			request := &ppsclient.ListPipelineRequest{History: history, AllowIncomplete: true, JqFilter: filter}
			if pipeline != "" {
				request.Pipeline = pachdclient.NewPipeline(pipeline)
			}
			response, err := client.PpsAPIClient.ListPipeline(client.Ctx(), request)
			if err != nil {
				return grpcutil.ScrubGRPC(err)
			}
			pipelineInfos := response.PipelineInfo
			if raw {
				e := encoder(output)
				for _, pipelineInfo := range pipelineInfos {
					if err := e.EncodeProto(pipelineInfo); err != nil {
						return err
					}
				}
				return nil
			} else if spec {
				e := encoder(output)
				for _, pipelineInfo := range pipelineInfos {
					if err := e.EncodeProto(ppsutil.PipelineReqFromInfo(pipelineInfo)); err != nil {
						return err
					}
				}
				return nil
			}
			for _, pi := range pipelineInfos {
				if ppsutil.ErrorState(pi.State) {
					fmt.Fprintln(os.Stderr, "One or more pipelines have encountered errors, use inspect pipeline to get more info.")
					break
				}
			}
			writer := tabwriter.NewWriter(os.Stdout, pretty.PipelineHeader)
			for _, pipelineInfo := range pipelineInfos {
				pretty.PrintPipelineInfo(writer, pipelineInfo, fullTimestamps)
			}
			return writer.Flush()
		}),
	}
	listPipeline.Flags().BoolVarP(&spec, "spec", "s", false, "Output 'create pipeline' compatibility specs.")
	listPipeline.Flags().AddFlagSet(outputFlags)
	listPipeline.Flags().AddFlagSet(fullTimestampsFlags)
	listPipeline.Flags().StringVar(&history, "history", "none", "Return revision history for pipelines.")
	listPipeline.Flags().StringArrayVar(&stateStrs, "state", []string{}, "Return only pipelines with the specified state. Can be repeated to include multiple states")
	commands = append(commands, cmdutil.CreateAlias(listPipeline, "list pipeline"))
	commands = append(commands, cmdutil.Hide(cmdutil.CreateAlias(listPipeline, "list pipelines")))

	var (
		all              bool
		force            bool
		keepRepo         bool
		splitTransaction bool
	)
	deletePipeline := &cobra.Command{
		Use:   "{{alias}} (<pipeline>|--all)",
		Short: "Delete a pipeline.",
		Long:  "Delete a pipeline.",
		Run: cmdutil.RunBoundedArgs(0, 1, func(args []string) error {
			client, err := pachdclient.NewOnUserMachine("user")
			if err != nil {
				return err
			}
			defer client.Close()
			if len(args) > 0 && all {
				return errors.Errorf("cannot use the --all flag with an argument")
			}
			if len(args) == 0 && !all {
				return errors.Errorf("either a pipeline name or the --all flag needs to be provided")
			}
			if splitTransaction {
				fmt.Println("WARNING: If using the --split-txn flag, this command must run until complete. If a failure or incomplete run occurs, then Pachyderm will be left in an inconsistent state. To resolve an inconsistent state, rerun this command.")
				if ok, err := cmdutil.InteractiveConfirm(); err != nil {
					return err
				} else if !ok {
					return nil
				}
			}
			req := &ppsclient.DeletePipelineRequest{
				All:              all,
				Force:            force,
				KeepRepo:         keepRepo,
				SplitTransaction: splitTransaction,
			}
			if len(args) > 0 {
				req.Pipeline = pachdclient.NewPipeline(args[0])
			}
			if _, err = client.PpsAPIClient.DeletePipeline(client.Ctx(), req); err != nil {
				return grpcutil.ScrubGRPC(err)
			}
			return nil
		}),
	}
	deletePipeline.Flags().BoolVar(&all, "all", false, "delete all pipelines")
	deletePipeline.Flags().BoolVarP(&force, "force", "f", false, "delete the pipeline regardless of errors; use with care")
	deletePipeline.Flags().BoolVar(&keepRepo, "keep-repo", false, "delete the pipeline, but keep the output repo around (the pipeline can be recreated later and use the same repo)")
	deletePipeline.Flags().BoolVar(&splitTransaction, "split-txn", false, "split large transactions into multiple smaller transactions")
	commands = append(commands, cmdutil.CreateAlias(deletePipeline, "delete pipeline"))

	startPipeline := &cobra.Command{
		Use:   "{{alias}} <pipeline>",
		Short: "Restart a stopped pipeline.",
		Long:  "Restart a stopped pipeline.",
		Run: cmdutil.RunFixedArgs(1, func(args []string) error {
			client, err := pachdclient.NewOnUserMachine("user")
			if err != nil {
				return err
			}
			defer client.Close()
			if err := client.StartPipeline(args[0]); err != nil {
				cmdutil.ErrorAndExit("error from StartPipeline: %s", err.Error())
			}
			return nil
		}),
	}
	commands = append(commands, cmdutil.CreateAlias(startPipeline, "start pipeline"))

	stopPipeline := &cobra.Command{
		Use:   "{{alias}} <pipeline>",
		Short: "Stop a running pipeline.",
		Long:  "Stop a running pipeline.",
		Run: cmdutil.RunFixedArgs(1, func(args []string) error {
			client, err := pachdclient.NewOnUserMachine("user")
			if err != nil {
				return err
			}
			defer client.Close()
			if err := client.StopPipeline(args[0]); err != nil {
				cmdutil.ErrorAndExit("error from StopPipeline: %s", err.Error())
			}
			return nil
		}),
	}
	commands = append(commands, cmdutil.CreateAlias(stopPipeline, "stop pipeline"))

	var file string
	createSecret := &cobra.Command{
		Short: "Create a secret on the cluster.",
		Long:  "Create a secret on the cluster.",
		Run: cmdutil.RunFixedArgs(0, func(args []string) (retErr error) {
			client, err := pachdclient.NewOnUserMachine("user")
			if err != nil {
				return err
			}
			defer client.Close()
			fileBytes, err := ioutil.ReadFile(file)
			if err != nil {
				return err
			}

			_, err = client.PpsAPIClient.CreateSecret(
				client.Ctx(),
				&ppsclient.CreateSecretRequest{
					File: fileBytes,
				})

			if err != nil {
				return grpcutil.ScrubGRPC(err)
			}
			return nil
		}),
	}
	createSecret.Flags().StringVarP(&file, "file", "f", "", "File containing Kubernetes secret.")
	commands = append(commands, cmdutil.CreateAlias(createSecret, "create secret"))

	deleteSecret := &cobra.Command{
		Short: "Delete a secret from the cluster.",
		Long:  "Delete a secret from the cluster.",
		Run: cmdutil.RunFixedArgs(1, func(args []string) (retErr error) {
			client, err := pachdclient.NewOnUserMachine("user")
			if err != nil {
				return err
			}
			defer client.Close()

			_, err = client.PpsAPIClient.DeleteSecret(
				client.Ctx(),
				&ppsclient.DeleteSecretRequest{
					Secret: &ppsclient.Secret{
						Name: args[0],
					},
				})

			if err != nil {
				return grpcutil.ScrubGRPC(err)
			}
			return nil
		}),
	}
	commands = append(commands, cmdutil.CreateAlias(deleteSecret, "delete secret"))

	inspectSecret := &cobra.Command{
		Short: "Inspect a secret from the cluster.",
		Long:  "Inspect a secret from the cluster.",
		Run: cmdutil.RunFixedArgs(1, func(args []string) (retErr error) {
			client, err := pachdclient.NewOnUserMachine("user")
			if err != nil {
				return err
			}
			defer client.Close()

			secretInfo, err := client.PpsAPIClient.InspectSecret(
				client.Ctx(),
				&ppsclient.InspectSecretRequest{
					Secret: &ppsclient.Secret{
						Name: args[0],
					},
				})

			if err != nil {
				return grpcutil.ScrubGRPC(err)
			}
			writer := tabwriter.NewWriter(os.Stdout, pretty.SecretHeader)
			pretty.PrintSecretInfo(writer, secretInfo)
			return writer.Flush()
		}),
	}
	commands = append(commands, cmdutil.CreateAlias(inspectSecret, "inspect secret"))

	listSecret := &cobra.Command{
		Short: "List all secrets from a namespace in the cluster.",
		Long:  "List all secrets from a namespace in the cluster.",
		Run: cmdutil.RunFixedArgs(0, func(args []string) (retErr error) {
			client, err := pachdclient.NewOnUserMachine("user")
			if err != nil {
				return err
			}
			defer client.Close()

			secretInfos, err := client.PpsAPIClient.ListSecret(
				client.Ctx(),
				&types.Empty{},
			)

			if err != nil {
				return grpcutil.ScrubGRPC(err)
			}
			writer := tabwriter.NewWriter(os.Stdout, pretty.SecretHeader)
			for _, si := range secretInfos.GetSecretInfo() {
				pretty.PrintSecretInfo(writer, si)
			}
			return writer.Flush()
		}),
	}
	commands = append(commands, cmdutil.CreateAlias(listSecret, "list secret"))
	commands = append(commands, cmdutil.Hide(cmdutil.CreateAlias(listSecret, "list secrets")))

	var memory string
	garbageCollect := &cobra.Command{
		Short: "Garbage collect unused data.",
		Long: `Garbage collect unused data.

When a file/commit/repo is deleted, the data is not immediately removed from
the underlying storage system (e.g. S3) for performance and architectural
reasons.  This is similar to how when you delete a file on your computer, the
file is not necessarily wiped from disk immediately.

To actually remove the data, you will need to manually invoke garbage
collection with "pachctl garbage-collect".

Currently "pachctl garbage-collect" can only be started when there are no
pipelines running.  You also need to ensure that there's no ongoing "put file".
Garbage collection puts the cluster into a readonly mode where no new jobs can
be created and no data can be added.

Pachyderm's garbage collection uses bloom filters to index live objects. This
means that some dead objects may erronously not be deleted during garbage
collection. The probability of this happening depends on how many objects you
have; at around 10M objects it starts to become likely with the default values.
To lower Pachyderm's error rate and make garbage-collection more comprehensive,
you can increase the amount of memory used for the bloom filters with the
--memory flag. The default value is 10MB.
`,
		Run: cmdutil.RunFixedArgs(0, func(args []string) (retErr error) {
			client, err := pachdclient.NewOnUserMachine("user")
			if err != nil {
				return err
			}
			defer client.Close()
			memoryBytes, err := units.RAMInBytes(memory)
			if err != nil {
				return err
			}
			return client.GarbageCollect(memoryBytes)
		}),
	}
	garbageCollect.Flags().StringVarP(&memory, "memory", "m", "0", "The amount of memory to use during garbage collection. Default is 10MB.")
	commands = append(commands, cmdutil.CreateAlias(garbageCollect, "garbage-collect"))

	return commands
}

func pipelineHelper(reprocess bool, build bool, pushImages bool, registry, username, pipelinePath string, update bool) error {
	if build && pushImages {
		logrus.Warning("`--push-images` is redundant, as it's already enabled with `--build`")
	}

	pipelineReader, err := ppsutil.NewPipelineManifestReader(pipelinePath)
	if err != nil {
		return err
	}

	pc, err := pachdclient.NewOnUserMachine("user")
	if err != nil {
		return errors.Wrapf(err, "error connecting to pachd")
	}
	defer pc.Close()

	for {
		request, err := pipelineReader.NextCreatePipelineRequest()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return err
		}

		if request.Pipeline == nil {
			return errors.New("no `pipeline` specified")
		}
		if request.Pipeline.Name == "" {
			return errors.New("no pipeline `name` specified")
		}

		// Add trace if env var is set
		ctx, err := extended.EmbedAnyDuration(pc.Ctx())
		pc = pc.WithCtx(ctx)
		if err != nil {
			logrus.Warning(err)
		}

		if update {
			request.Update = true
			request.Reprocess = reprocess
		}

		isLocal := true
		url, err := url.Parse(pipelinePath)
		if pipelinePath != "-" && err == nil && url.Scheme != "" {
			isLocal = false
		}

		if request.Transform != nil && request.Transform.Build != nil {
			if !isLocal {
				return errors.Errorf("cannot use build step-enabled pipelines that aren't local")
			}
			if request.Spout != nil {
				return errors.New("build step-enabled pipelines do not work with spouts")
			}
			if request.Input == nil {
				return errors.New("no `input` specified")
			}
			if request.Transform.Build.Language == "" && request.Transform.Build.Image == "" {
				return errors.New("must specify either a build `language` or `image`")
			}
			if request.Transform.Build.Language != "" && request.Transform.Build.Image != "" {
				return errors.New("cannot specify both a build `language` and `image`")
			}
			var err error
			ppsclient.VisitInput(request.Input, func(input *ppsclient.Input) {
				inputName := ppsclient.InputName(input)
				if inputName == "build" || inputName == "source" {
					err = errors.New("build step-enabled pipelines cannot have inputs with the name 'build' or 'source', as they are reserved for build assets")
				}
			})
			if err != nil {
				return err
			}
			pipelineParentPath, _ := filepath.Split(pipelinePath)
			if err := buildHelper(pc, request, pipelineParentPath, update); err != nil {
				return err
			}
		} else if build || pushImages {
			if build && !isLocal {
				return errors.Errorf("cannot build pipeline because it is not local")
			}
			if request.Transform == nil {
				return errors.New("must specify a pipeline `transform`")
			}
			pipelineParentPath, _ := filepath.Split(pipelinePath)
			if err := dockerBuildHelper(request, build, registry, username, pipelineParentPath); err != nil {
				return err
			}
		}

		// Don't warn if transform.build is set because latest is almost always
		// legit for build-enabled pipelines.
		if request.Transform != nil && request.Transform.Build == nil && request.Transform.Image != "" {
			if !strings.Contains(request.Transform.Image, ":") {
				fmt.Fprintf(os.Stderr,
					"WARNING: please specify a tag for the docker image in your transform.image spec.\n"+
						"For example, change 'python' to 'python:3' or 'bash' to 'bash:5'. This improves\n"+
						"reproducibility of your pipelines.\n")
			} else if strings.HasSuffix(request.Transform.Image, ":latest") {
				fmt.Fprintf(os.Stderr,
					"WARNING: please do not specify the ':latest' tag for the docker image in your\n"+
						"transform.image spec. For example, change 'python:latest' to 'python:3' or\n"+
						"'bash:latest' to 'bash:5'. This improves reproducibility of your pipelines.\n")
			}
		}

		if _, err := pc.PpsAPIClient.CreatePipeline(
			pc.Ctx(),
			request,
		); err != nil {
			return grpcutil.ScrubGRPC(err)
		}
	}

	return nil
}

func dockerBuildHelper(request *ppsclient.CreatePipelineRequest, build bool, registry, username, pipelineParentPath string) error {
	// create docker client
	dockerClient, err := docker.NewClientFromEnv()
	if err != nil {
		return errors.Wrapf(err, "could not create a docker client from the environment")
	}

	var authConfig docker.AuthConfiguration
	detectedAuthConfig := false

	// try to automatically determine the credentials
	authConfigs, err := docker.NewAuthConfigurationsFromDockerCfg()
	if err == nil {
		for _, ac := range authConfigs.Configs {
			u, err := url.Parse(ac.ServerAddress)
			if err == nil && u.Hostname() == registry && (username == "" || username == ac.Username) {
				authConfig = ac
				detectedAuthConfig = true
				break
			}
		}
	}
	// if that failed, manually build credentials
	if !detectedAuthConfig {
		if username == "" {
			// request the username if it hasn't been specified yet
			fmt.Printf("Username for %s: ", registry)
			reader := bufio.NewReader(os.Stdin)
			username, err = reader.ReadString('\n')
			if err != nil {
				return errors.Wrapf(err, "could not read username")
			}
			username = strings.TrimRight(username, "\r\n")
		}

		// request the password
		password, err := cmdutil.ReadPassword(fmt.Sprintf("Password for %s@%s: ", username, registry))
		if err != nil {
			return errors.Wrapf(err, "could not read password")
		}

		authConfig = docker.AuthConfiguration{
			Username: username,
			Password: password,
		}
	}

	repo, sourceTag := docker.ParseRepositoryTag(request.Transform.Image)
	if sourceTag == "" {
		sourceTag = "latest"
	}
	destTag := uuid.NewWithoutDashes()

	if build {
		dockerfile := request.Transform.Dockerfile
		if dockerfile == "" {
			dockerfile = "./Dockerfile"
		}

		contextDir, dockerfile := filepath.Split(dockerfile)
		if !filepath.IsAbs(contextDir) {
			contextDir = filepath.Join(pipelineParentPath, contextDir)
		}

		destImage := fmt.Sprintf("%s:%s", repo, destTag)

		fmt.Printf("Building %q, this may take a while.\n", destImage)

		err := dockerClient.BuildImage(docker.BuildImageOptions{
			Name:         destImage,
			ContextDir:   contextDir,
			Dockerfile:   dockerfile,
			OutputStream: os.Stdout,
		})
		if err != nil {
			return errors.Wrapf(err, "could not build docker image")
		}

		// Now that we've built into `destTag`, change the
		// `sourceTag` to be the same so that the push will work with
		// the right image
		sourceTag = destTag
	}

	sourceImage := fmt.Sprintf("%s:%s", repo, sourceTag)
	destImage := fmt.Sprintf("%s:%s", repo, destTag)

	fmt.Printf("Tagging/pushing %q, this may take a while.\n", destImage)

	if err := dockerClient.TagImage(sourceImage, docker.TagImageOptions{
		Repo:    repo,
		Tag:     destTag,
		Context: context.Background(),
	}); err != nil {
		return errors.Wrapf(err, "could not tag docker image")
	}

	if err := dockerClient.PushImage(
		docker.PushImageOptions{
			Name: repo,
			Tag:  destTag,
		},
		authConfig,
	); err != nil {
		return errors.Wrapf(err, "could not push docker image")
	}

	request.Transform.Image = destImage
	return nil
}

// TODO: if transactions ever add support for pipeline creation, use them here
// to create everything atomically
func buildHelper(pc *pachdclient.APIClient, request *ppsclient.CreatePipelineRequest, pipelineParentPath string, update bool) error {
	buildPath := request.Transform.Build.Path
	if buildPath == "" {
		buildPath = "."
	}
	if !filepath.IsAbs(buildPath) {
		buildPath = filepath.Join(pipelineParentPath, buildPath)
	}
	if _, err := os.Stat(buildPath); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("build path %q does not exist", buildPath)
		}
		return errors.Wrapf(err, "could not stat build path %q", buildPath)
	}

	buildPipelineName := fmt.Sprintf("%s_build", request.Pipeline.Name)

	image := request.Transform.Build.Image
	if image == "" {
		pachctlVersion := version.PrettyPrintVersion(version.Version)
		image = fmt.Sprintf("pachyderm/%s-build:%s", request.Transform.Build.Language, pachctlVersion)
	}
	if request.Transform.Image == "" {
		request.Transform.Image = image
	}

	// utility function for creating an input used as part of a build step
	createBuildPipelineInput := func(name string) *ppsclient.Input {
		return &ppsclient.Input{
			Pfs: &ppsclient.PFSInput{
				Name:   name,
				Glob:   "/",
				Repo:   buildPipelineName,
				Branch: name,
			},
		}
	}

	// create the source repo
	if err := pc.UpdateRepo(buildPipelineName); err != nil {
		return errors.Wrapf(err, "failed to create repo for build step-enabled pipeline")
	}

	// create the build pipeline
	if err := pc.CreatePipeline(
		buildPipelineName,
		image,
		[]string{"sh", "./build.sh"},
		[]string{},
		&ppsclient.ParallelismSpec{Constant: 1},
		createBuildPipelineInput("source"),
		"build",
		update,
	); err != nil {
		return errors.Wrapf(err, "failed to create build pipeline for build step-enabled pipeline")
	}

	// retrieve ignores (if any)
	ignores := []*glob.Glob{}
	ignorePath := filepath.Join(buildPath, ".pachignore")
	if _, err := os.Stat(ignorePath); err == nil {
		f, err := os.Open(ignorePath)
		if err != nil {
			return errors.Wrapf(err, "failed to read build step ignore file %q", ignorePath)
		}
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			line := scanner.Text()
			g, err := glob.Compile(line)
			if err != nil {
				return errors.Wrapf(err, "build step ignore file %q: failed to compile glob %q", ignorePath, line)
			}
			ignores = append(ignores, g)
		}
	}

	// insert the source code
	pfc, err := pc.NewPutFileClient()
	if err != nil {
		return errors.Wrapf(err, "failed to construct put file client for source code in build step-enabled pipeline")
	}
	if update {
		if err = pfc.DeleteFile(buildPipelineName, "source", "/"); err != nil {
			return errors.Wrapf(err, "failed to delete existing source code for build step-enabled pipeline")
		}
	}
	if err := filepath.Walk(buildPath, func(srcFilePath string, info os.FileInfo, _ error) (retErr error) {
		if info == nil {
			return errors.Errorf("%q doesn't exist", srcFilePath)
		}
		if info.IsDir() {
			return nil
		}

		destFilePath, err := filepath.Rel(buildPath, srcFilePath)
		if err != nil {
			return errors.Wrapf(err, "failed to discover relative path for %s", srcFilePath)
		}
		for _, g := range ignores {
			if g.Match(destFilePath) {
				return nil
			}
		}

		f, err := progress.Open(srcFilePath)
		if err != nil {
			return errors.Wrapf(err, "failed to open file %q for source code in build step-enabled pipeline", srcFilePath)
		}
		defer func() {
			if err := f.Close(); err != nil && retErr == nil {
				retErr = err
			}
		}()

		if _, err = pfc.PutFileOverwrite(buildPipelineName, "source", destFilePath, f, 0); err != nil {
			return errors.Wrapf(err, "failed to put file %q->%q for source code in build step-enabled pipeline", srcFilePath, destFilePath)
		}

		return nil
	}); err != nil {
		return err
	}
	if err := pfc.Close(); err != nil {
		return errors.Wrapf(err, "failed to close put file client for source code in build step-enabled pipeline")
	}

	// modify the pipeline to use the build assets
	request.Input = &ppsclient.Input{
		Cross: []*ppsclient.Input{
			createBuildPipelineInput("source"),
			createBuildPipelineInput("build"),
			request.Input,
		},
	}
	if request.Transform.Cmd == nil || len(request.Transform.Cmd) == 0 {
		request.Transform.Cmd = []string{"sh", "/pfs/build/run.sh"}
	}

	return nil
}

// ByCreationTime is an implementation of sort.Interface which
// sorts pps job info by creation time, ascending.
type ByCreationTime []*ppsclient.JobInfo

func (arr ByCreationTime) Len() int { return len(arr) }

func (arr ByCreationTime) Swap(i, j int) { arr[i], arr[j] = arr[j], arr[i] }

func (arr ByCreationTime) Less(i, j int) bool {
	if arr[i].Started == nil || arr[j].Started == nil {
		return false
	}

	if arr[i].Started.Seconds < arr[j].Started.Seconds {
		return true
	} else if arr[i].Started.Seconds == arr[j].Started.Seconds {
		return arr[i].Started.Nanos < arr[j].Started.Nanos
	}

	return false
}

func validateJQConditionString(filter string) (string, error) {
	q, err := gojq.Parse(filter)
	if err != nil {
		return "", err
	}
	_, err = gojq.Compile(q)
	if err != nil {
		return "", err
	}
	return filter, nil
}

// ParseJobStates parses a slice of state names into a jq filter suitable for ListJob
func ParseJobStates(stateStrs []string) (string, error) {
	var conditions []string
	for _, stateStr := range stateStrs {
		if state, err := ppsclient.JobStateFromName(stateStr); err == nil {
			conditions = append(conditions, fmt.Sprintf(".state == \"%s\"", state))
		} else {
			return "", err
		}
	}
	return validateJQConditionString(strings.Join(conditions, " or "))
}

// ParsePipelineStates parses a slice of state names into a jq filter suitable for ListPipeline
func ParsePipelineStates(stateStrs []string) (string, error) {
	var conditions []string
	for _, stateStr := range stateStrs {
		if state, err := ppsclient.PipelineStateFromName(stateStr); err == nil {
			conditions = append(conditions, fmt.Sprintf(".state == \"%s\"", state))
		} else {
			return "", err
		}
	}
	return validateJQConditionString(strings.Join(conditions, " or "))
}
