package pretty

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
	"text/template"

	"github.com/docker/go-units"
	"github.com/fatih/color"
	"github.com/gogo/protobuf/types"
	"github.com/pachyderm/pachyderm/src/client"
	ppsclient "github.com/pachyderm/pachyderm/src/client/pps"
	"github.com/pachyderm/pachyderm/src/server/pkg/pretty"
)

// PrintJobHeader prints a job header.
func PrintJobHeader(w io.Writer) {
	// because STATE is a colorful field it has to be at the end of the line,
	// otherwise the terminal escape characters will trip up the tabwriter
	fmt.Fprint(w, "ID\tOUTPUT COMMIT\tSTARTED\tDURATION\tRESTART\tPROGRESS\tDL\tUL\tSTATE\t\n")
}

// PrintJobInfo pretty-prints job info.
func PrintJobInfo(w io.Writer, jobInfo *ppsclient.JobInfo) {
	fmt.Fprintf(w, "%s\t", jobInfo.Job.ID)
	if jobInfo.OutputCommit != nil {
		fmt.Fprintf(w, "%s/%s\t", jobInfo.OutputCommit.Repo.Name, jobInfo.OutputCommit.ID)
	} else if jobInfo.Pipeline != nil {
		fmt.Fprintf(w, "%s/-\t", jobInfo.Pipeline.Name)
	} else {
		fmt.Fprintf(w, "-\t")
	}
	fmt.Fprintf(w, "%s\t", pretty.Ago(jobInfo.Started))
	if jobInfo.Finished != nil {
		fmt.Fprintf(w, "%s\t", pretty.TimeDifference(jobInfo.Started, jobInfo.Finished))
	} else {
		fmt.Fprintf(w, "-\t")
	}
	fmt.Fprintf(w, "%d\t", jobInfo.Restart)
	fmt.Fprintf(w, "%d + %d / %d\t", jobInfo.DataProcessed, jobInfo.DataSkipped, jobInfo.DataTotal)
	fmt.Fprintf(w, "%s\t", pretty.Size(jobInfo.Stats.DownloadBytes))
	fmt.Fprintf(w, "%s\t", pretty.Size(jobInfo.Stats.UploadBytes))
	fmt.Fprintf(w, "%s\t\n", jobState(jobInfo.State))
}

// PrintPipelineHeader prints a pipeline header.
func PrintPipelineHeader(w io.Writer) {
	// because STATE is a colorful field it has to be at the end of the line,
	// otherwise the terminal escape characters will trip up the tabwriter
	fmt.Fprint(w, "NAME\tINPUT\tOUTPUT\tCREATED\tSTATE\t\n")
}

// PrintPipelineInfo pretty-prints pipeline info.
func PrintPipelineInfo(w io.Writer, pipelineInfo *ppsclient.PipelineInfo) {
	fmt.Fprintf(w, "%s\t", pipelineInfo.Pipeline.Name)
	fmt.Fprintf(w, "%s\t", shorthandInput(pipelineInfo.Input))
	fmt.Fprintf(w, "%s/%s\t", pipelineInfo.Pipeline.Name, pipelineInfo.OutputBranch)
	fmt.Fprintf(w, "%s\t", pretty.Ago(pipelineInfo.CreatedAt))
	fmt.Fprintf(w, "%s\t\n", pipelineState(pipelineInfo.State))
}

// PrintJobInputHeader pretty prints a job input header.
func PrintJobInputHeader(w io.Writer) {
	fmt.Fprint(w, "NAME\tREPO\tCOMMIT\tGLOB\tLAZY\t\n")
}

// PrintJobInput pretty-prints a job input.
func PrintJobInput(w io.Writer, jobInput *ppsclient.JobInput) {
	fmt.Fprintf(w, "%s\t", jobInput.Name)
	fmt.Fprintf(w, "%s\t", jobInput.Commit.Repo.Name)
	fmt.Fprintf(w, "%s\t", jobInput.Commit.ID)
	fmt.Fprintf(w, "%s\t", jobInput.Glob)
	fmt.Fprintf(w, "%t\t\n", jobInput.Lazy)
}

// PrintWorkerStatusHeader pretty prints a worker status header.
func PrintWorkerStatusHeader(w io.Writer) {
	fmt.Fprint(w, "WORKER\tJOB\tDATUM\tSTARTED\tQUEUE\t\n")
}

// PrintWorkerStatus pretty prints a worker status.
func PrintWorkerStatus(w io.Writer, workerStatus *ppsclient.WorkerStatus) {
	fmt.Fprintf(w, "%s\t", workerStatus.WorkerID)
	fmt.Fprintf(w, "%s\t", workerStatus.JobID)
	for _, datum := range workerStatus.Data {
		fmt.Fprintf(w, datum.Path)
	}
	fmt.Fprintf(w, "\t")
	fmt.Fprintf(w, "%s\t", pretty.Ago(workerStatus.Started))
	fmt.Fprintf(w, "%d\t\n", workerStatus.QueueSize)
}

// PrintPipelineInputHeader prints a pipeline input header.
func PrintPipelineInputHeader(w io.Writer) {
	fmt.Fprint(w, "NAME\tREPO\tBRANCH\tGLOB\tLAZY\t\n")
}

// PrintPipelineInput pretty-prints a pipeline input.
func PrintPipelineInput(w io.Writer, pipelineInput *ppsclient.PipelineInput) {
	fmt.Fprintf(w, "%s\t", pipelineInput.Name)
	fmt.Fprintf(w, "%s\t", pipelineInput.Repo.Name)
	fmt.Fprintf(w, "%s\t", pipelineInput.Branch)
	fmt.Fprintf(w, "%s\t", pipelineInput.Glob)
	fmt.Fprintf(w, "%t\t\n", pipelineInput.Lazy)
}

// PrintJobCountsHeader prints a job counts header.
func PrintJobCountsHeader(w io.Writer) {
	fmt.Fprintf(w, strings.ToUpper(jobState(ppsclient.JobState_JOB_STARTING))+"\t")
	fmt.Fprintf(w, strings.ToUpper(jobState(ppsclient.JobState_JOB_RUNNING))+"\t")
	fmt.Fprintf(w, strings.ToUpper(jobState(ppsclient.JobState_JOB_FAILURE))+"\t")
	fmt.Fprintf(w, strings.ToUpper(jobState(ppsclient.JobState_JOB_SUCCESS))+"\t\n")
}

// PrintDetailedJobInfo pretty-prints detailed job info.
func PrintDetailedJobInfo(jobInfo *ppsclient.JobInfo) error {
	template, err := template.New("JobInfo").Funcs(funcMap).Parse(
		`ID: {{.Job.ID}} {{if .Pipeline}}
Pipeline: {{.Pipeline.Name}} {{end}} {{if .ParentJob}}
Parent: {{.ParentJob.ID}} {{end}}
Started: {{prettyAgo .Started}} {{if .Finished}}
Duration: {{prettyTimeDifference .Started .Finished}} {{end}}
State: {{jobState .State}}
Reason: {{.Reason}}
Processed: {{.DataProcessed}}
Skipped: {{.DataSkipped}}
Total: {{.DataTotal}}
Data Downloaded: {{prettySize .Stats.DownloadBytes}}
Data Uploaded: {{prettySize .Stats.UploadBytes}}
Download Time: {{prettyDuration .Stats.DownloadTime}}
Process Time: {{prettyDuration .Stats.ProcessTime}}
Upload Time: {{prettyDuration .Stats.UploadTime}}
Worker Status:
{{workerStatus .}}Restarts: {{.Restart}}
ParallelismSpec: {{.ParallelismSpec}}
{{ if .ResourceRequests }}ResourceRequests:
	CPU: {{ .ResourceRequests.Cpu }}
	Memory: {{ .ResourceRequests.Memory }} {{end}}
{{ if .ResourceLimits }}ResourceLimits:
	CPU: {{ .ResourceLimits.Cpu }}
	Memory: {{ .ResourceLimits.Memory }}
	GPU: {{ .ResourceLimits.Gpu }} {{end}}
{{ if .Service }}Service:
	{{ if .Service.InternalPort }}InternalPort: {{ .Service.InternalPort }} {{end}}
	{{ if .Service.ExternalPort }}ExternalPort: {{ .Service.ExternalPort }} {{end}} {{end}}Input:
{{jobInput .}}
Transform:
{{prettyTransform .Transform}} {{if .OutputCommit}}
Output Commit: {{.OutputCommit.ID}} {{end}} {{ if .StatsCommit }}
Stats Commit: {{.StatsCommit.ID}} {{end}} {{ if .Egress }}
Egress: {{.Egress.URL}} {{end}}
`)
	if err != nil {
		return err
	}
	err = template.Execute(os.Stdout, jobInfo)
	if err != nil {
		return err
	}
	return nil
}

// PrintDetailedPipelineInfo pretty-prints detailed pipeline info.
func PrintDetailedPipelineInfo(pipelineInfo *ppsclient.PipelineInfo) error {
	template, err := template.New("PipelineInfo").Funcs(funcMap).Parse(
		`Name: {{.Pipeline.Name}}{{if .Description}}
Description: {{.Description}}{{end}}
Created: {{prettyAgo .CreatedAt}}
State: {{pipelineState .State}}
Reason: {{.Reason}}
Parallelism Spec: {{.ParallelismSpec}}
{{ if .ResourceRequests }}ResourceRequests:
	CPU: {{ .ResourceRequests.Cpu }}
	Memory: {{ .ResourceRequests.Memory }} {{end}}
{{ if .ResourceLimits }}ResourceLimits:
	CPU: {{ .ResourceLimits.Cpu }}
	Memory: {{ .ResourceLimits.Memory }}
	GPU: {{ .ResourceLimits.Gpu }} {{end}}
Input:
{{pipelineInput .}}
Output Branch: {{.OutputBranch}}
Transform:
{{prettyTransform .Transform}}
{{ if .Egress }}Egress: {{.Egress.URL}} {{end}}
{{if .RecentError}} Recent Error: {{.RecentError}} {{end}}
Job Counts:
{{jobCounts .JobCounts}}
`)
	if err != nil {
		return err
	}
	err = template.Execute(os.Stdout, pipelineInfo)
	if err != nil {
		return err
	}
	return nil
}

// PrintDatumInfoHeader prints a file info header.
func PrintDatumInfoHeader(w io.Writer) {
	fmt.Fprint(w, "ID\tSTATUS\tTIME\t\n")
}

// PrintDatumInfo pretty-prints file info.
// If recurse is false and directory size is 0, display "-" instead
// If fast is true and file size is 0, display "-" instead
func PrintDatumInfo(w io.Writer, datumInfo *ppsclient.DatumInfo) {
	totalTime := "-"
	if datumInfo.Stats != nil {
		totalTime = units.HumanDuration(client.GetDatumTotalTime(datumInfo.Stats))
	}
	fmt.Fprintf(w, "%s\t%s\t%s\n", datumInfo.Datum.ID, datumState(datumInfo.State), totalTime)
}

// PrintDetailedDatumInfo pretty-prints detailed info about a datum
func PrintDetailedDatumInfo(w io.Writer, datumInfo *ppsclient.DatumInfo) {
	fmt.Fprintf(w, "ID\t%s\n", datumInfo.Datum.ID)
	fmt.Fprintf(w, "Job ID\t%s\n", datumInfo.Datum.Job.ID)
	fmt.Fprintf(w, "State\t%s\n", datumInfo.State)
	fmt.Fprintf(w, "Data Downloaded\t%s\n", pretty.Size(datumInfo.Stats.DownloadBytes))
	fmt.Fprintf(w, "Data Uploaded\t%s\n", pretty.Size(datumInfo.Stats.UploadBytes))

	totalTime := client.GetDatumTotalTime(datumInfo.Stats).String()
	fmt.Fprintf(w, "Total Time\t%s\n", totalTime)

	var downloadTime string
	dl, err := types.DurationFromProto(datumInfo.Stats.DownloadTime)
	if err != nil {
		downloadTime = err.Error()
	}
	downloadTime = dl.String()
	fmt.Fprintf(w, "Download Time\t%s\n", downloadTime)

	var procTime string
	proc, err := types.DurationFromProto(datumInfo.Stats.ProcessTime)
	if err != nil {
		procTime = err.Error()
	}
	procTime = proc.String()
	fmt.Fprintf(w, "Process Time\t%s\n", procTime)

	var uploadTime string
	ul, err := types.DurationFromProto(datumInfo.Stats.UploadTime)
	if err != nil {
		uploadTime = err.Error()
	}
	uploadTime = ul.String()
	fmt.Fprintf(w, "Upload Time\t%s\n", uploadTime)

	fmt.Fprintf(w, "PFS State:\n")
}

// PrintDatumPfsStateHeader prints the header for the PfsState field
func PrintDatumPfsStateHeader(w io.Writer) {
	fmt.Fprintf(w, "  REPO\tCOMMIT\tPATH\t\n")
}

// PrintDatumPfsState prints the values of the PfsState field
func PrintDatumPfsState(w io.Writer, datumInfo *ppsclient.DatumInfo) {
	fmt.Fprintf(w, "  %s\t%s\t%s\t\n", datumInfo.PfsState.Commit.Repo.Name, datumInfo.PfsState.Commit.ID, datumInfo.PfsState.Path)
}

func datumState(datumState ppsclient.DatumState) string {
	switch datumState {
	case ppsclient.DatumState_SKIPPED:
		return color.New(color.FgYellow).SprintFunc()("skipped")
	case ppsclient.DatumState_FAILED:
		return color.New(color.FgRed).SprintFunc()("failed")
	case ppsclient.DatumState_SUCCESS:
		return color.New(color.FgGreen).SprintFunc()("success")
	}
	return "-"
}

func jobState(jobState ppsclient.JobState) string {
	switch jobState {
	case ppsclient.JobState_JOB_STARTING:
		return color.New(color.FgYellow).SprintFunc()("starting")
	case ppsclient.JobState_JOB_RUNNING:
		return color.New(color.FgYellow).SprintFunc()("running")
	case ppsclient.JobState_JOB_FAILURE:
		return color.New(color.FgRed).SprintFunc()("failure")
	case ppsclient.JobState_JOB_SUCCESS:
		return color.New(color.FgGreen).SprintFunc()("success")
	case ppsclient.JobState_JOB_KILLED:
		return color.New(color.FgYellow).SprintFunc()("killed")
	}
	return "-"
}

func pipelineState(pipelineState ppsclient.PipelineState) string {
	switch pipelineState {
	case ppsclient.PipelineState_PIPELINE_STARTING:
		return color.New(color.FgYellow).SprintFunc()("starting")
	case ppsclient.PipelineState_PIPELINE_RUNNING:
		return color.New(color.FgGreen).SprintFunc()("running")
	case ppsclient.PipelineState_PIPELINE_RESTARTING:
		return color.New(color.FgYellow).SprintFunc()("restarting")
	case ppsclient.PipelineState_PIPELINE_FAILURE:
		return color.New(color.FgRed).SprintFunc()("failure")
	case ppsclient.PipelineState_PIPELINE_PAUSED:
		return color.New(color.FgYellow).SprintFunc()("paused")
	}
	return "-"
}

func jobInput(jobInfo *ppsclient.JobInfo) string {
	if jobInfo.Input == nil {
		return ""
	}
	input, err := json.MarshalIndent(jobInfo.Input, "", "  ")
	if err != nil {
		panic(fmt.Errorf("error marshalling input: %+v", err))
	}
	return string(input) + "\n"
}

func workerStatus(jobInfo *ppsclient.JobInfo) string {
	var buffer bytes.Buffer
	writer := tabwriter.NewWriter(&buffer, 20, 1, 3, ' ', 0)
	PrintWorkerStatusHeader(writer)
	for _, workerStatus := range jobInfo.WorkerStatus {
		PrintWorkerStatus(writer, workerStatus)
	}
	// can't error because buffer can't error on Write
	writer.Flush()
	return buffer.String()
}

func pipelineInput(pipelineInfo *ppsclient.PipelineInfo) string {
	if pipelineInfo.Input == nil {
		return ""
	}
	input, err := json.MarshalIndent(pipelineInfo.Input, "", "  ")
	if err != nil {
		panic(fmt.Errorf("error marshalling input: %+v", err))
	}
	return string(input) + "\n"
}

func jobCounts(counts map[int32]int32) string {
	var buffer bytes.Buffer
	for i := int32(ppsclient.JobState_JOB_STARTING); i <= int32(ppsclient.JobState_JOB_SUCCESS); i++ {
		fmt.Fprintf(&buffer, "%s: %d\t", jobState(ppsclient.JobState(i)), counts[i])
	}
	return buffer.String()
}

func prettyTransform(transform *ppsclient.Transform) (string, error) {
	result, err := json.MarshalIndent(transform, "", "  ")
	if err != nil {
		return "", err
	}
	return pretty.UnescapeHTML(string(result)), nil
}

func shorthandInput(input *ppsclient.Input) string {
	switch {
	case input.Atom != nil:
		return fmt.Sprintf("%s:%s", input.Atom.Repo, input.Atom.Glob)
	case input.Cross != nil:
		var subInput []string
		for _, input := range input.Cross {
			subInput = append(subInput, shorthandInput(input))
		}
		return "(" + strings.Join(subInput, " ⨯ ") + ")"
	case input.Union != nil:
		var subInput []string
		for _, input := range input.Union {
			subInput = append(subInput, shorthandInput(input))
		}
		return "(" + strings.Join(subInput, " ∪ ") + ")"
	case input.Cron != nil:
		return fmt.Sprintf("%s:%s", input.Cron.Name, input.Cron.Spec)
	}
	return ""
}

var funcMap = template.FuncMap{
	"pipelineState":        pipelineState,
	"jobState":             jobState,
	"datumState":           datumState,
	"workerStatus":         workerStatus,
	"pipelineInput":        pipelineInput,
	"jobInput":             jobInput,
	"prettyAgo":            pretty.Ago,
	"prettyTimeDifference": pretty.TimeDifference,
	"prettyDuration":       pretty.Duration,
	"prettySize":           pretty.Size,
	"jobCounts":            jobCounts,
	"prettyTransform":      prettyTransform,
}
