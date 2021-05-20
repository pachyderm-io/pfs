package pps

import (
	"fmt"
	"sort"
	"strings"

	"github.com/pachyderm/pachyderm/v2/src/pfs"

	"github.com/pachyderm/pachyderm/v2/src/internal/errors"
	"gopkg.in/src-d/go-git.v4"
)

var (
	// format strings for state name parsing errors
	errInvalidPipelineJobStateName string
	errInvalidPipelineStateName    string
)

func init() {
	// construct error messages from all current job and pipeline state names
	var states []string
	for i := int32(0); PipelineJobState_name[i] != ""; i++ {
		states = append(states, strings.ToLower(strings.TrimPrefix(PipelineJobState_name[i], "JOB_")))
	}
	errInvalidPipelineJobStateName = fmt.Sprintf("state %%s must be one of %s, or %s, etc", strings.Join(states, ", "), PipelineJobState_name[0])
	states = states[:0]
	for i := int32(0); PipelineState_name[i] != ""; i++ {
		states = append(states, strings.ToLower(strings.TrimPrefix(PipelineState_name[i], "PIPELINE_")))
	}
	errInvalidPipelineStateName = fmt.Sprintf("state %%s must be one of %s, or %s, etc", strings.Join(states, ", "), PipelineState_name[0])
}

// VisitInput visits each input recursively in ascending order (root last)
func VisitInput(input *Input, f func(*Input) error) error {
	var source []*Input
	switch {
	case input == nil:
		return nil // Spouts may have nil input
	case input.Cross != nil:
		source = input.Cross
	case input.Join != nil:
		source = input.Join
	case input.Group != nil:
		source = input.Group
	case input.Union != nil:
		source = input.Union
	}
	for _, input := range source {
		if err := VisitInput(input, f); err != nil {
			return err
		}
	}
	return f(input)
}

// InputName computes the name of an Input.
func InputName(input *Input) string {
	switch {
	case input == nil:
		return ""
	case input.Pfs != nil:
		return input.Pfs.Name
	case input.Cross != nil:
		if len(input.Cross) > 0 {
			return InputName(input.Cross[0])
		}
	case input.Join != nil:
		if len(input.Join) > 0 {
			return InputName(input.Join[0])
		}
	case input.Group != nil:
		if len(input.Group) > 0 {
			return InputName(input.Group[0])
		}
	case input.Union != nil:
		if len(input.Union) > 0 {
			return InputName(input.Union[0])
		}
	}
	return ""
}

// SortInput sorts an Input.
func SortInput(input *Input) {
	VisitInput(input, func(input *Input) error {
		SortInputs := func(inputs []*Input) {
			sort.SliceStable(inputs, func(i, j int) bool { return InputName(inputs[i]) < InputName(inputs[j]) })
		}
		switch {
		case input.Cross != nil:
			SortInputs(input.Cross)
		case input.Join != nil:
			SortInputs(input.Join)
		case input.Group != nil:
			SortInputs(input.Group)
		case input.Union != nil:
			SortInputs(input.Union)
		}
		return nil
	})
}

// InputBranches returns the branches in an Input.
func InputBranches(input *Input) []*pfs.Branch {
	var result []*pfs.Branch
	VisitInput(input, func(input *Input) error {
		if input.Pfs != nil {
			result = append(result, &pfs.Branch{
				Repo: &pfs.Repo{
					Name: input.Pfs.Repo,
					Type: input.Pfs.RepoType,
				},
				Name: input.Pfs.Branch,
			})
		}
		if input.Cron != nil {
			result = append(result, &pfs.Branch{
				Repo: &pfs.Repo{
					Name: input.Cron.Repo,
					Type: pfs.UserRepoType,
				},
				Name: "master",
			})
		}
		if input.Git != nil {
			result = append(result, &pfs.Branch{
				Repo: &pfs.Repo{
					Name: input.Pfs.Repo,
					Type: pfs.UserRepoType,
				},
				Name: input.Git.Branch,
			})
		}
		return nil
	})
	return result
}

// ValidateGitCloneURL returns an error if the provided URL is invalid
func ValidateGitCloneURL(url string) error {
	exampleURL := "https://github.com/org/foo.git"
	if url == "" {
		return errors.Errorf("clone URL is missing (example clone URL %v)", exampleURL)
	}
	// Use the git client's validator to make sure its a valid URL
	o := &git.CloneOptions{
		URL: url,
	}
	if err := o.Validate(); err != nil {
		return err
	}

	// Make sure its the type that we want. Of the following we
	// only accept the 'clone' type of url:
	//     git_url: "git://github.com/sjezewski/testgithook.git",
	//     ssh_url: "git@github.com:sjezewski/testgithook.git",
	//     clone_url: "https://github.com/sjezewski/testgithook.git",
	//     svn_url: "https://github.com/sjezewski/testgithook",

	if !strings.HasSuffix(url, ".git") {
		// svn_url case
		return errors.Errorf("clone URL is missing .git suffix (example clone URL %v)", exampleURL)
	}
	if !strings.HasPrefix(url, "https://") {
		// git_url or ssh_url cases
		return errors.Errorf("clone URL must use https protocol (example clone URL %v)", exampleURL)
	}

	return nil
}

// PipelineJobStateFromName attempts to interpret a string as a
// PipelineJobState, accepting either the enum names or the pretty printed state
// names
func PipelineJobStateFromName(name string) (PipelineJobState, error) {
	canonical := "JOB_" + strings.TrimPrefix(strings.ToUpper(name), "JOB_")
	if value, ok := PipelineJobState_value[canonical]; ok {
		return PipelineJobState(value), nil
	}
	return 0, fmt.Errorf(errInvalidPipelineJobStateName, name)
}

// PipelineStateFromName attempts to interpret a string as a PipelineState,
// accepting either the enum names or the pretty printed state names
func PipelineStateFromName(name string) (PipelineState, error) {
	canonical := "PIPELINE_" + strings.TrimPrefix(strings.ToUpper(name), "PIPELINE_")
	if value, ok := PipelineState_value[canonical]; ok {
		return PipelineState(value), nil
	}
	return 0, fmt.Errorf(errInvalidPipelineStateName, name)
}
