package cmdutil

import (
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"text/template"

	"github.com/pachyderm/pachyderm/src/client/pfs"
	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
	"github.com/pachyderm/pachyderm/src/server/pkg/uuid"

	"github.com/spf13/cobra"
)

// PrintErrorStacks should be set to true if you want to print out a stack for
// errors that are returned by the run commands.
var PrintErrorStacks bool

// RunFixedArgs wraps a function in a function
// that checks its exact argument count.
func RunFixedArgs(numArgs int, run func([]string) error) func(*cobra.Command, []string) {
	return func(cmd *cobra.Command, args []string) {
		if len(args) != numArgs {
			fmt.Printf("expected %d arguments, got %d\n\n", numArgs, len(args))
			cmd.Usage()
		} else {
			if err := run(args); err != nil {
				ErrorAndExit("%v", err)
			}
		}
	}
}

// RunCmdFixedArgs wraps a function in a function that checks its exact
// argument count. The only difference between this and RunFixedArgs is that
// this passes in the cobra command.
func RunCmdFixedArgs(numArgs int, run func(*cobra.Command, []string) error) func(*cobra.Command, []string) {
	return func(cmd *cobra.Command, args []string) {
		if len(args) != numArgs {
			fmt.Printf("expected %d arguments, got %d\n\n", numArgs, len(args))
			cmd.Usage()
		} else {
			if err := run(cmd, args); err != nil {
				ErrorAndExit("%v", err)
			}
		}
	}
}

// RunBoundedArgs wraps a function in a function
// that checks its argument count is within a range.
func RunBoundedArgs(min int, max int, run func([]string) error) func(*cobra.Command, []string) {
	return func(cmd *cobra.Command, args []string) {
		if len(args) < min || len(args) > max {
			fmt.Printf("expected %d to %d arguments, got %d\n\n", min, max, len(args))
			cmd.Usage()
		} else {
			if err := run(args); err != nil {
				ErrorAndExit("%v", err)
			}
		}
	}
}

// RunMinimumArgs wraps a function in a function
// that checks its argument count is above a minimum amount
func RunMinimumArgs(min int, run func([]string) error) func(*cobra.Command, []string) {
	return func(cmd *cobra.Command, args []string) {
		if len(args) < min {
			fmt.Printf("expected at least %d arguments, got %d\n\n", min, len(args))
			cmd.Usage()
		} else {
			if err := run(args); err != nil {
				ErrorAndExit("%v", err)
			}
		}
	}
}

// Run makes a new cobra run function that wraps the given function.
func Run(run func(args []string) error) func(*cobra.Command, []string) {
	return func(_ *cobra.Command, args []string) {
		if err := run(args); err != nil {
			ErrorAndExit("%v", err)
		}
	}
}

// ErrorAndExit errors with the given format and args, and then exits.
func ErrorAndExit(format string, args ...interface{}) {
	if errString := strings.TrimSpace(fmt.Sprintf(format, args...)); errString != "" {
		fmt.Fprintf(os.Stderr, "%s\n", errString)
	}
	err, ok := args[0].(error)
	if PrintErrorStacks && ok {
		errors.ForEachStackFrame(err, func(frame errors.Frame) {
			fmt.Fprintf(os.Stderr, "%+v\n", frame)
		})
	}
	os.Exit(1)
}

// ParseCommit takes an argument of the form "repo[@branch-or-commit]" and
// returns the corresponding *pfs.Commit.
func ParseCommit(arg string) (*pfs.Commit, error) {
	parts := strings.SplitN(arg, "@", 2)
	if parts[0] == "" {
		return nil, errors.Errorf("invalid format \"%s\": repo cannot be empty", arg)
	}
	commit := &pfs.Commit{
		Repo: &pfs.Repo{
			Name: parts[0],
		},
		ID: "",
	}
	if len(parts) == 2 {
		commit.ID = parts[1]
	}
	return commit, nil
}

// ParseCommits converts all arguments to *pfs.Commit structs using the
// semantics of ParseCommit
func ParseCommits(args []string) ([]*pfs.Commit, error) {
	var results []*pfs.Commit
	for _, arg := range args {
		commit, err := ParseCommit(arg)
		if err != nil {
			return nil, err
		}
		results = append(results, commit)
	}
	return results, nil
}

// ParseBranch takes an argument of the form "repo[@branch]" and
// returns the corresponding *pfs.Branch.  This uses ParseCommit under the hood
// because a branch name is usually interchangeable with a commit-id.
func ParseBranch(arg string) (*pfs.Branch, error) {
	commit, err := ParseCommit(arg)
	if err != nil {
		return nil, err
	}
	return &pfs.Branch{Repo: commit.Repo, Name: commit.ID}, nil
}

// ParseBranches converts all arguments to *pfs.Commit structs using the
// semantics of ParseBranch
func ParseBranches(args []string) ([]*pfs.Branch, error) {
	var results []*pfs.Branch
	for _, arg := range args {
		branch, err := ParseBranch(arg)
		if err != nil {
			return nil, err
		}
		results = append(results, branch)
	}
	return results, nil
}

// ParseCommitProvenance takes an argument of the form "repo@branch=commit" and
// returns the corresponding *pfs.CommitProvenance.
func ParseCommitProvenance(arg string) (*pfs.CommitProvenance, error) {
	commit, err := ParseCommit(arg)
	if err != nil {
		return nil, err
	}

	branchAndCommit := strings.SplitN(commit.ID, "=", 2)
	if len(branchAndCommit) < 1 {
		return nil, errors.Errorf("invalid format \"%s\": a branch name or branch and commit id must be given", arg)
	}
	branchOrCommit := branchAndCommit[0]
	commitID := branchOrCommit // default to using the head commit once this commit is resolved
	if branchOrCommit == "" {
		return nil, errors.Errorf("invalid format \"%s\": branch cannot be empty", arg)
	}

	if len(branchAndCommit) == 2 {
		commitID = branchAndCommit[1]
	} else if uuid.IsUUIDWithoutDashes(branchOrCommit) {
		commitID = branchOrCommit
		branchOrCommit = ""
	}

	if commitID == "" {
		return nil, errors.Errorf("invalid format \"%s\": commit cannot be empty", arg)
	}

	prov := &pfs.CommitProvenance{
		Branch: &pfs.Branch{
			Repo: commit.Repo,
			Name: branchOrCommit,
		},
		Commit: &pfs.Commit{
			Repo: commit.Repo,
			ID:   commitID,
		},
	}
	return prov, nil
}

// ParseCommitProvenances converts all arguments to *pfs.CommitProvenance structs using the
// semantics of ParseCommitProvenance
func ParseCommitProvenances(args []string) ([]*pfs.CommitProvenance, error) {
	var results []*pfs.CommitProvenance
	for _, arg := range args {
		prov, err := ParseCommitProvenance(arg)
		if err != nil {
			return nil, err
		}
		results = append(results, prov)
	}
	return results, nil
}

// ParseFile takes an argument of the form "repo[@branch-or-commit[:path]]", and
// returns the corresponding *pfs.File.
func ParseFile(arg string) (*pfs.File, error) {
	repoAndRest := strings.SplitN(arg, "@", 2)
	if repoAndRest[0] == "" {
		return nil, errors.Errorf("invalid format \"%s\": repo cannot be empty", arg)
	}
	file := &pfs.File{
		Commit: &pfs.Commit{
			Repo: &pfs.Repo{
				Name: repoAndRest[0],
			},
			ID: "",
		},
		Path: "",
	}
	if len(repoAndRest) > 1 {
		commitAndPath := strings.SplitN(repoAndRest[1], ":", 2)
		if commitAndPath[0] == "" {
			return nil, errors.Errorf("invalid format \"%s\": commit cannot be empty", arg)
		}
		file.Commit.ID = commitAndPath[0]
		if len(commitAndPath) > 1 {
			file.Path = commitAndPath[1]
		}
	}
	return file, nil
}

// ParsePartialFile returns the same thing as ParseFile, unless ParseFile would
// error on this input, in which case it returns as much as it was able to
// parse.
func ParsePartialFile(arg string) *pfs.File {
	file, err := ParseFile(arg)
	if err == nil {
		return file
	}
	partialFile := &pfs.File{}
	commit, err := ParseCommit(arg)
	if err == nil {
		partialFile.Commit = commit
		return partialFile
	}
	partialFile.Commit = &pfs.Commit{Repo: &pfs.Repo{Name: arg}}
	return partialFile
}

// ParseFiles converts all arguments to *pfs.Commit structs using the
// semantics of ParseFile
func ParseFiles(args []string) ([]*pfs.File, error) {
	var results []*pfs.File
	for _, arg := range args {
		commit, err := ParseFile(arg)
		if err != nil {
			return nil, err
		}
		results = append(results, commit)
	}
	return results, nil
}

// ParseHistory parses a --history flag argument. Permissable values are "all"
// "none", and integers greater than or equal to -1 (as strings).
func ParseHistory(history string) (int64, error) {
	if history == "all" {
		return -1, nil
	}
	if history == "none" {
		return 0, nil
	}
	result, err := strconv.Atoi(history)
	return int64(result), err
}

// RepeatedStringArg is an alias for []string
type RepeatedStringArg []string

func (r *RepeatedStringArg) String() string {
	result := "["
	for i, s := range *r {
		if i != 0 {
			result += ", "
		}
		result += s
	}
	return result + "]"
}

// Set adds a string to r
func (r *RepeatedStringArg) Set(s string) error {
	*r = append(*r, s)
	return nil
}

// Type returns the string representation of the type of r
func (r *RepeatedStringArg) Type() string {
	return "[]string"
}

// Hide marks all runnable subcommands as hidden and returns it
// for method chaining.
func Hide(cmd *cobra.Command) *cobra.Command {
	if cmd.Run != nil {
		cmd.Hidden = true
	}
	for _, subcmd := range cmd.Commands() {
		Hide(subcmd)
	}
	return cmd
}

// CreateAlias generates a nested command tree for the invocation specified,
// which should be space-delimited as on the command-line.  The 'Use' field of
// 'cmd' should specify '{{alias}}' instead of the command name as that will be
// filled in based on each invocation.  Similarly, for the 'Example' field,
// '{{alias}}' will be replaced with the full command path.  These commands can
// later be merged into the final Command tree using 'MergeCommands' below.
func CreateAlias(cmd *cobra.Command, invocation string) *cobra.Command {

	// Create logical commands for each substring in each invocation
	var root, prev *cobra.Command
	args := strings.Split(invocation, " ")

	for i, arg := range args {
		cur := &cobra.Command{}

		// The leaf command node should include the usage from the given cmd,
		// while logical nodes just need one piece of the invocation.
		if i == len(args)-1 {
			*cur = *cmd
			if cmd.Use == "" {
				cur.Use = arg
			} else {
				cur.Use = strings.Replace(cmd.Use, "{{alias}}", arg, -1)
			}
			cur.Example = strings.Replace(cmd.Example, "{{alias}}", fmt.Sprintf("%s %s", os.Args[0], invocation), -1)
		} else {
			cur.Use = arg
		}

		if root == nil {
			root = cur
		} else if prev != nil {
			prev.AddCommand(cur)
		}
		prev = cur
	}

	return root
}

// MergeCommands merges several command aliases (generated by 'CreateAlias'
// above) into a single coherent cobra command tree (with root command 'root').
// Because 'CreateAlias' generates empty commands to preserve the right
// hierarchy, we go through a little extra effort to allow intermediate 'docs'
// commands to be preserved in the final command structure.
func MergeCommands(root *cobra.Command, children []*cobra.Command) {
	// Implement our own 'find' function because Command.Find is not reliable?
	findCommand := func(parent *cobra.Command, name string) *cobra.Command {
		for _, cmd := range parent.Commands() {
			if cmd.Name() == name {
				return cmd
			}
		}
		return nil
	}

	// Sort children by max nesting depth - this will put 'docs' commands first,
	// so they are not overwritten by logical commands added by aliases.
	var depth func(*cobra.Command) int
	depth = func(cmd *cobra.Command) int {
		maxDepth := 0
		for _, subcmd := range cmd.Commands() {
			subcmdDepth := depth(subcmd)
			if subcmdDepth > maxDepth {
				maxDepth = subcmdDepth
			}
		}
		return maxDepth + 1
	}

	sort.Slice(children, func(i, j int) bool {
		return depth(children[i]) < depth(children[j])
	})

	// Move each child command over to the main command tree recursively
	for _, cmd := range children {
		parent := findCommand(root, cmd.Name())
		if parent == nil {
			root.AddCommand(cmd)
		} else {
			MergeCommands(parent, cmd.Commands())
		}
	}
}

// CreateDocsAlias sets the usage string for a docs-style command.  Docs
// commands have no functionality except to output some docs and related
// commands, and should not specify a 'Run' attribute.
func CreateDocsAlias(command *cobra.Command, invocation string, pattern string) *cobra.Command {
	// This should create a linked-list-shaped tree, follow it to the one leaf
	root := CreateAlias(command, invocation)
	command = root
	for len(command.Commands()) != 0 {
		command = command.Commands()[0]
	}

	// Normally cobra will not render usage if the command is not runnable or has
	// no subcommands, specify our own help template to override that.
	command.SetHelpTemplate(`{{with (or .Long .Short)}}{{. | trimRightSpace}}

{{end}}{{.UsageString}}`)

	originalUsageFunc := command.UsageFunc()
	command.SetUsageFunc(func(cmd *cobra.Command) error {
		// commands inherit their parents' usage function, so we'll want to pass-
		// through anything but usage for this specific command
		if cmd.CommandPath() != command.CommandPath() {
			return originalUsageFunc(cmd)
		}
		rootCmd := cmd.Root()

		// Walk the command tree, finding commands with the documented word
		var associated []*cobra.Command
		var walk func(*cobra.Command)
		walk = func(cursor *cobra.Command) {
			isMatch, _ := regexp.MatchString(pattern, cursor.CommandPath())
			if isMatch && cursor.CommandPath() != cmd.CommandPath() && cursor.Runnable() {
				associated = append(associated, cursor)
			}
			for _, subcmd := range cursor.Commands() {
				walk(subcmd)
			}
		}
		walk(rootCmd)

		var maxCommandPath int
		for _, x := range associated {
			commandPathLen := len(x.CommandPath())
			if commandPathLen > maxCommandPath {
				maxCommandPath = commandPathLen
			}
		}

		templateFuncs := template.FuncMap{
			"pad": func(s string) string {
				format := fmt.Sprintf("%%-%ds", maxCommandPath+1)
				return fmt.Sprintf(format, s)
			},
			"associated": func() []*cobra.Command {
				return associated
			},
		}

		text := `Associated Commands:{{range associated}}{{if .IsAvailableCommand}}
  {{pad .CommandPath}} {{.Short}}{{end}}{{end}}
`

		t := template.New("top")
		t.Funcs(templateFuncs)
		template.Must(t.Parse(text))
		return t.Execute(cmd.OutOrStderr(), cmd)
	})
	return root
}
