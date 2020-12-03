package cmds

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	gosync "sync"

	prompt "github.com/c-bata/go-prompt"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/pachyderm/pachyderm/src/client"
	"github.com/pachyderm/pachyderm/src/client/limit"
	pfsclient "github.com/pachyderm/pachyderm/src/client/pfs"
	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
	"github.com/pachyderm/pachyderm/src/client/pkg/grpcutil"
	"github.com/pachyderm/pachyderm/src/server/cmd/pachctl/shell"
	"github.com/pachyderm/pachyderm/src/server/pfs/pretty"
	"github.com/pachyderm/pachyderm/src/server/pkg/cmdutil"
	"github.com/pachyderm/pachyderm/src/server/pkg/errutil"
	"github.com/pachyderm/pachyderm/src/server/pkg/pager"
	"github.com/pachyderm/pachyderm/src/server/pkg/ppsconsts"
	"github.com/pachyderm/pachyderm/src/server/pkg/progress"
	"github.com/pachyderm/pachyderm/src/server/pkg/sync"
	"github.com/pachyderm/pachyderm/src/server/pkg/tabwriter"
	txncmds "github.com/pachyderm/pachyderm/src/server/transaction/cmds"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"golang.org/x/sync/errgroup"
)

const (
	// DefaultParallelism is the default parallelism used by 'get file' and 'put file'.
	DefaultParallelism = 10
)

// Cmds returns a slice containing pfs commands.
func Cmds() []*cobra.Command {
	var commands []*cobra.Command

	raw := false
	rawFlags := pflag.NewFlagSet("", pflag.ContinueOnError)
	rawFlags.BoolVar(&raw, "raw", false, "disable pretty printing, print raw json")

	fullTimestamps := false
	fullTimestampsFlags := pflag.NewFlagSet("", pflag.ContinueOnError)
	fullTimestampsFlags.BoolVar(&fullTimestamps, "full-timestamps", false, "Return absolute timestamps (as opposed to the default, relative timestamps).")

	noPager := false
	noPagerFlags := pflag.NewFlagSet("", pflag.ContinueOnError)
	noPagerFlags.BoolVar(&noPager, "no-pager", false, "Don't pipe output into a pager (i.e. less).")

	marshaller := &jsonpb.Marshaler{Indent: "  "}

	repoDocs := &cobra.Command{
		Short: "Docs for repos.",
		Long: `Repos, short for repository, are the top level data objects in Pachyderm.

Repos contain version-controlled directories and files. Files can be of any size
or type (e.g. csv, binary, images, etc).`,
	}
	commands = append(commands, cmdutil.CreateDocsAlias(repoDocs, "repo", " repo$"))

	var description string
	createRepo := &cobra.Command{
		Use:   "{{alias}} <repo>",
		Short: "Create a new repo.",
		Long:  "Create a new repo.",
		Run: cmdutil.RunFixedArgs(1, func(args []string) error {
			c, err := client.NewOnUserMachine("user")
			if err != nil {
				return err
			}
			defer c.Close()

			err = txncmds.WithActiveTransaction(c, func(c *client.APIClient) error {
				_, err = c.PfsAPIClient.CreateRepo(
					c.Ctx(),
					&pfsclient.CreateRepoRequest{
						Repo:        client.NewRepo(args[0]),
						Description: description,
					},
				)
				return err
			})
			return grpcutil.ScrubGRPC(err)
		}),
	}
	createRepo.Flags().StringVarP(&description, "description", "d", "", "A description of the repo.")
	commands = append(commands, cmdutil.CreateAlias(createRepo, "create repo"))

	updateRepo := &cobra.Command{
		Use:   "{{alias}} <repo>",
		Short: "Update a repo.",
		Long:  "Update a repo.",
		Run: cmdutil.RunFixedArgs(1, func(args []string) error {
			c, err := client.NewOnUserMachine("user")
			if err != nil {
				return err
			}
			defer c.Close()

			err = txncmds.WithActiveTransaction(c, func(c *client.APIClient) error {
				_, err = c.PfsAPIClient.CreateRepo(
					c.Ctx(),
					&pfsclient.CreateRepoRequest{
						Repo:        client.NewRepo(args[0]),
						Description: description,
						Update:      true,
					},
				)
				return err
			})
			return grpcutil.ScrubGRPC(err)
		}),
	}
	updateRepo.Flags().StringVarP(&description, "description", "d", "", "A description of the repo.")
	shell.RegisterCompletionFunc(updateRepo, shell.RepoCompletion)
	commands = append(commands, cmdutil.CreateAlias(updateRepo, "update repo"))

	inspectRepo := &cobra.Command{
		Use:   "{{alias}} <repo>",
		Short: "Return info about a repo.",
		Long:  "Return info about a repo.",
		Run: cmdutil.RunFixedArgs(1, func(args []string) error {
			c, err := client.NewOnUserMachine("user")
			if err != nil {
				return err
			}
			defer c.Close()
			repoInfo, err := c.InspectRepo(args[0])
			if err != nil {
				return err
			}
			if repoInfo == nil {
				return errors.Errorf("repo %s not found", args[0])
			}
			if raw {
				return marshaller.Marshal(os.Stdout, repoInfo)
			}
			ri := &pretty.PrintableRepoInfo{
				RepoInfo:       repoInfo,
				FullTimestamps: fullTimestamps,
			}
			return pretty.PrintDetailedRepoInfo(ri)
		}),
	}
	inspectRepo.Flags().AddFlagSet(rawFlags)
	inspectRepo.Flags().AddFlagSet(fullTimestampsFlags)
	shell.RegisterCompletionFunc(inspectRepo, shell.RepoCompletion)
	commands = append(commands, cmdutil.CreateAlias(inspectRepo, "inspect repo"))

	listRepo := &cobra.Command{
		Short: "Return all repos.",
		Long:  "Return all repos.",
		Run: cmdutil.RunFixedArgs(0, func(args []string) error {
			c, err := client.NewOnUserMachine("user")
			if err != nil {
				return err
			}
			defer c.Close()
			repoInfos, err := c.ListRepo()
			if err != nil {
				return err
			}
			if raw {
				for _, repoInfo := range repoInfos {
					if err := marshaller.Marshal(os.Stdout, repoInfo); err != nil {
						return err
					}
				}
				return nil
			}

			header := pretty.RepoHeader
			if (len(repoInfos) > 0) && (repoInfos[0].AuthInfo != nil) {
				header = pretty.RepoAuthHeader
			}
			writer := tabwriter.NewWriter(os.Stdout, header)
			for _, repoInfo := range repoInfos {
				pretty.PrintRepoInfo(writer, repoInfo, fullTimestamps)
			}
			return writer.Flush()
		}),
	}
	listRepo.Flags().AddFlagSet(rawFlags)
	listRepo.Flags().AddFlagSet(fullTimestampsFlags)
	commands = append(commands, cmdutil.CreateAlias(listRepo, "list repo"))
	commands = append(commands, cmdutil.Hide(cmdutil.CreateAlias(listRepo, "list repos")))

	var force bool
	var all bool
	var splitTransaction bool
	deleteRepo := &cobra.Command{
		Use:   "{{alias}} <repo>",
		Short: "Delete a repo.",
		Long:  "Delete a repo.",
		Run: cmdutil.RunBoundedArgs(0, 1, func(args []string) error {
			c, err := client.NewOnUserMachine("user")
			if err != nil {
				return err
			}
			defer c.Close()

			request := &pfsclient.DeleteRepoRequest{
				Force:            force,
				All:              all,
				SplitTransaction: splitTransaction,
			}
			if len(args) > 0 {
				if all {
					return errors.Errorf("cannot use the --all flag with an argument")
				}
				request.Repo = client.NewRepo(args[0])
			} else if !all {
				return errors.Errorf("either a repo name or the --all flag needs to be provided")
			}
			if splitTransaction {
				fmt.Println("WARNING: If using the --split-txn flag, this command must run until complete. If a failure or incomplete run occurs, then Pachyderm will be left in an inconsistent state. To resolve an inconsistent state, rerun this command.")
				if ok, err := cmdutil.InteractiveConfirm(); err != nil {
					return err
				} else if !ok {
					return nil
				}
			}

			err = txncmds.WithActiveTransaction(c, func(c *client.APIClient) error {
				_, err = c.PfsAPIClient.DeleteRepo(c.Ctx(), request)
				return err
			})
			return grpcutil.ScrubGRPC(err)
		}),
	}
	deleteRepo.Flags().BoolVarP(&force, "force", "f", false, "remove the repo regardless of errors; use with care")
	deleteRepo.Flags().BoolVar(&all, "all", false, "remove all repos")
	deleteRepo.Flags().BoolVar(&splitTransaction, "split-txn", false, "split large transactions into multiple smaller transactions")
	shell.RegisterCompletionFunc(deleteRepo, shell.RepoCompletion)
	commands = append(commands, cmdutil.CreateAlias(deleteRepo, "delete repo"))

	commitDocs := &cobra.Command{
		Short: "Docs for commits.",
		Long: `Commits are atomic transactions on the content of a repo.

Creating a commit is a multistep process:
- start a new commit with 'start commit'
- write files to the commit via 'put file'
- finish the new commit with 'finish commit'

Commits that have been started but not finished are NOT durable storage.
Commits become reliable (and immutable) when they are finished.

Commits can be created with another commit as a parent.`,
	}
	commands = append(commands, cmdutil.CreateDocsAlias(commitDocs, "commit", " commit$"))

	var parent string
	startCommit := &cobra.Command{
		Use:   "{{alias}} <repo>@<branch-or-commit>",
		Short: "Start a new commit.",
		Long:  "Start a new commit with parent-commit as the parent, or start a commit on the given branch; if the branch does not exist, it will be created.",
		Example: `# Start a new commit in repo "test" that's not on any branch
$ {{alias}} test

# Start a commit in repo "test" on branch "master"
$ {{alias}} test@master

# Start a commit with "master" as the parent in repo "test", on a new branch "patch"; essentially a fork.
$ {{alias}} test@patch -p master

# Start a commit with XXX as the parent in repo "test", not on any branch
$ {{alias}} test -p XXX`,
		Run: cmdutil.RunFixedArgs(1, func(args []string) error {
			branch, err := cmdutil.ParseBranch(args[0])
			if err != nil {
				return err
			}
			c, err := newClient("user")
			if err != nil {
				return err
			}
			defer c.Close()

			var commit *pfsclient.Commit
			err = txncmds.WithActiveTransaction(c, func(c *client.APIClient) error {
				var err error
				commit, err = c.PfsAPIClient.StartCommit(
					c.Ctx(),
					&pfsclient.StartCommitRequest{
						Branch:      branch.Name,
						Parent:      client.NewCommit(branch.Repo.Name, parent),
						Description: description,
					},
				)
				return err
			})
			if err == nil {
				fmt.Println(commit.ID)
			}
			return grpcutil.ScrubGRPC(err)
		}),
	}
	startCommit.Flags().StringVarP(&parent, "parent", "p", "", "The parent of the new commit, unneeded if branch is specified and you want to use the previous head of the branch as the parent.")
	startCommit.MarkFlagCustom("parent", "__pachctl_get_commit $(__parse_repo ${nouns[0]})")
	startCommit.Flags().StringVarP(&description, "message", "m", "", "A description of this commit's contents")
	startCommit.Flags().StringVar(&description, "description", "", "A description of this commit's contents (synonym for --message)")
	shell.RegisterCompletionFunc(startCommit, shell.BranchCompletion)
	commands = append(commands, cmdutil.CreateAlias(startCommit, "start commit"))

	finishCommit := &cobra.Command{
		Use:   "{{alias}} <repo>@<branch-or-commit>",
		Short: "Finish a started commit.",
		Long:  "Finish a started commit. Commit-id must be a writeable commit.",
		Run: cmdutil.RunFixedArgs(1, func(args []string) error {
			commit, err := cmdutil.ParseCommit(args[0])
			if err != nil {
				return err
			}
			c, err := newClient("user")
			if err != nil {
				return err
			}
			defer c.Close()

			err = txncmds.WithActiveTransaction(c, func(c *client.APIClient) error {
				_, err = c.PfsAPIClient.FinishCommit(
					c.Ctx(),
					&pfsclient.FinishCommitRequest{
						Commit:      commit,
						Description: description,
					},
				)
				return err
			})
			return grpcutil.ScrubGRPC(err)
		}),
	}
	finishCommit.Flags().StringVarP(&description, "message", "m", "", "A description of this commit's contents (overwrites any existing commit description)")
	finishCommit.Flags().StringVar(&description, "description", "", "A description of this commit's contents (synonym for --message)")
	shell.RegisterCompletionFunc(finishCommit, shell.BranchCompletion)
	commands = append(commands, cmdutil.CreateAlias(finishCommit, "finish commit"))

	inspectCommit := &cobra.Command{
		Use:   "{{alias}} <repo>@<branch-or-commit>",
		Short: "Return info about a commit.",
		Long:  "Return info about a commit.",
		Run: cmdutil.RunFixedArgs(1, func(args []string) error {
			commit, err := cmdutil.ParseCommit(args[0])
			if err != nil {
				return err
			}
			c, err := client.NewOnUserMachine("user")
			if err != nil {
				return err
			}
			defer c.Close()

			commitInfo, err := c.InspectCommit(commit.Repo.Name, commit.ID)
			if err != nil {
				return err
			}
			if commitInfo == nil {
				return errors.Errorf("commit %s not found", commit.ID)
			}
			if raw {
				return marshaller.Marshal(os.Stdout, commitInfo)
			}
			ci := &pretty.PrintableCommitInfo{
				CommitInfo:     commitInfo,
				FullTimestamps: fullTimestamps,
			}
			return pretty.PrintDetailedCommitInfo(ci)
		}),
	}
	inspectCommit.Flags().AddFlagSet(rawFlags)
	inspectCommit.Flags().AddFlagSet(fullTimestampsFlags)
	shell.RegisterCompletionFunc(inspectCommit, shell.BranchCompletion)
	commands = append(commands, cmdutil.CreateAlias(inspectCommit, "inspect commit"))

	var from string
	var number int
	listCommit := &cobra.Command{
		Use:   "{{alias}} <repo>[@<branch>]",
		Short: "Return all commits on a repo.",
		Long:  "Return all commits on a repo.",
		Example: `
# return commits in repo "foo"
$ {{alias}} foo

# return commits in repo "foo" on branch "master"
$ {{alias}} foo@master

# return the last 20 commits in repo "foo" on branch "master"
$ {{alias}} foo@master -n 20

# return commits in repo "foo" since commit XXX
$ {{alias}} foo@master --from XXX`,
		Run: cmdutil.RunFixedArgs(1, func(args []string) (retErr error) {
			c, err := client.NewOnUserMachine("user")
			if err != nil {
				return err
			}
			defer c.Close()

			branch, err := cmdutil.ParseBranch(args[0])
			if err != nil {
				return err
			}

			if raw {
				return c.ListCommitF(branch.Repo.Name, branch.Name, from, uint64(number), false, func(ci *pfsclient.CommitInfo) error {
					return marshaller.Marshal(os.Stdout, ci)
				})
			}
			writer := tabwriter.NewWriter(os.Stdout, pretty.CommitHeader)
			if err := c.ListCommitF(branch.Repo.Name, branch.Name, from, uint64(number), false, func(ci *pfsclient.CommitInfo) error {
				pretty.PrintCommitInfo(writer, ci, fullTimestamps)
				return nil
			}); err != nil {
				return err
			}
			return writer.Flush()
		}),
	}
	listCommit.Flags().StringVarP(&from, "from", "f", "", "list all commits since this commit")
	listCommit.Flags().IntVarP(&number, "number", "n", 0, "list only this many commits; if set to zero, list all commits")
	listCommit.MarkFlagCustom("from", "__pachctl_get_commit $(__parse_repo ${nouns[0]})")
	listCommit.Flags().AddFlagSet(rawFlags)
	listCommit.Flags().AddFlagSet(fullTimestampsFlags)
	shell.RegisterCompletionFunc(listCommit, shell.RepoCompletion)
	commands = append(commands, cmdutil.CreateAlias(listCommit, "list commit"))
	commands = append(commands, cmdutil.Hide(cmdutil.CreateAlias(listCommit, "list commits")))

	printCommitIter := func(commitIter client.CommitInfoIterator) error {
		if raw {
			for {
				commitInfo, err := commitIter.Next()
				if errors.Is(err, io.EOF) {
					return nil
				}
				if err != nil {
					return err
				}
				if err := marshaller.Marshal(os.Stdout, commitInfo); err != nil {
					return err
				}
			}
		}
		writer := tabwriter.NewWriter(os.Stdout, pretty.CommitHeader)
		for {
			commitInfo, err := commitIter.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return err
			}
			pretty.PrintCommitInfo(writer, commitInfo, fullTimestamps)
		}
		return writer.Flush()
	}

	var repos cmdutil.RepeatedStringArg
	flushCommit := &cobra.Command{
		Use:   "{{alias}} <repo>@<branch-or-commit> ...",
		Short: "Wait for all commits caused by the specified commits to finish and return them.",
		Long:  "Wait for all commits caused by the specified commits to finish and return them.",
		Example: `
# return commits caused by foo@XXX and bar@YYY
$ {{alias}} foo@XXX bar@YYY

# return commits caused by foo@XXX leading to repos bar and baz
$ {{alias}} foo@XXX -r bar -r baz`,
		Run: cmdutil.Run(func(args []string) error {
			commits, err := cmdutil.ParseCommits(args)
			if err != nil {
				return err
			}

			c, err := client.NewOnUserMachine("user")
			if err != nil {
				return err
			}
			defer c.Close()

			var toRepos []*pfsclient.Repo
			for _, repoName := range repos {
				toRepos = append(toRepos, client.NewRepo(repoName))
			}

			commitIter, err := c.FlushCommit(commits, toRepos)
			if err != nil {
				return err
			}

			return printCommitIter(commitIter)
		}),
	}
	flushCommit.Flags().VarP(&repos, "repos", "r", "Wait only for commits leading to a specific set of repos")
	flushCommit.MarkFlagCustom("repos", "__pachctl_get_repo")
	flushCommit.Flags().AddFlagSet(rawFlags)
	flushCommit.Flags().AddFlagSet(fullTimestampsFlags)
	shell.RegisterCompletionFunc(flushCommit, shell.BranchCompletion)
	commands = append(commands, cmdutil.CreateAlias(flushCommit, "flush commit"))
	commands = append(commands, cmdutil.Hide(cmdutil.CreateAlias(flushCommit, "flush commits")))

	var newCommits bool
	var pipeline string
	subscribeCommit := &cobra.Command{
		Use:   "{{alias}} <repo>@<branch>",
		Short: "Print commits as they are created (finished).",
		Long:  "Print commits as they are created in the specified repo and branch.  By default, all existing commits on the specified branch are returned first.  A commit is only considered 'created' when it's been finished.",
		Example: `
# subscribe to commits in repo "test" on branch "master"
$ {{alias}} test@master

# subscribe to commits in repo "test" on branch "master", but only since commit XXX.
$ {{alias}} test@master --from XXX

# subscribe to commits in repo "test" on branch "master", but only for new commits created from now on.
$ {{alias}} test@master --new`,
		Run: cmdutil.RunFixedArgs(1, func(args []string) error {
			branch, err := cmdutil.ParseBranch(args[0])
			if err != nil {
				return err
			}
			c, err := client.NewOnUserMachine("user")
			if err != nil {
				return err
			}
			defer c.Close()

			if newCommits && from != "" {
				return errors.Errorf("--new and --from cannot be used together")
			}

			if newCommits {
				from = branch.Name
			}

			var prov *pfsclient.CommitProvenance
			if pipeline != "" {
				pipelineInfo, err := c.InspectPipeline(pipeline)
				if err != nil {
					return err
				}
				prov = client.NewCommitProvenance(ppsconsts.SpecRepo, pipeline, pipelineInfo.SpecCommit.ID)
			}

			commitIter, err := c.SubscribeCommit(branch.Repo.Name, branch.Name, prov, from, pfsclient.CommitState_STARTED)
			if err != nil {
				return err
			}

			return printCommitIter(commitIter)
		}),
	}
	subscribeCommit.Flags().StringVar(&from, "from", "", "subscribe to all commits since this commit")
	subscribeCommit.Flags().StringVar(&pipeline, "pipeline", "", "subscribe to all commits created by this pipeline")
	subscribeCommit.MarkFlagCustom("from", "__pachctl_get_commit $(__parse_repo ${nouns[0]})")
	subscribeCommit.Flags().BoolVar(&newCommits, "new", false, "subscribe to only new commits created from now on")
	subscribeCommit.Flags().AddFlagSet(rawFlags)
	subscribeCommit.Flags().AddFlagSet(fullTimestampsFlags)
	shell.RegisterCompletionFunc(subscribeCommit, shell.BranchCompletion)
	commands = append(commands, cmdutil.CreateAlias(subscribeCommit, "subscribe commit"))
	commands = append(commands, cmdutil.Hide(cmdutil.CreateAlias(subscribeCommit, "subscribe commits")))

	deleteCommit := &cobra.Command{
		Use:   "{{alias}} <repo>@<branch-or-commit>",
		Short: "Delete an input commit.",
		Long:  "Delete an input commit. An input is a commit which is not the output of a pipeline.",
		Run: cmdutil.RunFixedArgs(1, func(args []string) error {
			commit, err := cmdutil.ParseCommit(args[0])
			if err != nil {
				return err
			}
			c, err := client.NewOnUserMachine("user")
			if err != nil {
				return err
			}
			defer c.Close()

			return txncmds.WithActiveTransaction(c, func(c *client.APIClient) error {
				return c.DeleteCommit(commit.Repo.Name, commit.ID)
			})
		}),
	}
	shell.RegisterCompletionFunc(deleteCommit, shell.BranchCompletion)
	commands = append(commands, cmdutil.CreateAlias(deleteCommit, "delete commit"))

	branchDocs := &cobra.Command{
		Short: "Docs for branches.",
		Long: `A branch in Pachyderm is an alias for a Commit ID.

The branch reference will "float" to always refer to the latest commit on the
branch, known as the HEAD commit. Not all commits must be on a branch and
multiple branches can refer to the same commit.

Any pachctl command that can take a Commit ID, can take a branch name instead.`,
	}
	commands = append(commands, cmdutil.CreateDocsAlias(branchDocs, "branch", " branch$"))

	var branchProvenance cmdutil.RepeatedStringArg
	var head string
	trigger := &pfsclient.Trigger{}
	createBranch := &cobra.Command{
		Use:   "{{alias}} <repo>@<branch-or-commit>",
		Short: "Create a new branch, or update an existing branch, on a repo.",
		Long:  "Create a new branch, or update an existing branch, on a repo, starting a commit on the branch will also create it, so there's often no need to call this.",
		Run: cmdutil.RunFixedArgs(1, func(args []string) error {
			branch, err := cmdutil.ParseBranch(args[0])
			if err != nil {
				return err
			}
			provenance, err := cmdutil.ParseBranches(branchProvenance)
			if err != nil {
				return err
			}
			if len(provenance) != 0 && trigger.Branch != "" {
				return errors.Errorf("cannot use provenance and triggers on the same branch")
			}
			if (trigger.CronSpec != "" || trigger.Size_ != "" || trigger.Commits != 0) &&
				trigger.Branch == "" {
				return errors.Errorf("trigger condition specified without a branch to trigger on, specify a branch with --trigger")
			}
			c, err := client.NewOnUserMachine("user")
			if err != nil {
				return err
			}
			defer c.Close()

			return txncmds.WithActiveTransaction(c, func(c *client.APIClient) error {
				if trigger.Branch != "" {
					return c.CreateBranchTrigger(branch.Repo.Name, branch.Name, head, trigger)
				}
				return c.CreateBranch(branch.Repo.Name, branch.Name, head, provenance)
			})
		}),
	}
	createBranch.Flags().VarP(&branchProvenance, "provenance", "p", "The provenance for the branch. format: <repo>@<branch-or-commit>")
	createBranch.MarkFlagCustom("provenance", "__pachctl_get_repo_commit")
	createBranch.Flags().StringVarP(&head, "head", "", "", "The head of the newly created branch.")
	createBranch.MarkFlagCustom("head", "__pachctl_get_commit $(__parse_repo ${nouns[0]})")
	createBranch.Flags().StringVarP(&trigger.Branch, "trigger", "t", "", "The branch to trigger this branch on.")
	createBranch.Flags().StringVar(&trigger.CronSpec, "trigger-cron", "", "The cron spec to use in triggering.")
	createBranch.Flags().StringVar(&trigger.Size_, "trigger-size", "", "The data size to use in triggering.")
	createBranch.Flags().Int64Var(&trigger.Commits, "trigger-commits", 0, "The number of commits to use in triggering.")
	createBranch.Flags().BoolVar(&trigger.All, "trigger-all", false, "Only trigger when all conditions are met, rather than when any are met.")
	commands = append(commands, cmdutil.CreateAlias(createBranch, "create branch"))

	inspectBranch := &cobra.Command{
		Use:   "{{alias}}  <repo>@<branch>",
		Short: "Return info about a branch.",
		Long:  "Return info about a branch.",
		Run: cmdutil.RunFixedArgs(1, func(args []string) error {
			c, err := client.NewOnUserMachine("user")
			if err != nil {
				return err
			}
			defer c.Close()
			branch, err := cmdutil.ParseBranch(args[0])
			if err != nil {
				return err
			}

			branchInfo, err := c.InspectBranch(branch.Repo.Name, branch.Name)
			if err != nil {
				return err
			}
			if branchInfo == nil {
				return errors.Errorf("branch %s not found", args[0])
			}
			if raw {
				return marshaller.Marshal(os.Stdout, branchInfo)
			}

			return pretty.PrintDetailedBranchInfo(branchInfo)
		}),
	}
	inspectBranch.Flags().AddFlagSet(rawFlags)
	inspectBranch.Flags().AddFlagSet(fullTimestampsFlags)
	shell.RegisterCompletionFunc(inspectBranch, shell.BranchCompletion)
	commands = append(commands, cmdutil.CreateAlias(inspectBranch, "inspect branch"))

	listBranch := &cobra.Command{
		Use:   "{{alias}} <repo>",
		Short: "Return all branches on a repo.",
		Long:  "Return all branches on a repo.",
		Run: cmdutil.RunFixedArgs(1, func(args []string) error {
			c, err := client.NewOnUserMachine("user")
			if err != nil {
				return err
			}
			defer c.Close()
			branches, err := c.ListBranch(args[0])
			if err != nil {
				return err
			}
			if raw {
				for _, branch := range branches {
					if err := marshaller.Marshal(os.Stdout, branch); err != nil {
						return err
					}
				}
				return nil
			}
			writer := tabwriter.NewWriter(os.Stdout, pretty.BranchHeader)
			for _, branch := range branches {
				pretty.PrintBranch(writer, branch)
			}
			return writer.Flush()
		}),
	}
	listBranch.Flags().AddFlagSet(rawFlags)
	shell.RegisterCompletionFunc(listBranch, shell.RepoCompletion)
	commands = append(commands, cmdutil.CreateAlias(listBranch, "list branch"))
	commands = append(commands, cmdutil.Hide(cmdutil.CreateAlias(listBranch, "list branches")))

	deleteBranch := &cobra.Command{
		Use:   "{{alias}} <repo>@<branch-or-commit>",
		Short: "Delete a branch",
		Long:  "Delete a branch, while leaving the commits intact",
		Run: cmdutil.RunFixedArgs(1, func(args []string) error {
			branch, err := cmdutil.ParseBranch(args[0])
			if err != nil {
				return err
			}
			c, err := client.NewOnUserMachine("user")
			if err != nil {
				return err
			}
			defer c.Close()

			return txncmds.WithActiveTransaction(c, func(c *client.APIClient) error {
				return c.DeleteBranch(branch.Repo.Name, branch.Name, force)
			})
		}),
	}
	deleteBranch.Flags().BoolVarP(&force, "force", "f", false, "remove the branch regardless of errors; use with care")
	shell.RegisterCompletionFunc(deleteBranch, shell.BranchCompletion)
	commands = append(commands, cmdutil.CreateAlias(deleteBranch, "delete branch"))

	fileDocs := &cobra.Command{
		Short: "Docs for files.",
		Long: `Files are the lowest level data objects in Pachyderm.

Files can be of any type (e.g. csv, binary, images, etc) or size and can be
written to started (but not finished) commits with 'put file'. Files can be read
from commits with 'get file'.`,
	}
	commands = append(commands, cmdutil.CreateDocsAlias(fileDocs, "file", " file$"))

	var filePaths []string
	var recursive bool
	var inputFile string
	var parallelism int
	var split string
	var targetFileDatums uint
	var targetFileBytes uint
	var headerRecords uint
	var putFileCommit bool
	var overwrite bool
	var compress bool
	putFile := &cobra.Command{
		Use:   "{{alias}} <repo>@<branch-or-commit>[:<path/to/file>]",
		Short: "Put a file into the filesystem.",
		Long:  "Put a file into the filesystem.  This command supports a number of ways to insert data into PFS.",
		Example: `
# Put data from stdin as repo/branch/path:
$ echo "data" | {{alias}} repo@branch:/path

# Put data from stdin as repo/branch/path and start / finish a new commit on the branch.
$ echo "data" | {{alias}} -c repo@branch:/path

# Put a file from the local filesystem as repo/branch/path:
$ {{alias}} repo@branch:/path -f file

# Put a file from the local filesystem as repo/branch/file:
$ {{alias}} repo@branch -f file

# Put the contents of a directory as repo/branch/path/dir/file:
$ {{alias}} -r repo@branch:/path -f dir

# Put the contents of a directory as repo/branch/dir/file:
$ {{alias}} -r repo@branch -f dir

# Put the contents of a directory as repo/branch/file, i.e. put files at the top level:
$ {{alias}} -r repo@branch:/ -f dir

# Put the data from a URL as repo/branch/path:
$ {{alias}} repo@branch:/path -f http://host/path

# Put the data from a URL as repo/branch/path:
$ {{alias}} repo@branch -f http://host/path

# Put the data from an S3 bucket as repo/branch/s3_object:
$ {{alias}} repo@branch -r -f s3://my_bucket

# Put several files or URLs that are listed in file.
# Files and URLs should be newline delimited.
$ {{alias}} repo@branch -i file

# Put several files or URLs that are listed at URL.
# NOTE this URL can reference local files, so it could cause you to put sensitive
# files into your Pachyderm cluster.
$ {{alias}} repo@branch -i http://host/path`,
		Run: cmdutil.RunFixedArgs(1, func(args []string) (retErr error) {
			file, err := cmdutil.ParseFile(args[0])
			if err != nil {
				return err
			}
			opts := []client.Option{client.WithMaxConcurrentStreams(parallelism)}
			if compress {
				opts = append(opts, client.WithGZIPCompression())
			}
			c, err := newClient("user", opts...)
			if err != nil {
				return err
			}
			defer c.Close()

			// load data into pachyderm
			pfc, err := c.NewPutFileClient()
			if err != nil {
				return err
			}
			defer func() {
				if err := pfc.Close(); err != nil && retErr == nil {
					retErr = err
				}
			}()
			if putFileCommit {
				fmt.Fprintf(os.Stderr, "flag --commit / -c is deprecated; as of 1.7.2, you will get the same behavior without it\n")
			}

			limiter := limit.New(int(parallelism))
			var sources []string
			if inputFile != "" {
				// User has provided a file listing sources, one per line. Read sources
				var r io.Reader
				if inputFile == "-" {
					r = os.Stdin
				} else if url, err := url.Parse(inputFile); err == nil && url.Scheme != "" {
					resp, err := http.Get(url.String())
					if err != nil {
						return err
					}
					defer func() {
						if err := resp.Body.Close(); err != nil && retErr == nil {
							retErr = err
						}
					}()
					r = resp.Body
				} else {
					inputFile, err := os.Open(inputFile)
					if err != nil {
						return err
					}
					defer func() {
						if err := inputFile.Close(); err != nil && retErr == nil {
							retErr = err
						}
					}()
					r = inputFile
				}
				// scan line by line
				scanner := bufio.NewScanner(r)
				for scanner.Scan() {
					if filePath := scanner.Text(); filePath != "" {
						sources = append(sources, filePath)
					}
				}
			} else {
				// User has provided a single source
				sources = filePaths
			}

			// Arguments parsed; create putFileHelper and begin copying data
			var eg errgroup.Group
			filesPut := &gosync.Map{}
			for _, source := range sources {
				source := source
				if file.Path == "" {
					// The user has not specified a path so we use source as path.
					if source == "-" {
						return errors.Errorf("must specify filename when reading data from stdin")
					}
					eg.Go(func() error {
						return putFileHelper(c, pfc, file.Commit.Repo.Name, file.Commit.ID, joinPaths("", source), source, recursive, overwrite, limiter, split, targetFileDatums, targetFileBytes, headerRecords, filesPut)
					})
				} else if len(sources) == 1 {
					// We have a single source and the user has specified a path,
					// we use the path and ignore source (in terms of naming the file).
					eg.Go(func() error {
						return putFileHelper(c, pfc, file.Commit.Repo.Name, file.Commit.ID, file.Path, source, recursive, overwrite, limiter, split, targetFileDatums, targetFileBytes, headerRecords, filesPut)
					})
				} else {
					// We have multiple sources and the user has specified a path,
					// we use that path as a prefix for the filepaths.
					eg.Go(func() error {
						return putFileHelper(c, pfc, file.Commit.Repo.Name, file.Commit.ID, joinPaths(file.Path, source), source, recursive, overwrite, limiter, split, targetFileDatums, targetFileBytes, headerRecords, filesPut)
					})
				}
			}
			return eg.Wait()
		}),
	}
	putFile.Flags().StringSliceVarP(&filePaths, "file", "f", []string{"-"}, "The file to be put, it can be a local file or a URL.")
	putFile.Flags().StringVarP(&inputFile, "input-file", "i", "", "Read filepaths or URLs from a file.  If - is used, paths are read from the standard input.")
	putFile.Flags().BoolVarP(&recursive, "recursive", "r", false, "Recursively put the files in a directory.")
	putFile.Flags().BoolVarP(&compress, "compress", "", false, "Compress data during upload. This parameter might help you upload your uncompressed data, such as CSV files, to Pachyderm faster. Use 'compress' with caution, because if your data is already compressed, this parameter might slow down the upload speed instead of increasing.")
	putFile.Flags().IntVarP(&parallelism, "parallelism", "p", DefaultParallelism, "The maximum number of files that can be uploaded in parallel.")
	putFile.Flags().StringVar(&split, "split", "", "Split the input file into smaller files, subject to the constraints of --target-file-datums and --target-file-bytes. Permissible values are `line`, `json`, `sql` and `csv`.")
	putFile.Flags().UintVar(&targetFileDatums, "target-file-datums", 0, "The upper bound of the number of datums that each file contains, the last file will contain fewer if the datums don't divide evenly; needs to be used with --split.")
	putFile.Flags().UintVar(&targetFileBytes, "target-file-bytes", 0, "The target upper bound of the number of bytes that each file contains; needs to be used with --split.")
	putFile.Flags().UintVar(&headerRecords, "header-records", 0, "the number of records that will be converted to a PFS 'header', and prepended to future retrievals of any subset of data from PFS; needs to be used with --split=(json|line|csv)")
	putFile.Flags().BoolVarP(&putFileCommit, "commit", "c", false, "DEPRECATED: Put file(s) in a new commit.")
	putFile.Flags().BoolVarP(&overwrite, "overwrite", "o", false, "Overwrite the existing content of the file, either from previous commits or previous calls to 'put file' within this commit.")
	shell.RegisterCompletionFunc(putFile,
		func(flag, text string, maxCompletions int64) ([]prompt.Suggest, shell.CacheFunc) {
			if flag == "-f" || flag == "--file" || flag == "-i" || flag == "input-file" {
				cs, cf := shell.FilesystemCompletion(flag, text, maxCompletions)
				return cs, shell.AndCacheFunc(cf, shell.SameFlag(flag))
			} else if flag == "" || flag == "-c" || flag == "--commit" || flag == "-o" || flag == "--overwrite" {
				cs, cf := shell.FileCompletion(flag, text, maxCompletions)
				return cs, shell.AndCacheFunc(cf, shell.SameFlag(flag))
			}
			return nil, shell.SameFlag(flag)
		})
	commands = append(commands, cmdutil.CreateAlias(putFile, "put file"))
	commands = append(commands, cmdutil.Hide(cmdutil.CreateAlias(putFile, "put files")))

	copyFile := &cobra.Command{
		Use:   "{{alias}} <src-repo>@<src-branch-or-commit>:<src-path> <dst-repo>@<dst-branch-or-commit>:<dst-path>",
		Short: "Copy files between pfs paths.",
		Long:  "Copy files between pfs paths.",
		Run: cmdutil.RunFixedArgs(2, func(args []string) (retErr error) {
			srcFile, err := cmdutil.ParseFile(args[0])
			if err != nil {
				return err
			}
			destFile, err := cmdutil.ParseFile(args[1])
			if err != nil {
				return err
			}
			c, err := client.NewOnUserMachine("user", client.WithMaxConcurrentStreams(parallelism))
			if err != nil {
				return err
			}
			defer c.Close()

			return c.CopyFile(
				srcFile.Commit.Repo.Name, srcFile.Commit.ID, srcFile.Path,
				destFile.Commit.Repo.Name, destFile.Commit.ID, destFile.Path,
				overwrite,
			)
		}),
	}
	copyFile.Flags().BoolVarP(&overwrite, "overwrite", "o", false, "Overwrite the existing content of the file, either from previous commits or previous calls to 'put file' within this commit.")
	shell.RegisterCompletionFunc(copyFile, shell.FileCompletion)
	commands = append(commands, cmdutil.CreateAlias(copyFile, "copy file"))
	commands = append(commands, cmdutil.Hide(cmdutil.CreateAlias(copyFile, "copy files")))

	var outputPath string
	getFile := &cobra.Command{
		Use:   "{{alias}} <repo>@<branch-or-commit>:<path/in/pfs>",
		Short: "Return the contents of a file.",
		Long:  "Return the contents of a file.",
		Example: `
# get file "XXX" on branch "master" in repo "foo"
$ {{alias}} foo@master:XXX

# get file "XXX" in the parent of the current head of branch "master"
# in repo "foo"
$ {{alias}} foo@master^:XXX

# get file "XXX" in the grandparent of the current head of branch "master"
# in repo "foo"
$ {{alias}} foo@master^2:XXX`,
		Run: cmdutil.RunFixedArgs(1, func(args []string) error {
			file, err := cmdutil.ParseFile(args[0])
			if err != nil {
				return err
			}
			c, err := newClient("user")
			if err != nil {
				return err
			}
			defer c.Close()
			if recursive {
				if outputPath == "" {
					return errors.Errorf("an output path needs to be specified when using the --recursive flag")
				}
				puller := sync.NewPuller()
				return puller.Pull(c, outputPath, file.Commit.Repo.Name, file.Commit.ID, file.Path, false, false, parallelism, nil, "")
			}
			var w io.Writer
			// If an output path is given, print the output to stdout
			if outputPath == "" {
				w = os.Stdout
			} else {
				f, err := os.Create(outputPath)
				if err != nil {
					return err
				}
				defer f.Close()
				w = f
			}
			return c.GetFile(file.Commit.Repo.Name, file.Commit.ID, file.Path, 0, 0, w)
		}),
	}
	getFile.Flags().BoolVarP(&recursive, "recursive", "r", false, "Recursively download a directory.")
	getFile.Flags().StringVarP(&outputPath, "output", "o", "", "The path where data will be downloaded.")
	getFile.Flags().IntVarP(&parallelism, "parallelism", "p", DefaultParallelism, "The maximum number of files that can be downloaded in parallel")
	shell.RegisterCompletionFunc(getFile, shell.FileCompletion)
	commands = append(commands, cmdutil.CreateAlias(getFile, "get file"))
	commands = append(commands, cmdutil.Hide(cmdutil.CreateAlias(getFile, "get files")))

	inspectFile := &cobra.Command{
		Use:   "{{alias}} <repo>@<branch-or-commit>:<path/in/pfs>",
		Short: "Return info about a file.",
		Long:  "Return info about a file.",
		Run: cmdutil.RunFixedArgs(1, func(args []string) error {
			file, err := cmdutil.ParseFile(args[0])
			if err != nil {
				return err
			}
			c, err := client.NewOnUserMachine("user")
			if err != nil {
				return err
			}
			defer c.Close()
			fileInfo, err := c.InspectFile(file.Commit.Repo.Name, file.Commit.ID, file.Path)
			if err != nil {
				return err
			}
			if fileInfo == nil {
				return errors.Errorf("file %s not found", file.Path)
			}
			if raw {
				return marshaller.Marshal(os.Stdout, fileInfo)
			}
			return pretty.PrintDetailedFileInfo(fileInfo)
		}),
	}
	inspectFile.Flags().AddFlagSet(rawFlags)
	shell.RegisterCompletionFunc(inspectFile, shell.FileCompletion)
	commands = append(commands, cmdutil.CreateAlias(inspectFile, "inspect file"))

	var history string
	listFile := &cobra.Command{
		Use:   "{{alias}} <repo>@<branch-or-commit>[:<path/in/pfs>]",
		Short: "Return the files in a directory.",
		Long:  "Return the files in a directory.",
		Example: `
# list top-level files on branch "master" in repo "foo"
$ {{alias}} foo@master

# list files under directory "dir" on branch "master" in repo "foo"
$ {{alias}} foo@master:dir

# list top-level files in the parent commit of the current head of "master"
# in repo "foo"
$ {{alias}} foo@master^

# list top-level files in the grandparent of the current head of "master"
# in repo "foo"
$ {{alias}} foo@master^2

# list the last n versions of top-level files on branch "master" in repo "foo"
$ {{alias}} foo@master --history n

# list all versions of top-level files on branch "master" in repo "foo"
$ {{alias}} foo@master --history all`,
		Run: cmdutil.RunFixedArgs(1, func(args []string) error {
			file, err := cmdutil.ParseFile(args[0])
			if err != nil {
				return err
			}
			history, err := cmdutil.ParseHistory(history)
			if err != nil {
				return errors.Wrapf(err, "error parsing history flag")
			}
			c, err := client.NewOnUserMachine("user")
			if err != nil {
				return err
			}
			defer c.Close()
			if raw {
				return c.ListFileF(file.Commit.Repo.Name, file.Commit.ID, file.Path, history, func(fi *pfsclient.FileInfo) error {
					return marshaller.Marshal(os.Stdout, fi)
				})
			}
			header := pretty.FileHeader
			if history != 0 {
				header = pretty.FileHeaderWithCommit
			}
			writer := tabwriter.NewWriter(os.Stdout, header)
			if err := c.ListFileF(file.Commit.Repo.Name, file.Commit.ID, file.Path, history, func(fi *pfsclient.FileInfo) error {
				pretty.PrintFileInfo(writer, fi, fullTimestamps, history != 0)
				return nil
			}); err != nil {
				return err
			}
			return writer.Flush()
		}),
	}
	listFile.Flags().AddFlagSet(rawFlags)
	listFile.Flags().AddFlagSet(fullTimestampsFlags)
	listFile.Flags().StringVar(&history, "history", "none", "Return revision history for files.")
	shell.RegisterCompletionFunc(listFile, shell.FileCompletion)
	commands = append(commands, cmdutil.CreateAlias(listFile, "list file"))
	commands = append(commands, cmdutil.Hide(cmdutil.CreateAlias(listFile, "list files")))

	globFile := &cobra.Command{
		Use:   "{{alias}} <repo>@<branch-or-commit>:<pattern>",
		Short: "Return files that match a glob pattern in a commit.",
		Long:  "Return files that match a glob pattern in a commit (that is, match a glob pattern in a repo at the state represented by a commit). Glob patterns are documented [here](https://golang.org/pkg/path/filepath/#Match).",
		Example: `
# Return files in repo "foo" on branch "master" that start
# with the character "A".  Note how the double quotation marks around the
# parameter are necessary because otherwise your shell might interpret the "*".
$ {{alias}} "foo@master:A*"

# Return files in repo "foo" on branch "master" under directory "data".
$ {{alias}} "foo@master:data/*"`,
		Run: cmdutil.RunFixedArgs(1, func(args []string) error {
			file, err := cmdutil.ParseFile(args[0])
			if err != nil {
				return err
			}
			c, err := client.NewOnUserMachine("user")
			if err != nil {
				return err
			}
			defer c.Close()
			fileInfos, err := c.GlobFile(file.Commit.Repo.Name, file.Commit.ID, file.Path)
			if err != nil {
				return err
			}
			if raw {
				for _, fileInfo := range fileInfos {
					if err := marshaller.Marshal(os.Stdout, fileInfo); err != nil {
						return err
					}
				}
				return nil
			}
			writer := tabwriter.NewWriter(os.Stdout, pretty.FileHeader)
			for _, fileInfo := range fileInfos {
				pretty.PrintFileInfo(writer, fileInfo, fullTimestamps, false)
			}
			return writer.Flush()
		}),
	}
	globFile.Flags().AddFlagSet(rawFlags)
	globFile.Flags().AddFlagSet(fullTimestampsFlags)
	shell.RegisterCompletionFunc(globFile, shell.FileCompletion)
	commands = append(commands, cmdutil.CreateAlias(globFile, "glob file"))
	commands = append(commands, cmdutil.Hide(cmdutil.CreateAlias(globFile, "glob files")))

	var shallow bool
	var nameOnly bool
	var diffCmdArg string
	diffFile := &cobra.Command{
		Use:   "{{alias}} <new-repo>@<new-branch-or-commit>:<new-path> [<old-repo>@<old-branch-or-commit>:<old-path>]",
		Short: "Return a diff of two file trees.",
		Long:  "Return a diff of two file trees.",
		Example: `
# Return the diff of the file "path" of the repo "foo" between the head of the
# "master" branch and its parent.
$ {{alias}} foo@master:path

# Return the diff between the master branches of repos foo and bar at paths
# path1 and path2, respectively.
$ {{alias}} foo@master:path1 bar@master:path2`,
		Run: cmdutil.RunBoundedArgs(1, 2, func(args []string) error {
			newFile, err := cmdutil.ParseFile(args[0])
			if err != nil {
				return err
			}
			oldFile := client.NewFile("", "", "")
			if len(args) == 2 {
				oldFile, err = cmdutil.ParseFile(args[1])
				if err != nil {
					return err
				}
			}
			c, err := client.NewOnUserMachine("user")
			if err != nil {
				return err
			}
			defer c.Close()

			return pager.Page(noPager, os.Stdout, func(w io.Writer) (retErr error) {
				var writer *tabwriter.Writer
				if nameOnly {
					writer = tabwriter.NewWriter(w, pretty.DiffFileHeader)
					defer func() {
						if err := writer.Flush(); err != nil && retErr == nil {
							retErr = err
						}
					}()
				}

				newFiles, oldFiles, err := c.DiffFile(
					newFile.Commit.Repo.Name, newFile.Commit.ID, newFile.Path,
					oldFile.Commit.Repo.Name, oldFile.Commit.ID, oldFile.Path,
					shallow,
				)
				if err != nil {
					return err
				}
				diffCmd := diffCommand(diffCmdArg)
				return forEachDiffFile(newFiles, oldFiles, func(nFI, oFI *pfsclient.FileInfo) error {
					if nameOnly {
						if nFI != nil {
							pretty.PrintDiffFileInfo(writer, true, nFI, fullTimestamps)
						}
						if oFI != nil {
							pretty.PrintDiffFileInfo(writer, false, oFI, fullTimestamps)
						}
						return nil
					}
					nPath, oPath := "/dev/null", "/dev/null"
					if nFI != nil {
						nPath, err = dlFile(c, nFI.File)
						if err != nil {
							return err
						}
						defer func() {
							if err := os.RemoveAll(nPath); err != nil && retErr == nil {
								retErr = err
							}
						}()
					}
					if oFI != nil {
						oPath, err = dlFile(c, oFI.File)
						defer func() {
							if err := os.RemoveAll(oPath); err != nil && retErr == nil {
								retErr = err
							}
						}()
					}
					cmd := exec.Command(diffCmd[0], append(diffCmd[1:], oPath, nPath)...)
					cmd.Stdout = w
					cmd.Stderr = os.Stderr
					// Diff returns exit code 1 when it finds differences
					// between the files, so we catch it.
					if err := cmd.Run(); err != nil && cmd.ProcessState.ExitCode() != 1 {
						return err
					}
					return nil
				})
			})
		}),
	}
	diffFile.Flags().BoolVarP(&shallow, "shallow", "s", false, "Don't descend into sub directories.")
	diffFile.Flags().BoolVar(&nameOnly, "name-only", false, "Show only the names of changed files.")
	diffFile.Flags().StringVar(&diffCmdArg, "diff-command", "", "Use a program other than git to diff files.")
	diffFile.Flags().AddFlagSet(fullTimestampsFlags)
	diffFile.Flags().AddFlagSet(noPagerFlags)
	shell.RegisterCompletionFunc(diffFile, shell.FileCompletion)
	commands = append(commands, cmdutil.CreateAlias(diffFile, "diff file"))
	commands = append(commands, cmdutil.Hide(cmdutil.CreateAlias(diffFile, "diff files")))

	deleteFile := &cobra.Command{
		Use:   "{{alias}} <repo>@<branch-or-commit>:<path/in/pfs>",
		Short: "Delete a file.",
		Long:  "Delete a file.",
		Run: cmdutil.RunFixedArgs(1, func(args []string) error {
			file, err := cmdutil.ParseFile(args[0])
			if err != nil {
				return err
			}
			c, err := client.NewOnUserMachine("user")
			if err != nil {
				return err
			}
			defer c.Close()

			return c.DeleteFile(file.Commit.Repo.Name, file.Commit.ID, file.Path)
		}),
	}
	shell.RegisterCompletionFunc(deleteFile, shell.FileCompletion)
	commands = append(commands, cmdutil.CreateAlias(deleteFile, "delete file"))
	commands = append(commands, cmdutil.Hide(cmdutil.CreateAlias(deleteFile, "delete files")))

	objectDocs := &cobra.Command{
		Short: "Docs for objects.",
		Long: `Objects are content-addressed blobs of data that are directly stored in the backend object store.

Objects are a low-level resource and should not be accessed directly by most users.`,
	}
	commands = append(commands, cmdutil.CreateDocsAlias(objectDocs, "object", " object$"))

	getObject := &cobra.Command{
		Use:   "{{alias}} <hash>",
		Short: "Print the contents of an object.",
		Long:  "Print the contents of an object.",
		Run: cmdutil.RunFixedArgs(1, func(args []string) error {
			c, err := client.NewOnUserMachine("user")
			if err != nil {
				return err
			}
			defer c.Close()
			return c.GetObject(args[0], os.Stdout)
		}),
	}
	commands = append(commands, cmdutil.CreateAlias(getObject, "get object"))

	tagDocs := &cobra.Command{
		Short: "Docs for tags.",
		Long: `Tags are aliases for objects. Many tags can refer to the same object.

Tags are a low-level resource and should not be accessed directly by most users.`,
	}
	commands = append(commands, cmdutil.CreateDocsAlias(tagDocs, "tag", " tag$"))

	getTag := &cobra.Command{
		Use:   "{{alias}} <tag>",
		Short: "Print the contents of a tag.",
		Long:  "Print the contents of a tag.",
		Run: cmdutil.RunFixedArgs(1, func(args []string) error {
			c, err := client.NewOnUserMachine("user")
			if err != nil {
				return err
			}
			defer c.Close()
			return c.GetTag(args[0], os.Stdout)
		}),
	}
	commands = append(commands, cmdutil.CreateAlias(getTag, "get tag"))

	var fix bool
	fsck := &cobra.Command{
		Use:   "{{alias}}",
		Short: "Run a file system consistency check on pfs.",
		Long:  "Run a file system consistency check on the pachyderm file system, ensuring the correct provenance relationships are satisfied.",
		Run: cmdutil.RunFixedArgs(0, func(args []string) error {
			c, err := client.NewOnUserMachine("user")
			if err != nil {
				return err
			}
			defer c.Close()
			errors := false
			if err = c.Fsck(fix, func(resp *pfsclient.FsckResponse) error {
				if resp.Error != "" {
					errors = true
					fmt.Printf("Error: %s\n", resp.Error)
				} else {
					fmt.Printf("Fix applied: %v", resp.Fix)
				}
				return nil
			}); err != nil {
				return err
			}
			if !errors {
				fmt.Println("No errors found.")
			}
			return nil
		}),
	}
	fsck.Flags().BoolVarP(&fix, "fix", "f", false, "Attempt to fix as many issues as possible.")
	commands = append(commands, cmdutil.CreateAlias(fsck, "fsck"))

	// Add the mount commands (which aren't available on Windows, so they're in
	// their own file)
	commands = append(commands, mountCmds()...)

	return commands
}

func putFileHelper(c *client.APIClient, pfc client.PutFileClient,
	repo, commit, path, source string, recursive, overwrite bool, // destination
	limiter limit.ConcurrencyLimiter,
	split string, targetFileDatums, targetFileBytes, headerRecords uint, // split
	filesPut *gosync.Map) (retErr error) {
	// Resolve the path, then trim any prefixed '../' to avoid sending bad paths
	// to the server
	path = filepath.Clean(path)
	for strings.HasPrefix(path, "../") {
		path = strings.TrimPrefix(path, "../")
	}

	if _, ok := filesPut.LoadOrStore(path, nil); ok {
		return errors.Errorf("multiple files put with the path %s, aborting, "+
			"some files may already have been put and should be cleaned up with "+
			"'delete file' or 'delete commit'", path)
	}
	putFile := func(reader io.ReadSeeker) error {
		if split == "" {
			pipe, err := isPipe(reader)
			if err != nil {
				return err
			}
			if overwrite && !pipe {
				return sync.PushFile(c, pfc, client.NewFile(repo, commit, path), reader)
			}
			if overwrite {
				_, err = pfc.PutFileOverwrite(repo, commit, path, reader, 0)
				return err
			}
			_, err = pfc.PutFile(repo, commit, path, reader)
			return err
		}

		var delimiter pfsclient.Delimiter
		switch split {
		case "line":
			delimiter = pfsclient.Delimiter_LINE
		case "json":
			delimiter = pfsclient.Delimiter_JSON
		case "sql":
			delimiter = pfsclient.Delimiter_SQL
		case "csv":
			delimiter = pfsclient.Delimiter_CSV
		default:
			return errors.Errorf("unrecognized delimiter '%s'; only accepts one of "+
				"{json,line,sql,csv}", split)
		}
		_, err := pfc.PutFileSplit(repo, commit, path, delimiter, int64(targetFileDatums), int64(targetFileBytes), int64(headerRecords), overwrite, reader)
		return err
	}

	if source == "-" {
		if recursive {
			return errors.New("cannot set -r and read from stdin (must also set -f or -i)")
		}
		limiter.Acquire()
		defer limiter.Release()
		stdin := progress.Stdin()
		defer stdin.Finish()
		return putFile(stdin)
	}
	// try parsing the filename as a url, if it is one do a PutFileURL
	if url, err := url.Parse(source); err == nil && url.Scheme != "" {
		limiter.Acquire()
		defer limiter.Release()
		return pfc.PutFileURL(repo, commit, path, url.String(), recursive, overwrite)
	}
	if recursive {
		var eg errgroup.Group
		if err := filepath.Walk(source, func(filePath string, info os.FileInfo, err error) error {
			// file doesn't exist
			if info == nil {
				return errors.Errorf("%s doesn't exist", filePath)
			}
			if info.IsDir() {
				return nil
			}
			childDest := filepath.Join(path, strings.TrimPrefix(filePath, source))
			eg.Go(func() error {
				// don't do a second recursive 'put file', just put the one file at
				// filePath into childDest, and then this walk loop will go on to the
				// next one
				return putFileHelper(c, pfc, repo, commit, childDest, filePath, false,
					overwrite, limiter, split, targetFileDatums, targetFileBytes,
					headerRecords, filesPut)
			})
			return nil
		}); err != nil {
			return err
		}
		return eg.Wait()
	}
	limiter.Acquire()
	defer limiter.Release()
	f, err := progress.Open(source)
	if err != nil {
		return err
	}
	defer func() {
		if err := f.Close(); err != nil && retErr == nil {
			retErr = err
		}
	}()
	return putFile(f)
}

func joinPaths(prefix, filePath string) string {
	if url, err := url.Parse(filePath); err == nil && url.Scheme != "" {
		if url.Scheme == "pfs" {
			// pfs paths are of the form pfs://host/repo/branch/path we don't
			// want to prefix every file with host/repo so we remove those
			splitPath := strings.Split(strings.TrimPrefix(url.Path, "/"), "/")
			if len(splitPath) < 3 {
				return prefix
			}
			return filepath.Join(append([]string{prefix}, splitPath[2:]...)...)
		}
		return filepath.Join(prefix, strings.TrimPrefix(url.Path, "/"))
	}
	return filepath.Join(prefix, filePath)
}

func isPipe(r io.ReadSeeker) (bool, error) {
	file, ok := r.(*os.File)
	if !ok {
		return false, nil
	}
	fi, err := file.Stat()
	if err != nil {
		return false, err
	}
	return fi.Mode()&os.ModeNamedPipe != 0, nil
}

func dlFile(pachClient *client.APIClient, f *pfsclient.File) (_ string, retErr error) {
	if err := os.MkdirAll(filepath.Join(os.TempDir(), filepath.Dir(f.Path)), 0777); err != nil {
		return "", err
	}
	file, err := ioutil.TempFile("", f.Path+"_")
	if err != nil {
		return "", err
	}
	defer func() {
		if err := file.Close(); err != nil && retErr == nil {
			retErr = err
		}
	}()
	if err := pachClient.GetFile(f.Commit.Repo.Name, f.Commit.ID, f.Path, 0, 0, file); err != nil {
		return "", err
	}
	return file.Name(), nil
}

func diffCommand(cmdArg string) []string {
	if cmdArg != "" {
		return strings.Fields(cmdArg)
	}
	_, err := exec.LookPath("git")
	if err == nil {
		return []string{"git", "-c", "color.ui=always", "--no-pager", "diff", "--no-index"}
	}
	return []string{"diff"}
}

func forEachDiffFile(newFiles, oldFiles []*pfsclient.FileInfo, f func(newFile, oldFile *pfsclient.FileInfo) error) error {
	nI, oI := 0, 0
	for {
		if nI == len(newFiles) && oI == len(oldFiles) {
			return nil
		}
		var oFI *pfsclient.FileInfo
		var nFI *pfsclient.FileInfo
		switch {
		case oI == len(oldFiles) || (nI < len(newFiles) && newFiles[nI].File.Path < oldFiles[oI].File.Path):
			nFI = newFiles[nI]
			nI++
		case nI == len(newFiles) || (oI < len(oldFiles) && oldFiles[oI].File.Path < newFiles[nI].File.Path):
			oFI = oldFiles[oI]
			oI++
		case newFiles[nI].File.Path == oldFiles[oI].File.Path:
			nFI = newFiles[nI]
			nI++
			oFI = oldFiles[oI]
			oI++
		}
		if err := f(nFI, oFI); err != nil {
			if errors.Is(err, errutil.ErrBreak) {
				return nil
			}
			return err
		}
	}
}

func newClient(name string, options ...client.Option) (*client.APIClient, error) {
	if inWorkerStr, ok := os.LookupEnv("PACH_IN_WORKER"); ok {
		inWorker, err := strconv.ParseBool(inWorkerStr)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't parse PACH_IN_WORKER")
		}
		if inWorker {
			return client.NewInWorker(options...)
		}
	}
	return client.NewOnUserMachine(name, options...)
}
