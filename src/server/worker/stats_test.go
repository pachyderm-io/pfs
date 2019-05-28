package worker

import (
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/pachyderm/pachyderm/src/client"
	"github.com/pachyderm/pachyderm/src/client/enterprise"
	"github.com/pachyderm/pachyderm/src/client/pfs"
	"github.com/pachyderm/pachyderm/src/client/pkg/require"
	"github.com/pachyderm/pachyderm/src/client/pps"
	"github.com/pachyderm/pachyderm/src/server/pkg/backoff"
	tu "github.com/pachyderm/pachyderm/src/server/pkg/testutil"
)

func activateEnterprise(c *client.APIClient) error {
	_, err := c.Enterprise.Activate(context.Background(),
		&enterprise.ActivateRequest{ActivationCode: tu.GetTestEnterpriseCode()})

	if err != nil {
		return err
	}

	return backoff.Retry(func() error {
		resp, err := c.Enterprise.GetState(context.Background(),
			&enterprise.GetStateRequest{})
		if err != nil {
			return err
		}
		if resp.State != enterprise.State_ACTIVE {
			return fmt.Errorf("expected enterprise state to be ACTIVE but was %v", resp.State)
		}
		return nil
	}, backoff.NewTestingBackOff())
}

// Regression: stats commits would not close when there were no input datums.
//For more info, see github.com/pachyderm/pachyderm/issues/3337
func TestCloseStatsCommitWithNoInputDatums(t *testing.T) {
	c := getPachClient(t)
	defer require.NoError(t, c.DeleteAll())
	require.NoError(t, activateEnterprise(c))

	dataRepo := tu.UniqueString("TestSimplePipeline_data")
	require.NoError(t, c.CreateRepo(dataRepo))

	pipeline := tu.UniqueString("TestSimplePipeline")

	_, err := c.PpsAPIClient.CreatePipeline(
		c.Ctx(),
		&pps.CreatePipelineRequest{
			Pipeline: client.NewPipeline(pipeline),
			Transform: &pps.Transform{
				Cmd:   []string{"bash"},
				Stdin: []string{"sleep 1"},
			},
			Input:        client.NewPFSInput(dataRepo, "/*"),
			OutputBranch: "",
			Update:       false,
			EnableStats:  true,
		},
	)
	require.NoError(t, err)

	commit, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.FinishCommit(dataRepo, commit.ID))

	// If the error exists, the stats commit will never close, and this will
	// timeout
	commitIter, err := c.FlushCommit([]*pfs.Commit{commit}, nil)
	require.NoError(t, err)

	for {
		_, err := commitIter.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
	}

	// Make sure the job succeeded as well
	jobs, err := c.ListJob(pipeline, nil, nil, -1)
	require.NoError(t, err)
	require.Equal(t, 1, len(jobs))
	jobInfo, err := c.InspectJob(jobs[0].Job.ID, true)
	require.NoError(t, err)
	require.Equal(t, pps.JobState_JOB_SUCCESS, jobInfo.State)
}
