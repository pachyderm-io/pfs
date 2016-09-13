package client_test

import (
	"bytes"

	"github.com/pachyderm/pachyderm/src/client"
	"github.com/pachyderm/pachyderm/src/client/pps"
)

func Example_pps() {
	c, err := client.NewFromAddress("0.0.0.0:30650")
	if err != nil {
		return // handle error
	}

	// we assume there's already a repo called "repo"
	// and that it already has some data in it
	// take a look at src/client/pfs_test.go for an example of how to get there.

	// Create a map pipeline
	if err := c.CreatePipeline(
		"map", // the name of the pipeline
		"pachyderm/test_image", // your docker image
		[]string{"map"},        // the command run in your docker image
		nil,                    // no stdin
		&pps.ParallelismSpec{
			Strategy:    pps.ParallelismSpec_COEFFICIENT,
			Coefficient: 1.0,
		},
		[]*pps.PipelineInput{
			// map over "repo"
			client.NewPipelineInput("repo", client.MapMethod),
		},
		false, // not an update
	); err != nil {
		return // handle error
	}
	if err := c.CreatePipeline(
		"reduce",               // the name of the pipeline
		"pachyderm/test_image", // your docker image
		[]string{"reduce"},     // the command run in your docker image
		nil,                    // no stdin
		&pps.ParallelismSpec{
			Strategy:    pps.ParallelismSpec_COEFFICIENT,
			Coefficient: 1.0,
		},
		[]*pps.PipelineInput{
			// reduce over "map"
			client.NewPipelineInput("map", client.ReduceMethod),
		},
		false, // not an update
	); err != nil {
		return // handle error
	}

	commits, err := c.ListCommit( // List commits that are...
		[]string{"reduce"}, // from the "reduce" repo (which the "reduce" pipeline outputs)
		nil,                // starting at the beginning of time
		client.CommitTypeRead, // are readable
		true, // block until commits are available
		client.CommitStatusNormal, // ignore cancelled commits
		nil, // have no provenance
	)
	if err != nil {
		return // handle error
	}
	for _, commitInfo := range commits {
		// Read output from the pipeline
		var buffer bytes.Buffer
		if err := c.GetFile("reduce", commitInfo.Commit.ID, "file", 0, 0, "", false, nil, &buffer); err != nil {
			return //handle error
		}
	}
}
