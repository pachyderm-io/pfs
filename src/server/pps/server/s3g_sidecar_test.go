/*
This test is for PPS pipelines that use S3 inputs/outputs. Most of these
pipelines use the pachyderm/s3testing image, which exists on dockerhub but can
be built by running:
  cd etc/testing/images/s3testing
  make push-to-minikube
*/
package server

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pachyderm/pachyderm/v2/src/client"
	"github.com/pachyderm/pachyderm/v2/src/internal/errors"
	"github.com/pachyderm/pachyderm/v2/src/internal/ppsutil"
	"github.com/pachyderm/pachyderm/v2/src/internal/require"
	"github.com/pachyderm/pachyderm/v2/src/internal/tarutil"
	tu "github.com/pachyderm/pachyderm/v2/src/internal/testutil"
	"github.com/pachyderm/pachyderm/v2/src/pfs"
	"github.com/pachyderm/pachyderm/v2/src/pps"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// This test is designed to run against pachyderm in custom namespaces and with
// auth on or off. It reads env vars into here and adjusts tests to make sure
// our s3 gateway feature works in those contexts
var Namespace string

func init() {
	var ok bool
	Namespace, ok = os.LookupEnv("PACH_NAMESPACE")
	if !ok {
		Namespace = v1.NamespaceDefault
	}
}

func initPachClient(t testing.TB) (*client.APIClient, string) {
	if _, ok := os.LookupEnv("PACH_TEST_WITH_AUTH"); !ok {
		c := tu.GetPachClient(t)
		require.NoError(t, c.DeleteAll())
		return c, ""
	}
	rootClient := tu.GetAuthenticatedPachClient(t, tu.RootToken)
	require.NoError(t, rootClient.DeleteAll())
	c := tu.GetAuthenticatedPachClient(t, tu.UniqueString("user-"))
	return c, c.AuthToken()
}

func TestS3PipelineErrors(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c, _ := initPachClient(t)

	repo1, repo2 := tu.UniqueString(t.Name()+"_data"), tu.UniqueString(t.Name()+"_data")
	require.NoError(t, c.CreateRepo(repo1))
	require.NoError(t, c.CreateRepo(repo2))

	pipeline := tu.UniqueString("Pipeline")
	err := c.CreatePipeline(
		pipeline,
		"",
		[]string{"bash"},
		[]string{
			"ls -R /pfs >/pfs/out/files",
		},
		&pps.ParallelismSpec{
			Constant: 1,
		},
		&pps.Input{
			Union: []*pps.Input{
				{Pfs: &pps.PFSInput{
					Repo:   repo1,
					Branch: "master",
					S3:     true,
					Glob:   "/",
				}},
				{Pfs: &pps.PFSInput{
					Repo:   repo2,
					Branch: "master",
					Glob:   "/*",
				}},
			},
		},
		"",
		false,
	)
	require.YesError(t, err)
	require.Matches(t, "union", err.Error())
	err = c.CreatePipeline(
		pipeline,
		"",
		[]string{"bash"},
		[]string{
			"ls -R /pfs >/pfs/out/files",
		},
		&pps.ParallelismSpec{
			Constant: 1,
		},
		&pps.Input{
			Join: []*pps.Input{
				{Pfs: &pps.PFSInput{
					Repo:   repo1,
					Branch: "master",
					S3:     true,
					Glob:   "/",
				}},
				{Pfs: &pps.PFSInput{
					Repo:   repo2,
					Branch: "master",
					Glob:   "/*",
				}},
			},
		},
		"",
		false,
	)
	require.YesError(t, err)
	require.Matches(t, "join", err.Error())
}

func TestS3Input(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c, userToken := initPachClient(t)

	repo := tu.UniqueString(t.Name() + "_data")
	require.NoError(t, c.CreateRepo(repo))
	masterCommit := client.NewCommit(repo, "master", "")

	require.NoError(t, c.PutFile(masterCommit, "foo", strings.NewReader("foo")))

	pipeline := tu.UniqueString("Pipeline")
	pipelineCommit := client.NewCommit(pipeline, "master", "")
	_, err := c.PpsAPIClient.CreatePipeline(c.Ctx(), &pps.CreatePipelineRequest{
		Pipeline: client.NewPipeline(pipeline),
		Transform: &pps.Transform{
			Image: "pachyderm/ubuntu-with-s3-clients:v0.0.1",
			Cmd:   []string{"bash", "-x"},
			Stdin: []string{
				"ls -R /pfs >/pfs/out/pfs_files",
				"aws --endpoint=${S3_ENDPOINT} s3 ls >/pfs/out/s3_buckets",
				"aws --endpoint=${S3_ENDPOINT} s3 ls s3://input_repo >/pfs/out/s3_files",
			},
			Env: map[string]string{
				"AWS_ACCESS_KEY_ID":     userToken,
				"AWS_SECRET_ACCESS_KEY": userToken,
			},
		},
		ParallelismSpec: &pps.ParallelismSpec{Constant: 1},
		Input: &pps.Input{
			Pfs: &pps.PFSInput{
				Name:   "input_repo",
				Repo:   repo,
				Branch: "master",
				S3:     true,
				Glob:   "/",
			},
		},
	})
	require.NoError(t, err)

	commitInfo, err := c.InspectCommit(pipeline, "master", "")
	require.NoError(t, err)

	jobInfo, err := c.WaitJob(pipeline, commitInfo.Commit.ID, false)
	require.NoError(t, err)
	require.Equal(t, "JOB_SUCCESS", jobInfo.State.String())

	// Make sure ListFile works
	files, err := c.ListFileAll(pipelineCommit, "/")
	require.NoError(t, err)
	require.ElementsEqualUnderFn(t, []string{"/pfs_files", "/s3_buckets", "/s3_files"}, files,
		func(i interface{}) interface{} {
			return i.(*pfs.FileInfo).File.Path
		})

	// check files in /pfs
	var buf bytes.Buffer
	c.GetFile(pipelineCommit, "pfs_files", &buf)
	require.True(t,
		strings.Contains(buf.String(), "out") && !strings.Contains(buf.String(), "input_repo"),
		"expected \"out\" but not \"input_repo\" in %s: %q", "pfs_files", buf.String())

	// check s3 buckets
	buf.Reset()
	c.GetFile(pipelineCommit, "s3_buckets", &buf)
	require.True(t,
		strings.Contains(buf.String(), "input_repo") && !strings.Contains(buf.String(), "out"),
		"expected \"input_repo\" but not \"out\" in %s: %q", "s3_buckets", buf.String())

	// Check files in input_repo
	buf.Reset()
	c.GetFile(pipelineCommit, "s3_files", &buf)
	require.Matches(t, "foo", buf.String())

	// Check that no service is left over
	k := tu.GetKubeClient(t)
	require.NoErrorWithinTRetry(t, 68*time.Second, func() error {
		svcs, err := k.CoreV1().Services(Namespace).List(metav1.ListOptions{})
		require.NoError(t, err)
		for _, s := range svcs.Items {
			if s.ObjectMeta.Name == ppsutil.SidecarS3GatewayService(jobInfo.Job.ID) {
				return errors.Errorf("service %q should be cleaned up by sidecar after job", s.ObjectMeta.Name)
			}
		}
		return nil
	})
}

func TestNamespaceInEndpoint(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c, _ := initPachClient(t)

	repo := tu.UniqueString(t.Name() + "_data")
	require.NoError(t, c.CreateRepo(repo))
	masterCommit := client.NewCommit(repo, "master", "")

	require.NoError(t, c.PutFile(masterCommit, "foo", strings.NewReader("foo")))

	pipeline := tu.UniqueString("Pipeline")
	pipelineCommit := client.NewCommit(pipeline, "master", "")
	_, err := c.PpsAPIClient.CreatePipeline(c.Ctx(), &pps.CreatePipelineRequest{
		Pipeline: client.NewPipeline(pipeline),
		Transform: &pps.Transform{
			Cmd: []string{"bash", "-x"},
			Stdin: []string{
				"echo \"${S3_ENDPOINT}\" >/pfs/out/s3_endpoint",
			},
		},
		ParallelismSpec: &pps.ParallelismSpec{Constant: 1},
		Input: &pps.Input{
			Pfs: &pps.PFSInput{
				Name:   "input_repo",
				Repo:   repo,
				Branch: "master",
				S3:     true,
				Glob:   "/",
			},
		},
	})
	require.NoError(t, err)

	commitInfo, err := c.InspectCommit(pipeline, "master", "")
	require.NoError(t, err)

	jobInfo, err := c.WaitJob(pipeline, commitInfo.Commit.ID, false)
	require.NoError(t, err)
	require.Equal(t, "JOB_SUCCESS", jobInfo.State.String())

	// check S3_ENDPOINT variable
	var buf bytes.Buffer
	c.GetFile(pipelineCommit, "s3_endpoint", &buf)
	require.True(t, strings.Contains(buf.String(), ".default"))
}

func TestS3Output(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c, userToken := initPachClient(t)

	repo := tu.UniqueString(t.Name() + "_data")
	require.NoError(t, c.CreateRepo(repo))
	masterCommit := client.NewCommit(repo, "master", "")

	require.NoError(t, c.PutFile(masterCommit, "foo", strings.NewReader("foo")))

	pipeline := tu.UniqueString("Pipeline")
	pipelineCommit := client.NewCommit(pipeline, "master", "")
	_, err := c.PpsAPIClient.CreatePipeline(c.Ctx(), &pps.CreatePipelineRequest{
		Pipeline: client.NewPipeline(pipeline),
		Transform: &pps.Transform{
			Image: "pachyderm/ubuntu-with-s3-clients:v0.0.1",
			Cmd:   []string{"bash", "-x"},
			Stdin: []string{
				"ls -R /pfs | aws --endpoint=${S3_ENDPOINT} s3 cp - s3://out/pfs_files",
				"aws --endpoint=${S3_ENDPOINT} s3 ls | aws --endpoint=${S3_ENDPOINT} s3 cp - s3://out/s3_buckets",
			},
			Env: map[string]string{
				"AWS_ACCESS_KEY_ID":     userToken,
				"AWS_SECRET_ACCESS_KEY": userToken,
			},
		},
		ParallelismSpec: &pps.ParallelismSpec{Constant: 1},
		Input: &pps.Input{
			Pfs: &pps.PFSInput{
				Name:   "input_repo",
				Repo:   repo,
				Branch: "master",
				Glob:   "/",
			},
		},
		S3Out: true,
	})
	require.NoError(t, err)

	commitInfo, err := c.InspectCommit(pipeline, "master", "")
	require.NoError(t, err)

	jobInfo, err := c.WaitJob(pipeline, commitInfo.Commit.ID, false)
	require.NoError(t, err)
	require.Equal(t, "JOB_SUCCESS", jobInfo.State.String())

	// Make sure ListFile works
	files, err := c.ListFileAll(pipelineCommit, "/")
	require.NoError(t, err)
	require.ElementsEqualUnderFn(t, []string{"/pfs_files", "/s3_buckets"}, files,
		func(i interface{}) interface{} {
			return i.(*pfs.FileInfo).File.Path
		})

	// check files in /pfs
	var buf bytes.Buffer
	c.GetFile(pipelineCommit, "pfs_files", &buf)
	require.True(t,
		!strings.Contains(buf.String(), "out") && strings.Contains(buf.String(), "input_repo"),
		"expected \"input_repo\" but not \"out\" in %s: %q", "pfs_files", buf.String())

	// check s3 buckets
	buf.Reset()
	c.GetFile(pipelineCommit, "s3_buckets", &buf)
	require.True(t,
		!strings.Contains(buf.String(), "input_repo") && strings.Contains(buf.String(), "out"),
		"expected \"out\" but not \"input_repo\" in %s: %q", "s3_buckets", buf.String())

	// Check that no service is left over
	k := tu.GetKubeClient(t)
	require.NoErrorWithinTRetry(t, 68*time.Second, func() error {
		svcs, err := k.CoreV1().Services(Namespace).List(metav1.ListOptions{})
		require.NoError(t, err)
		for _, s := range svcs.Items {
			if s.ObjectMeta.Name == ppsutil.SidecarS3GatewayService(jobInfo.Job.ID) {
				return errors.Errorf("service %q should be cleaned up by sidecar after job", s.ObjectMeta.Name)
			}
		}
		return nil
	})
}

func TestFullS3(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c, userToken := initPachClient(t)

	repo := tu.UniqueString(t.Name() + "_data")
	require.NoError(t, c.CreateRepo(repo))
	masterCommit := client.NewCommit(repo, "master", "")

	require.NoError(t, c.PutFile(masterCommit, "foo", strings.NewReader("foo")))

	pipeline := tu.UniqueString("Pipeline")
	pipelineCommit := client.NewCommit(pipeline, "master", "")
	_, err := c.PpsAPIClient.CreatePipeline(c.Ctx(), &pps.CreatePipelineRequest{
		Pipeline: client.NewPipeline(pipeline),
		Transform: &pps.Transform{
			Image: "pachyderm/ubuntu-with-s3-clients:v0.0.1",
			Cmd:   []string{"bash", "-x"},
			Stdin: []string{
				"ls -R /pfs | aws --endpoint=${S3_ENDPOINT} s3 cp - s3://out/pfs_files",
				"aws --endpoint=${S3_ENDPOINT} s3 ls | aws --endpoint=${S3_ENDPOINT} s3 cp - s3://out/s3_buckets",
			},
			Env: map[string]string{
				"AWS_ACCESS_KEY_ID":     userToken,
				"AWS_SECRET_ACCESS_KEY": userToken,
			},
		},
		ParallelismSpec: &pps.ParallelismSpec{Constant: 1},
		Input: &pps.Input{
			Pfs: &pps.PFSInput{
				Name:   "input_repo",
				Repo:   repo,
				Branch: "master",
				S3:     true,
				Glob:   "/",
			},
		},
		S3Out: true,
	})
	require.NoError(t, err)

	commitInfo, err := c.InspectCommit(pipeline, "master", "")
	require.NoError(t, err)

	jobInfo, err := c.WaitJob(pipeline, commitInfo.Commit.ID, false)
	require.NoError(t, err)
	require.Equal(t, "JOB_SUCCESS", jobInfo.State.String())

	// Make sure ListFile works
	files, err := c.ListFileAll(pipelineCommit, "/")
	require.NoError(t, err)
	require.ElementsEqualUnderFn(t, []string{"/pfs_files", "/s3_buckets"}, files,
		func(i interface{}) interface{} {
			return i.(*pfs.FileInfo).File.Path
		})

	// check files in /pfs
	var buf bytes.Buffer
	c.GetFile(pipelineCommit, "pfs_files", &buf)
	require.True(t,
		!strings.Contains(buf.String(), "input_repo") && !strings.Contains(buf.String(), "out"),
		"expected neither \"out\" nor \"input_repo\" in %s: %q", "pfs_files", buf.String())

	// check s3 buckets
	buf.Reset()
	c.GetFile(pipelineCommit, "s3_buckets", &buf)
	require.True(t,
		strings.Contains(buf.String(), "out") && strings.Contains(buf.String(), "input_repo"),
		"expected both \"input_repo\" and \"out\" in %s: %q", "s3_buckets", buf.String())

	// Check that no service is left over
	k := tu.GetKubeClient(t)
	require.NoErrorWithinTRetry(t, 68*time.Second, func() error {
		svcs, err := k.CoreV1().Services(Namespace).List(metav1.ListOptions{})
		require.NoError(t, err)
		for _, s := range svcs.Items {
			if s.ObjectMeta.Name == ppsutil.SidecarS3GatewayService(jobInfo.Job.ID) {
				return errors.Errorf("service %q should be cleaned up by sidecar after job", s.ObjectMeta.Name)
			}
		}
		return nil
	})
}

func TestS3SkippedDatums(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	name := t.Name()

	c, userToken := initPachClient(t)

	t.Run("S3Inputs", func(t *testing.T) {
		s3in := tu.UniqueString(name + "_s3_data")
		require.NoError(t, c.CreateRepo(s3in))
		pfsin := tu.UniqueString(name + "_pfs_data")
		require.NoError(t, c.CreateRepo(pfsin))

		s3Commit := client.NewCommit(s3in, "master", "")
		// Pipelines with S3 inputs should still skip datums, as long as the S3 input
		// hasn't changed. We'll check this by reading from a repo that isn't a
		// pipeline input
		background := tu.UniqueString(name + "_bg_data")
		require.NoError(t, c.CreateRepo(background))

		require.NoError(t, c.PutFile(s3Commit, "file", strings.NewReader("foo")))

		pipeline := tu.UniqueString("Pipeline")
		pipelineCommit := client.NewCommit(pipeline, "master", "")
		_, err := c.PpsAPIClient.CreatePipeline(c.Ctx(), &pps.CreatePipelineRequest{
			Pipeline: client.NewPipeline(pipeline),
			Transform: &pps.Transform{
				Image: "pachyderm/ubuntu-with-s3-clients:v0.0.1",
				Cmd:   []string{"bash", "-x"},
				Stdin: []string{
					fmt.Sprintf(
						// access background repo via regular s3g (not S3_ENDPOINT, which
						// can only access inputs)
						"aws --endpoint=http://pachd.%s:1600 s3 cp s3://master.%s/round /tmp/bg",
						Namespace, background,
					),
					"aws --endpoint=${S3_ENDPOINT} s3 cp s3://s3g_in/file /tmp/s3in",
					"cat /pfs/pfs_in/* >/tmp/pfsin",
					"echo \"$(cat /tmp/bg) $(cat /tmp/pfsin) $(cat /tmp/s3in)\" >/pfs/out/out",
				},
				Env: map[string]string{
					"AWS_ACCESS_KEY_ID":     userToken,
					"AWS_SECRET_ACCESS_KEY": userToken,
				},
			},
			ParallelismSpec: &pps.ParallelismSpec{Constant: 1},
			Input: &pps.Input{
				Cross: []*pps.Input{
					{Pfs: &pps.PFSInput{
						Name:   "pfs_in",
						Repo:   pfsin,
						Branch: "master",
						Glob:   "/*",
					}},
					{Pfs: &pps.PFSInput{
						Name:   "s3g_in",
						Repo:   s3in,
						Branch: "master",
						S3:     true,
						Glob:   "/",
					}},
				}},
		})
		require.NoError(t, err)

		_, err = c.WaitCommit(pipeline, "master", "")
		require.NoError(t, err)

		// Part 1: add files in pfs input w/o changing s3 input. Old files in
		// 'pfsin' should be skipped datums
		// ----------------------------------------------------------------------
		for i := 0; i < 10; i++ {
			// Increment "/round" in 'background'
			iS := fmt.Sprintf("%d", i)
			bgc, err := c.StartCommit(background, "master")
			require.NoError(t, err)
			c.DeleteFile(bgc, "/round")
			require.NoError(t, c.PutFile(bgc, "/round", strings.NewReader(iS)))
			c.FinishCommit(background, bgc.Branch.Name, bgc.ID)

			//  Put new file in 'pfsin' to create a new datum and trigger a job
			require.NoError(t, c.PutFile(client.NewCommit(pfsin, "master", ""), iS, strings.NewReader(iS)))

			_, err = c.WaitCommit(pipeline, "master", "")
			require.NoError(t, err)

			jis, err := c.ListJob(pipeline, nil, 0, false)
			require.NoError(t, err)
			require.Equal(t, i+2, len(jis)) // one empty job w/ initial s3in commit
			for j := 0; j < len(jis); j++ {
				require.Equal(t, "JOB_SUCCESS", jis[j].State.String())
			}

			// check output
			var buf bytes.Buffer
			c.GetFile(pipelineCommit, "out", &buf)
			s := bufio.NewScanner(&buf)
			for s.Scan() {
				// [0] = bg, [1] = pfsin, [2] = s3in
				p := strings.Split(s.Text(), " ")
				// Check that bg is the same as the datum; this implies that bg is not
				// being re-read during each job and the datum is being skipped (what we
				// want)
				require.Equal(t, p[0], p[1], "line: "+s.Text())
				require.Equal(t, p[2], "foo", "line: "+s.Text()) // s3 input is being read but not changing
			}
		}

		// Part 2: change s3 input. All old datums should get reprocessed
		// --------------------------------------------------------------
		// Increment "/round" in 'background'
		bgc, err := c.StartCommit(background, "master")
		require.NoError(t, err)
		c.DeleteFile(bgc, "/round")
		require.NoError(t, c.PutFile(bgc, "/round", strings.NewReader("10")))
		c.FinishCommit(background, bgc.Branch.Name, bgc.ID)

		//  Put new file in 's3in' to create a new datum and trigger a job
		s3c, err := c.StartCommit(s3in, "master")
		require.NoError(t, err)
		c.DeleteFile(s3Commit, "/file")
		require.NoError(t, c.PutFile(s3Commit, "/file", strings.NewReader("bar")))
		c.FinishCommit(s3in, s3c.Branch.Name, s3c.ID)

		_, err = c.WaitCommit(pipeline, "master", "")
		require.NoError(t, err)

		jis, err := c.ListJob(pipeline, nil, 0, false)
		require.NoError(t, err)
		require.Equal(t, 12, len(jis))
		for j := 0; j < len(jis); j++ {
			require.Equal(t, "JOB_SUCCESS", jis[j].State.String())

			// Check that no service is left over
			k := tu.GetKubeClient(t)
			require.NoErrorWithinTRetry(t, 68*time.Second, func() error {
				svcs, err := k.CoreV1().Services(Namespace).List(metav1.ListOptions{})
				require.NoError(t, err)
				for _, s := range svcs.Items {
					if s.ObjectMeta.Name == ppsutil.SidecarS3GatewayService(jis[j].Job.ID) {
						return errors.Errorf("service %q should be cleaned up by sidecar after job", s.ObjectMeta.Name)
					}
				}
				return nil
			})
		}

		// check output
		var buf bytes.Buffer
		rc, err := c.GetFileTAR(pipelineCommit, "out")
		require.NoError(t, err)
		defer rc.Close()
		require.NoError(t, tarutil.ConcatFileContent(&buf, rc))
		s := bufio.NewScanner(&buf)
		var seen [10]bool // One per file in 'pfsin'
		for s.Scan() {
			// [0] = bg, [1] = pfsin, [2] = s3in
			p := strings.Split(s.Text(), " ")
			// Updating the S3 input should've altered all datums, forcing all files
			// to be reprocessed and changing p[0] in every line of "out"
			require.Equal(t, p[0], "10")
			pfsinI, err := strconv.Atoi(p[1])
			require.NoError(t, err)
			require.False(t, seen[pfsinI])
			seen[pfsinI] = true
			require.Equal(t, p[2], "bar") // s3 input is now "bar" everywhere
		}
		for j := 0; j < len(seen); j++ {
			require.True(t, seen[j], "%d", j) // all datums from pfsin were reprocessed
		}
	})

	t.Run("S3Output", func(t *testing.T) {
		// TODO(2.0 required): Investigate hang
		t.Skip("Investigate hang")
		repo := tu.UniqueString(name + "_pfs_data")
		require.NoError(t, c.CreateRepo(repo))
		// Pipelines with S3 output should not skip datums, as they have no way of
		// tracking which output data should be associated with which input data.
		// We'll check this by reading from a repo that isn't a pipeline input
		background := tu.UniqueString(name + "_bg_data")
		require.NoError(t, c.CreateRepo(background))

		pipeline := tu.UniqueString("Pipeline")
		_, err := c.PpsAPIClient.CreatePipeline(c.Ctx(), &pps.CreatePipelineRequest{
			Pipeline: client.NewPipeline(pipeline),
			Transform: &pps.Transform{
				Image: "pachyderm/ubuntu-with-s3-clients:v0.0.1",
				Cmd:   []string{"bash", "-x"},
				Stdin: []string{
					fmt.Sprintf(
						// access background repo via regular s3g (not S3_ENDPOINT, which
						// can only access inputs)
						"aws --endpoint=http://pachd.%s:1600 s3 cp s3://master.%s/round /tmp/bg",
						Namespace, background,
					),
					"cat /pfs/in/* >/tmp/pfsin",
					// Write the "background" value to a new file in every datum. As
					// 'aws s3 cp' is destructive, datums will overwrite each others
					// values unless each datum writes to a unique key. This way, we
					// should see a file for every datum processed written in every job
					"aws --endpoint=${S3_ENDPOINT} s3 cp /tmp/bg s3://out/\"$(cat /tmp/pfsin)\"",
					// Also write a file that is itself named for the background value.
					// Because S3-out jobs don't merge their outputs with their parent's
					// outputs, we should only see one such file in every job's output.
					"aws --endpoint=${S3_ENDPOINT} s3 cp /tmp/bg s3://out/bg/\"$(cat /tmp/bg)\"",
				},
				Env: map[string]string{
					"AWS_ACCESS_KEY_ID":     userToken,
					"AWS_SECRET_ACCESS_KEY": userToken,
				},
			},
			ParallelismSpec: &pps.ParallelismSpec{Constant: 1},
			Input: &pps.Input{
				Pfs: &pps.PFSInput{
					Name:   "in",
					Repo:   repo,
					Branch: "master",
					Glob:   "/*",
				},
			},
			S3Out: true,
		})
		require.NoError(t, err)

		masterCommit := client.NewCommit(repo, "master", "")
		pipelineCommit := client.NewCommit(pipeline, "master", "")
		// Add files to 'repo'. Old files in 'repo' should be reprocessed in every
		// job, changing the 'background' field in the output
		for i := 0; i < 10; i++ {
			// Increment "/round" in 'background'
			iS := strconv.Itoa(i)
			bgc, err := c.StartCommit(background, "master")
			require.NoError(t, err)
			c.DeleteFile(bgc, "/round")
			require.NoError(t, c.PutFile(bgc, "/round", strings.NewReader(iS)))
			c.FinishCommit(background, bgc.Branch.Name, bgc.ID)

			// Put new file in 'repo' to create a new datum and trigger a job
			require.NoError(t, c.PutFile(masterCommit, iS, strings.NewReader(iS)))

			_, err = c.WaitCommit(pipeline, "master", "")
			require.NoError(t, err)
			jis, err := c.ListJob(pipeline, nil, 0, false)
			require.NoError(t, err)
			require.Equal(t, i+1, len(jis))
			for j := 0; j < len(jis); j++ {
				require.Equal(t, "JOB_SUCCESS", jis[j].State.String())
			}

			for j := 0; j <= i; j++ {
				var buf bytes.Buffer
				require.NoError(t, c.GetFile(pipelineCommit, strconv.Itoa(j), &buf))
				// buf contains the background value; this should be updated in every
				// datum by every job, because this is an S3Out pipeline
				require.Equal(t, iS, buf.String())
			}

			// Make sure that there's exactly one file in 'bg/'--this confirms that
			// the output commit isn't merged with its parent, it only contains the
			// result of processing all datums
			// -------------------------------
			// check no extra files
			fis, err := c.ListFileAll(pipelineCommit, "/bg")
			require.NoError(t, err)
			require.Equal(t, 1, len(fis))
			// check that expected file exists
			fis, err = c.ListFileAll(pipelineCommit, fmt.Sprintf("/bg/%d", i))
			require.NoError(t, err)
			require.Equal(t, 1, len(fis))

		}
	})
}
