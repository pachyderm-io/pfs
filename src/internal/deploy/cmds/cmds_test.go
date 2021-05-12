package cmds

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/pachyderm/pachyderm/v2/src/internal/errors"
	"github.com/pachyderm/pachyderm/v2/src/internal/require"
	tu "github.com/pachyderm/pachyderm/v2/src/internal/testutil"
)

const FakeAWSAccessKeyID = "MADEUPAWSACCESSKEYID"
const FakeAWSSecret = "YIUo7lLijgheOTbSR57DCv8eGVklj8UHUQb9aTDf"

func TestDashImageExists(t *testing.T) {
	if os.Getenv("RUN_BAD_TESTS") == "" {
		t.Skip("Skipping because RUN_BAD_TESTS was empty")
	}
	c := exec.Command("docker", "pull", fmt.Sprintf("%s:%s", defaultDashImage, defaultDashVersion))
	require.NoError(t, c.Run())
}

func TestWarnInvalidAmazonCreds(t *testing.T) {
	c := tu.Cmd("pachctl", "deploy", "amazon", "us-west-1", "10", "bucket",
		"--credentials=lol,wat",
		"--dynamic-etcd-nodes=1", "--dry-run")
	var warningMsg bytes.Buffer
	c.Stdin = strings.NewReader(strings.Repeat("y\n", 10))
	c.Stderr = &warningMsg
	c.Stdout = ioutil.Discard
	err := c.Run()
	require.NoError(t, err)
	require.Matches(t, "seems invalid", warningMsg.String())
}

func TestWarnBadRegion(t *testing.T) {
	c := tu.Cmd("pachctl", "deploy", "amazon", "bad-region", "10", "bucket",
		fmt.Sprintf("--credentials=%s,%s", FakeAWSAccessKeyID, FakeAWSSecret),
		"--dynamic-etcd-nodes=1", "--dry-run")
	var warningMsg bytes.Buffer
	c.Stdin = strings.NewReader(strings.Repeat("y\n", 10))
	c.Stderr = &warningMsg
	c.Stdout = ioutil.Discard
	err := c.Run()
	require.NoError(t, err)
	require.Matches(t, "seems invalid", warningMsg.String())
}

func TestStripS3Prefix(t *testing.T) {
	c := tu.Cmd("pachctl", "deploy", "amazon", "us-west-1", "10", "s3://bucket",
		fmt.Sprintf("--credentials=%s,%s", FakeAWSAccessKeyID, FakeAWSSecret),
		"--dynamic-etcd-nodes=1", "--dry-run")
	var k8sManifest bytes.Buffer
	c.Stdout = &k8sManifest
	err := c.Run()
	require.NoError(t, err)

	var manifestPiece struct {
		Data struct {
			AmazonBucket string `json:"amazon-bucket"`
		} `json:"data"`
	}

	// k8s manifest is a stream of json objects. We can't unmarshal the whole
	// thing (json.Unmarshal yields an error), so unmarshal objects from the
	// stream one at a time until we find & validate the storage secret
	d := json.NewDecoder(&k8sManifest)
	for {
		// decode next object
		if err := d.Decode(&manifestPiece); err != nil {
			if errors.Is(err, io.EOF) {
				t.Fatalf("never found S3 bucket name in kubernetes manifest")
			}
			t.Fatalf("could not deserialize json object: %v", err)
		}

		// figure out if the object was the storage secret--if so, make sure the
		// bucket name is right
		bucketName64 := manifestPiece.Data.AmazonBucket
		if bucketName64 != "" {
			bucketName, err := base64.RawStdEncoding.DecodeString(bucketName64)
			require.NoError(t, err)
			require.Equal(t, "bucket", string(bucketName)) // "s3://" removed

			return // done--success
		}
	}
}
