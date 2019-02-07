package cmds

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/pachyderm/pachyderm/src/client/pkg/require"
	tu "github.com/pachyderm/pachyderm/src/server/pkg/testutil"
)

const FakeAWSAccessKeyID = "MADEUPAWSACCESSKEYID"
const FakeAWSSecret = "YIUo7lLijgheOTbSR57DCv8eGVklj8UHUQb9aTDf"

func TestWarnInvalidAmazonCreds(t *testing.T) {
	c := tu.Cmd("pachctl", "deploy", "amazon", "bucket", "us-west-1", "10",
		"--credentials=lol,wat",
		"--dynamic-etcd-nodes=1", "--dry-run", "--no-port-forwarding")
	var warningMsg bytes.Buffer
	c.Stdin = strings.NewReader(strings.Repeat("y\n", 10))
	c.Stderr = ioutil.Discard
	c.Stdout = &warningMsg
	err := c.Run()
	require.NoError(t, err)
	require.Matches(t, "invalid", warningMsg.String())
}

func TestWarnBadRegion(t *testing.T) {
	c := tu.Cmd("pachctl", "deploy", "amazon", "bucket", "bad-region", "10",
		fmt.Sprintf("--credentials=%s,%s", FakeAWSAccessKeyID, FakeAWSSecret),
		"--dynamic-etcd-nodes=1", "--dry-run", "--no-port-forwarding")
	var warningMsg bytes.Buffer
	c.Stdin = strings.NewReader(strings.Repeat("y\n", 10))
	c.Stderr = ioutil.Discard
	c.Stdout = &warningMsg
	err := c.Run()
	require.NoError(t, err)
	require.Matches(t, "invalid", warningMsg.String())
}

func TestStripS3Prefix(t *testing.T) {
	c := tu.Cmd("pachctl", "deploy", "amazon", "s3://bucket", "us-west-1", "10",
		fmt.Sprintf("--credentials=%s,%s", FakeAWSAccessKeyID, FakeAWSSecret),
		"--dynamic-etcd-nodes=1", "--dry-run", "--no-port-forwarding")
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
			if err == io.EOF {
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
	t.Fatalf("could not find storage secret in kubernetes manifest")
}
