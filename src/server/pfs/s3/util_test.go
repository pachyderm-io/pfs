package s3

import (
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	minio "github.com/minio/minio-go/v6"
	"github.com/pachyderm/pachyderm/src/client"
	"github.com/pachyderm/pachyderm/src/client/pkg/require"
	"github.com/pachyderm/pachyderm/src/server/pkg/serviceenv"
)

func getObject(t *testing.T, minioClient *minio.Client, bucket, file string) (string, error) {
	t.Helper()

	obj, err := minioClient.GetObject(bucket, file, minio.GetObjectOptions{})
	if err != nil {
		return "", err
	}
	defer func() { err = obj.Close() }()
	bytes, err := ioutil.ReadAll(obj)
	if err != nil {
		return "", err
	}
	return string(bytes), err
}

func checkListObjects(t *testing.T, ch <-chan minio.ObjectInfo, startTime *time.Time, endTime *time.Time, expectedFiles []string, expectedDirs []string) {
	t.Helper()

	// sort expected files/dirs, as the S3 gateway should always return
	// results in sorted order
	sort.Strings(expectedFiles)
	sort.Strings(expectedDirs)

	actualFiles := []minio.ObjectInfo{}
	actualDirs := []minio.ObjectInfo{}
	for obj := range ch {
		require.NoError(t, obj.Err)

		if strings.HasSuffix(obj.Key, "/") {
			actualDirs = append(actualDirs, obj)
		} else {
			actualFiles = append(actualFiles, obj)
		}
	}

	require.Equal(t, len(expectedFiles), len(actualFiles), "unexpected number of files")
	require.Equal(t, len(expectedDirs), len(actualDirs), "unexpected number of dirs")

	for i, expectedFilename := range expectedFiles {
		actualFile := actualFiles[i]
		require.Equal(t, expectedFilename, actualFile.Key)
		require.True(t, len(actualFile.ETag) > 0, fmt.Sprintf("unexpected empty etag for %s", expectedFilename))
		expectedLen := int64(len(filepath.Base(expectedFilename)) + 1)
		require.Equal(t, expectedLen, actualFile.Size, fmt.Sprintf("unexpected file length for %s", expectedFilename))

		if startTime != nil {
			require.True(t, startTime.Before(actualFile.LastModified), fmt.Sprintf("unexpected last modified for %s", expectedFilename))
		}

		if endTime != nil {
			require.True(t, endTime.After(actualFile.LastModified), fmt.Sprintf("unexpected last modified for %s", expectedFilename))
		}
	}

	for i, expectedDirname := range expectedDirs {
		actualDir := actualDirs[i]
		require.Equal(t, expectedDirname, actualDir.Key)
		require.True(t, len(actualDir.ETag) == 0, fmt.Sprintf("unexpected etag for %s: %s", expectedDirname, actualDir.ETag))
		require.Equal(t, int64(0), actualDir.Size)
		require.True(t, actualDir.LastModified.IsZero(), fmt.Sprintf("unexpected last modified for %s", expectedDirname))
	}
}

func putListFileTestObject(t *testing.T, pachClient *client.APIClient, repo string, commitID string, dir string, i int) {
	t.Helper()
	_, err := pachClient.PutFile(
		repo,
		commitID,
		fmt.Sprintf("%s%d", dir, i),
		strings.NewReader(fmt.Sprintf("%d\n", i)),
	)
	require.NoError(t, err)
}

func bucketNotFoundError(t *testing.T, err error) {
	t.Helper()
	require.YesError(t, err)
	require.Equal(t, "The specified bucket does not exist.", err.Error())
}

func keyNotFoundError(t *testing.T, err error) {
	t.Helper()
	require.YesError(t, err)
	require.Equal(t, "The specified key does not exist.", err.Error())
}

func notImplementedError(t *testing.T, err error) {
	t.Helper()
	require.YesError(t, err)
	require.Equal(t, "This functionality is not implemented.", err.Error())
}

func fileHash(t *testing.T, name string) (int64, []byte) {
	t.Helper()

	f, err := os.Open(name)
	require.NoError(t, err)

	fi, err := f.Stat()
	require.NoError(t, err)

	hash := md5.New()
	_, err = io.Copy(hash, f)
	require.NoError(t, err)

	// make sure it's not the hash of an empty string
	hashSum := hash.Sum(nil)
	require.NotEqual(t, hashSum, []byte{0xd4, 0x1d, 0x8c, 0xd9, 0x8f, 0x0, 0xb2, 0x4, 0xe9, 0x80, 0x9, 0x98, 0xec, 0xf8, 0x42, 0x7e})

	return fi.Size(), hashSum
}

func testRunner(t *testing.T, group string, driver Driver, runner func(t *testing.T, pachClient *client.APIClient, minioClient *minio.Client)) {
	env := serviceenv.InitPachOnlyTestEnv(t, &serviceenv.Configuration{
		GlobalConfiguration: &serviceenv.GlobalConfiguration{
			S3GatewayPort: 0,
		}})
	server, err := Server(env, driver)
	require.NoError(t, err)
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	go func() {
		server.Serve(listener)
	}()

	port := listener.Addr().(*net.TCPAddr).Port

	pachClient, err := client.NewForTest()
	require.NoError(t, err)

	minioClient, err := minio.NewV4(fmt.Sprintf("127.0.0.1:%d", port), "", "", false)
	require.NoError(t, err)

	t.Run(group, func(t *testing.T) {
		runner(t, pachClient, minioClient)
	})

	require.NoError(t, server.Shutdown(context.Background()))
}
