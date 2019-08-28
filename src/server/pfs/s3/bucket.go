package s3

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/gogo/protobuf/types"
	"github.com/gorilla/mux"
	glob "github.com/pachyderm/ohmyglob"
	pfsClient "github.com/pachyderm/pachyderm/src/client/pfs"
	pfsServer "github.com/pachyderm/pachyderm/src/server/pfs"
	"github.com/pachyderm/pachyderm/src/server/pkg/ancestry"
	"github.com/pachyderm/pachyderm/src/server/pkg/errutil"
	"github.com/pachyderm/s2"
)

func newContents(fileInfo *pfsClient.FileInfo) (s2.Contents, error) {
	t, err := types.TimestampFromProto(fileInfo.Committed)
	if err != nil {
		return s2.Contents{}, err
	}

	return s2.Contents{
		Key:          fileInfo.File.Path,
		LastModified: t,
		ETag:         fmt.Sprintf("%x", fileInfo.Hash),
		Size:         fileInfo.SizeBytes,
		StorageClass: globalStorageClass,
		Owner:        defaultUser,
	}, nil
}

func newCommonPrefixes(dir string) s2.CommonPrefixes {
	return s2.CommonPrefixes{
		Prefix: fmt.Sprintf("%s/", dir),
		Owner:  defaultUser,
	}
}

func (c controller) GetLocation(r *http.Request, bucket string) (string, error) {
	vars := mux.Vars(r)
	pc, err := c.pachClient(vars["authAccessKey"])
	if err != nil {
		return "", err
	}
	repo, branch, err := bucketArgs(r, bucket)
	if err != nil {
		return "", err
	}

	_, err = pc.InspectBranch(repo, branch)
	if err != nil {
		return "", maybeNotFoundError(r, err)
	}

	return globalLocation, nil
}

func (c controller) ListObjects(r *http.Request, bucket, prefix, marker, delimiter string, maxKeys int) (*s2.ListObjectsResult, error) {
	vars := mux.Vars(r)
	pc, err := c.pachClient(vars["authAccessKey"])
	if err != nil {
		return nil, err
	}
	repo, branch, err := bucketArgs(r, bucket)
	if err != nil {
		return nil, err
	}

	if delimiter != "" && delimiter != "/" {
		return nil, invalidDelimiterError(r)
	}

	result := s2.ListObjectsResult{
		Contents:       []s2.Contents{},
		CommonPrefixes: []s2.CommonPrefixes{},
	}

	// ensure the branch exists and has a head
	branchInfo, err := pc.InspectBranch(repo, branch)
	if err != nil {
		return nil, maybeNotFoundError(r, err)
	}
	if branchInfo.Head == nil {
		// if there's no head commit, just print an empty list of files
		return &result, nil
	}

	recursive := delimiter == ""
	var pattern string
	if recursive {
		pattern = fmt.Sprintf("%s**", glob.QuoteMeta(prefix))
	} else {
		pattern = fmt.Sprintf("%s*", glob.QuoteMeta(prefix))
	}

	err = pc.GlobFileF(repo, branch, pattern, func(fileInfo *pfsClient.FileInfo) error {
		if fileInfo.FileType == pfsClient.FileType_DIR {
			if fileInfo.File.Path == "/" {
				// skip the root directory
				return nil
			}
			if recursive {
				// skip directories if recursing
				return nil
			}
		} else if fileInfo.FileType != pfsClient.FileType_FILE {
			// skip anything that isn't a file or dir
			return nil
		}

		fileInfo.File.Path = fileInfo.File.Path[1:] // strip leading slash

		if !strings.HasPrefix(fileInfo.File.Path, prefix) {
			return nil
		}
		if fileInfo.File.Path <= marker {
			return nil
		}

		if len(result.Contents)+len(result.CommonPrefixes) >= maxKeys {
			if maxKeys > 0 {
				result.IsTruncated = true
			}
			return errutil.ErrBreak
		}
		if fileInfo.FileType == pfsClient.FileType_FILE {
			c, err := newContents(fileInfo)
			if err != nil {
				return err
			}

			result.Contents = append(result.Contents, c)
		} else {
			result.CommonPrefixes = append(result.CommonPrefixes, newCommonPrefixes(fileInfo.File.Path))
		}

		return nil
	})

	return &result, err
}

func (c controller) CreateBucket(r *http.Request, bucket string) error {
	vars := mux.Vars(r)
	pc, err := c.pachClient(vars["authAccessKey"])
	if err != nil {
		return err
	}
	repo, branch, err := bucketArgs(r, bucket)
	if err != nil {
		return err
	}

	err = pc.CreateRepo(repo)
	if err != nil {
		if errutil.IsAlreadyExistError(err) {
			// Bucket already exists - this is not an error so long as the
			// branch being created is new. Verify if that is the case now,
			// since PFS' `CreateBranch` won't error out.
			_, err := pc.InspectBranch(repo, branch)
			if err != nil {
				if !pfsServer.IsBranchNotFoundErr(err) {
					return s2.InternalError(r, err)
				}
			} else {
				return s2.BucketAlreadyOwnedByYouError(r)
			}
		} else if ancestry.IsInvalidNameError(err) {
			return s2.InvalidBucketNameError(r)
		} else {
			return s2.InternalError(r, err)
		}
	}

	err = pc.CreateBranch(repo, branch, "", nil)
	if err != nil {
		if ancestry.IsInvalidNameError(err) {
			return s2.InvalidBucketNameError(r)
		}
		return s2.InternalError(r, err)
	}

	return nil
}

func (c controller) DeleteBucket(r *http.Request, bucket string) error {
	vars := mux.Vars(r)
	pc, err := c.pachClient(vars["authAccessKey"])
	if err != nil {
		return err
	}
	repo, branch, err := bucketArgs(r, bucket)
	if err != nil {
		return err
	}

	// `DeleteBranch` does not return an error if a non-existing branch is
	// deleting. So first, we verify that the branch exists so we can
	// otherwise return a 404.
	branchInfo, err := pc.InspectBranch(repo, branch)
	if err != nil {
		return maybeNotFoundError(r, err)
	}

	if branchInfo.Head != nil {
		hasFiles := false
		err = pc.Walk(branchInfo.Branch.Repo.Name, branchInfo.Head.ID, "", func(fileInfo *pfsClient.FileInfo) error {
			if fileInfo.FileType == pfsClient.FileType_FILE {
				hasFiles = true
				return errutil.ErrBreak
			}
			return nil
		})
		if err != nil {
			return s2.InternalError(r, err)
		}

		if hasFiles {
			return s2.BucketNotEmptyError(r)
		}
	}

	err = pc.DeleteBranch(repo, branch, false)
	if err != nil {
		return s2.InternalError(r, err)
	}

	repoInfo, err := pc.InspectRepo(repo)
	if err != nil {
		return s2.InternalError(r, err)
	}

	// delete the repo if this was the last branch
	if len(repoInfo.Branches) == 0 {
		err = pc.DeleteRepo(repo, false)
		if err != nil {
			return s2.InternalError(r, err)
		}
	}

	return nil
}

func (c *controller) ListObjectVersions(r *http.Request, repo, prefix, keyMarker, versionIDMarker string, delimiter string, maxKeys int) (*s2.ListObjectVersionsResult, error) {
	return nil, s2.NotImplementedError(r)
}

func (c *controller) GetBucketVersioning(r *http.Request, repo string) (string, error) {
	return s2.VersioningEnabled, nil
}

func (c *controller) SetBucketVersioning(r *http.Request, repo, status string) error {
	return s2.NotImplementedError(r)
}
