package s3

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gobwas/glob"
	"github.com/gogo/protobuf/types"
	"github.com/pachyderm/pachyderm/src/client"
	pfsClient "github.com/pachyderm/pachyderm/src/client/pfs"
	"github.com/pachyderm/pachyderm/src/client/pkg/errutil"
	pfsServer "github.com/pachyderm/pachyderm/src/server/pfs"
)

const defaultMaxKeys int = 1000

// the raw XML returned for a request to get the location of a bucket
const locationSource = `<?xml version="1.0" encoding="UTF-8"?>
<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">PACHYDERM</LocationConstraint>`

// ListBucketResult is an XML-encodable listing of files/objects in a
// repo/bucket
type ListBucketResult struct {
	Contents       []Contents       `xml:"Contents"`
	CommonPrefixes []CommonPrefixes `xml:"CommonPrefixes"`
	Delimiter      string           `xml:"Delimiter,omitempty"`
	IsTruncated    bool             `xml:"IsTruncated"`
	Marker         string           `xml:"Marker"`
	MaxKeys        int              `xml:"MaxKeys"`
	Name           string           `xml:"Name"`
	NextMarker     string           `xml:"NextMarker,omitempty"`
	Prefix         string           `xml:"Prefix"`
}

func (r *ListBucketResult) isFull() bool {
	return len(r.Contents)+len(r.CommonPrefixes) >= r.MaxKeys
}

// Contents is an individual file/object
type Contents struct {
	Key          string    `xml:"Key"`
	LastModified time.Time `xml:"LastModified"`
	ETag         string    `xml:"ETag"`
	Size         uint64    `xml:"Size"`
	StorageClass string    `xml:"StorageClass"`
	Owner        User      `xml:"Owner"`
}

func newContents(fileInfo *pfsClient.FileInfo) (Contents, error) {
	t, err := types.TimestampFromProto(fileInfo.Committed)
	if err != nil {
		return Contents{}, err
	}

	return Contents{
		Key:          fileInfo.File.Path,
		LastModified: t,
		ETag:         fmt.Sprintf("%x", fileInfo.Hash),
		Size:         fileInfo.SizeBytes,
		StorageClass: storageClass,
		Owner:        defaultUser,
	}, nil
}

// CommonPrefixes is an individual PFS directory
type CommonPrefixes struct {
	Prefix string `xml:"Prefix"`
	Owner  User   `xml:"Owner"`
}

func newCommonPrefixes(dir string) CommonPrefixes {
	return CommonPrefixes{
		Prefix: fmt.Sprintf("%s/", dir),
		Owner:  defaultUser,
	}
}

type bucketHandler struct {
	pc *client.APIClient
}

func newBucketHandler(pc *client.APIClient) bucketHandler {
	return bucketHandler{pc: pc}
}

func (h bucketHandler) location(w http.ResponseWriter, r *http.Request) {
	repo, branch := bucketArgs(w, r)

	_, err := h.pc.InspectBranch(repo, branch)
	if err != nil {
		maybeNotFoundError(w, r, err)
		return
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(locationSource))
}

func (h bucketHandler) get(w http.ResponseWriter, r *http.Request) {
	repo, branch := bucketArgs(w, r)

	// ensure the branch exists and has a head
	branchInfo, err := h.pc.InspectBranch(repo, branch)
	if err != nil {
		maybeNotFoundError(w, r, err)
		return
	}

	maxKeys := defaultMaxKeys
	maxKeysStr := r.FormValue("max-keys")
	if maxKeysStr != "" {
		i, err := strconv.Atoi(maxKeysStr)
		if err != nil || i < 0 || i > defaultMaxKeys {
			invalidArgument(w, r)
			return
		}
		maxKeys = i
	}

	delimiter := r.FormValue("delimiter")
	if delimiter != "" && delimiter != "/" {
		invalidDelimiterError(w, r)
		return
	}
	recursive := delimiter == ""

	result := &ListBucketResult{
		Name:        repo,
		Prefix:      r.FormValue("prefix"),
		Marker:      r.FormValue("marker"),
		Delimiter:   delimiter,
		MaxKeys:     maxKeys,
		IsTruncated: false,
	}

	if branchInfo.Head == nil {
		// if there's no head commit, just print an empty list of files
		writeXML(w, r, http.StatusOK, result)
		return
	}

	var pattern string
	if recursive {
		pattern = fmt.Sprintf("%s**", glob.QuoteMeta(result.Prefix))
	} else {
		pattern = fmt.Sprintf("%s*", glob.QuoteMeta(result.Prefix))
	}

	if err = h.pc.GlobFileF(result.Name, branch, pattern, func(fileInfo *pfsClient.FileInfo) error {
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

		if !strings.HasPrefix(fileInfo.File.Path, result.Prefix) {
			return nil
		}
		if fileInfo.File.Path <= result.Marker {
			return nil
		}

		if result.isFull() {
			if result.MaxKeys > 0 {
				result.IsTruncated = true
			}
			return errutil.ErrBreak
		}
		if fileInfo.FileType == pfsClient.FileType_FILE {
			contents, err := newContents(fileInfo)
			if err != nil {
				return err
			}

			result.Contents = append(result.Contents, contents)
		} else {
			result.CommonPrefixes = append(result.CommonPrefixes, newCommonPrefixes(fileInfo.File.Path))
		}

		return nil
	}); err != nil {
		internalError(w, r, err)
		return
	}

	if result.IsTruncated {
		if len(result.Contents) > 0 && len(result.CommonPrefixes) == 0 {
			result.NextMarker = result.Contents[len(result.Contents)-1].Key
		} else if len(result.Contents) == 0 && len(result.CommonPrefixes) > 0 {
			result.NextMarker = result.CommonPrefixes[len(result.CommonPrefixes)-1].Prefix
		} else if len(result.Contents) > 0 && len(result.CommonPrefixes) > 0 {
			lastContents := result.Contents[len(result.Contents)-1].Key
			lastCommonPrefixes := result.CommonPrefixes[len(result.CommonPrefixes)-1].Prefix

			if lastContents > lastCommonPrefixes {
				result.NextMarker = lastContents
			} else {
				result.NextMarker = lastCommonPrefixes
			}
		}
	}

	writeXML(w, r, http.StatusOK, result)
}

func (h bucketHandler) put(w http.ResponseWriter, r *http.Request) {
	repo, branch := bucketArgs(w, r)

	err := h.pc.CreateRepo(repo)
	if err != nil {
		if errutil.IsAlreadyExistError(err) {
			// Bucket already exists - this is not an error so long as the
			// branch being created is new. Verify if that is the case now,
			// since PFS' `CreateBranch` won't error out.
			_, err := h.pc.InspectBranch(repo, branch)
			if err != nil {
				if !pfsServer.IsBranchNotFoundErr(err) {
					internalError(w, r, err)
					return
				}
			} else {
				bucketAlreadyOwnedByYouError(w, r)
				return
			}
		} else {
			internalError(w, r, err)
			return
		}
	}

	err = h.pc.CreateBranch(repo, branch, "", nil)
	if err != nil {
		internalError(w, r, err)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h bucketHandler) del(w http.ResponseWriter, r *http.Request) {
	repo, branch := bucketArgs(w, r)

	// `DeleteBranch` does not return an error if a non-existing branch is
	// deleting. So first, we verify that the branch exists so we can
	// otherwise return a 404.
	branchInfo, err := h.pc.InspectBranch(repo, branch)
	if err != nil {
		maybeNotFoundError(w, r, err)
		return
	}

	if branchInfo.Head != nil {
		hasFiles := false
		err = h.pc.Walk(branchInfo.Branch.Repo.Name, branchInfo.Head.ID, "", func(fileInfo *pfsClient.FileInfo) error {
			if fileInfo.FileType == pfsClient.FileType_FILE {
				hasFiles = true
				return errutil.ErrBreak
			}
			return nil
		})
		if err != nil {
			internalError(w, r, err)
			return
		}

		if hasFiles {
			bucketNotEmptyError(w, r)
			return
		}
	}

	err = h.pc.DeleteBranch(repo, branch, false)
	if err != nil {
		internalError(w, r, err)
		return
	}

	repoInfo, err := h.pc.InspectRepo(repo)
	if err != nil {
		internalError(w, r, err)
		return
	}

	// delete the repo if this was the last branch
	if len(repoInfo.Branches) == 0 {
		err = h.pc.DeleteRepo(repo, false)
		if err != nil {
			internalError(w, r, err)
			return
		}
	}

	w.WriteHeader(http.StatusNoContent)
}
