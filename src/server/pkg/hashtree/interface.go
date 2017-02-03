package hashtree

import (
	"github.com/pachyderm/pachyderm/src/client/pfs"
)

// ErrCode identifies different kinds of errors returned by methods in
// Interface below. The ErrCode of any such error can be retrieved with Code().
type ErrCode uint8

const (
	// OK is returned on success
	OK ErrCode = iota

	// Unknown is returned by Code() when an error wasn't emitted by the HashTree
	// implementation.
	Unknown

	// Internal is returned when a HashTree encounters a bug (usually due to the
	// violation of an internal invariant).
	Internal

	// PathNotFound is returned when Get() or DeleteFile() is called with a path
	// that doesn't lead to a node.
	PathNotFound

	// MalformedGlob is returned when Glob() is called with an invalid glob
	// pattern.
	MalformedGlob

	// PathConflict is returned when a path that is expected to point to a
	// directory in fact points to a file, or the reverse. For example:
	// 1. PutFile is called with a path that points to a directory.
	// 2. PutFile is called with a path that contains a prefix that
	//    points to a file.
	// 3. Merge is forced to merge a directory into a file
	PathConflict
)

// Interface is the signature of a HashTree provided by this library
type HashTree interface {
	// PutFile appends data to a file (and creates the file if it doesn't exist)
	PutFile(path string, blockRefs []*pfs.BlockRef) error

	// PutDir creates a directory (or does nothing if one exists)
	PutDir(path string) error

	// DeleteFile deletes a regular file or directory (along with its children).
	DeleteFile(path string) error

	// Get retrieves the contents of a regular file
	Get(path string) (*NodeProto, error)

	// List retrieves the list of files and subdirectories of the directory at
	// 'path'.
	List(path string) ([]*NodeProto, error)

	// Glob returns a list of files and directories that match 'pattern'
	Glob(pattern string) ([]*NodeProto, error)

	// Merge adds all of the files and directories in each tree in 'trees' into
	// this tree. The effect is equivalent to calling this.PutFile with every
	// file in every tree in 'tree', though the performance may be slightly
	// better.
	Merge(trees []HashTree) error
}
