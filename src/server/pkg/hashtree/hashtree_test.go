package hashtree

import (
	"crypto/sha256"
	"fmt"
	"runtime"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/pachyderm/pachyderm/src/client/pfs"
	"github.com/pachyderm/pachyderm/src/client/pkg/require"
)

// br parses a string as a BlockRef
func br(s ...string) []*pfs.BlockRef {
	result := make([]*pfs.BlockRef, len(s))
	for i, ss := range s {
		result[i] = &pfs.BlockRef{}
		proto.UnmarshalText(ss, result[i])
		if result[i].Range == nil {
			result[i].Range = &pfs.ByteRange{
				Lower: 0,
				Upper: 1, // Makes sure test files have non-zero size
			}
		}
	}
	return result
}

// Convenience function to convert a list of strings to []interface{} for
// EqualOneOf
func i(ss ...string) []interface{} {
	result := make([]interface{}, len(ss))
	for i, v := range ss {
		result[i] = v
	}
	return result
}

// requireSame compares 'h' to another hash tree (e.g. to make sure that it
// hasn't changed)
func (h *HashTreeProto) requireSame(t *testing.T, other *HashTreeProto) {
	// Make sure 'h' is still the same
	_, file, line, _ := runtime.Caller(1)
	require.True(t, proto.Equal(h, other),
		fmt.Sprintf("%s %s:%d\n%s %s\n%s  %s\n",
			"requireSame called at", file, line,
			"expected:", proto.MarshalTextString(h),
			"but got:", proto.MarshalTextString(other)))
}

// requireOperationInvariant makes sure that h isn't affected by calling 'op'.
// Good for checking that adding and deleting a file does nothing persistent,
// etc. This is separate from 'requireSame()' because often we want to test that
// an operation is invariant on several slightly different trees, and with this
// we only have to define 'op' once.
func (h *HashTreeProto) requireOperationInvariant(t *testing.T, op func()) {
	preop := proto.Clone(h)

	// perform operation on 'h'
	op()

	// Make sure 'h' is still the same
	_, file, line, _ := runtime.Caller(1)
	require.True(t, proto.Equal(preop, h),
		fmt.Sprintf("%s %s:%d\n%s  %s\n%s %s\n",
			"requireOperationInvariant called at", file, line,
			"pre-op HashTree:", proto.MarshalTextString(preop),
			"post-op HashTree:", proto.MarshalTextString(h)))
}

func TestPutFileBasic(t *testing.T) {
	// Put a file
	h := HashTreeProto{}
	h.PutFile("/foo", br(`block{hash:"20c27"}`))
	require.Equal(t, int64(1), h.Fs["/foo"].SubtreeSize)
	require.Equal(t, int64(1), h.Fs[""].SubtreeSize)

	// Put a file under a directory and make sure changes are propagated upwards
	h.PutFile("/dir/bar", br(`block{hash:"ebc57"}`))
	require.Equal(t, int64(1), h.Fs["/dir/bar"].SubtreeSize)
	require.Equal(t, int64(1), h.Fs["/dir"].SubtreeSize)
	require.Equal(t, int64(2), h.Fs[""].SubtreeSize)
	h.PutFile("/dir/buzz", br(`block{hash:"8e02c"}`))
	require.Equal(t, int64(1), h.Fs["/dir/buzz"].SubtreeSize)
	require.Equal(t, int64(2), h.Fs["/dir"].SubtreeSize)
	require.Equal(t, int64(3), h.Fs[""].SubtreeSize)

	// inspect h
	nodes, err := h.List("/")
	require.NoError(t, err)
	require.Equal(t, 2, len(nodes))
	for _, node := range nodes {
		require.EqualOneOf(t, i("foo", "dir"), node.Name)
	}

	nodes, err = h.List("/dir")
	require.NoError(t, err)
	require.Equal(t, 2, len(nodes))
	for _, node := range nodes {
		require.EqualOneOf(t, i("bar", "buzz"), node.Name)
	}

	// Make sure subsequent PutFile calls append
	oldSha := make([]byte, len(h.Fs["/foo"].Hash))
	copy(oldSha, h.Fs["/foo"].Hash)
	require.Equal(t, int64(1), h.Fs["/foo"].SubtreeSize)

	h.PutFile("/foo", br(`block{hash:"413e7"}`))
	require.NotEqual(t, oldSha, h.Fs["/foo"].Hash)
	require.Equal(t, int64(2), h.Fs["/foo"].SubtreeSize)
}

func TestPutDirBasic(t *testing.T) {
	h := HashTreeProto{}
	emptySha := sha256.Sum256([]byte{})

	// put a directory
	h.PutDir("/dir")
	require.Equal(t, emptySha[:], h.Fs["/dir"].Hash)
	require.Equal(t, []string(nil), h.Fs["/dir"].DirNode.Children)
	require.Equal(t, len(h.Fs), 2)

	// put a directory under another directory
	h.PutDir("/dir/foo")
	nodes, err := h.List("/dir")
	require.NoError(t, err)
	require.Equal(t, 1, len(nodes))
	require.NotEqual(t, emptySha[:], h.Fs["/dir"].Hash)
	require.NotEqual(t, []string{}, h.Fs["/dir"].DirNode.Children)

	// delete the directory
	h.DeleteFile("/dir/foo")
	nodes, err = h.List("/dir")
	require.NoError(t, err)
	require.Equal(t, 0, len(nodes))
	require.Equal(t, emptySha[:], h.Fs["/dir"].Hash)
	require.Equal(t, []string{}, h.Fs["/dir"].DirNode.Children)

	// Make sure that deleting a dir also deletes files under the dir
	h.PutFile("/dir/foo/bar", br(`block{hash:"20c27"}`))
	h.DeleteFile("/dir/foo")
	nodes, err = h.List("/dir")
	require.NoError(t, err)
	require.Equal(t, 0, len(nodes))
	require.Equal(t, emptySha[:], h.Fs["/dir"].Hash)
	require.Equal(t, []string{}, h.Fs["/dir"].DirNode.Children)
	require.Equal(t, len(h.Fs), 2)
}

func TestPutError(t *testing.T) {
	h := &HashTreeProto{}
	err := h.PutFile("/foo", br(`block{hash:"20c27"}`))
	require.NoError(t, err)

	// PutFile fails if the parent is a file, and h is unchanged
	h.requireOperationInvariant(t, func() {
		err := h.PutFile("/foo/bar", br(`block{hash:"8e02c"}`))
		require.YesError(t, err)
		node, err := h.Get("/foo/bar")
		require.YesError(t, err)
		require.Equal(t, PathNotFound, Code(err))
		require.Nil(t, node)
	})

	// PutDir fails if the parent is a file, and h is unchanged
	h.requireOperationInvariant(t, func() {
		err := h.PutDir("/foo/bar")
		require.YesError(t, err)
		node, err := h.Get("/foo/bar")
		require.YesError(t, err)
		require.Equal(t, PathNotFound, Code(err))
		require.Nil(t, node)
	})

	// Merge fails if src and dest disagree about whether a node is a file or
	// directory, and h is unchanged
	src := &HashTreeProto{}
	src.PutFile("/buzz", br(`block{hash:"9d432"}`))
	src.PutFile("/foo/bar", br(`block{hash:"ebc57"}`))
	h.requireOperationInvariant(t, func() {
		err := h.Merge([]HashTree{src})
		require.NotNil(t, err)
		require.Equal(t, PathConflict, Code(err))
	})
}

func TestDeleteDirError(t *testing.T) {
	// Put root dir
	h := &HashTreeProto{}
	h.PutDir("/")
	require.Equal(t, 1, len(h.Fs))

	err := h.DeleteFile("/does/not/exist")
	require.YesError(t, err)
	require.Equal(t, PathNotFound, Code(err))
	require.Equal(t, 1, len(h.Fs))
}

// Given a directory D, test that adding and then deleting a file/directory to
// D does not change D.
func TestAddDeleteReverts(t *testing.T) {
	h := HashTreeProto{}
	addDeleteFile := func() {
		h.PutFile("/dir/__NEW_FILE__", br(`block{hash:"8e02c"}`))
		h.DeleteFile("/dir/__NEW_FILE__")
	}
	addDeleteDir := func() {
		h.PutDir("/dir/__NEW_DIR__")
		h.DeleteFile("/dir/__NEW_DIR__")
	}
	addDeleteSubFile := func() {
		h.PutFile("/dir/__NEW_DIR__/__NEW_FILE__", br(`block{hash:"8e02c"}`))
		h.DeleteFile("/dir/__NEW_DIR__")
	}

	h.PutDir("/dir")
	h.requireOperationInvariant(t, addDeleteFile)
	h.requireOperationInvariant(t, addDeleteDir)
	h.requireOperationInvariant(t, addDeleteSubFile)
	// Add some files to make sure the test still passes when D already has files
	// in it.
	h.PutFile("/dir/foo", br(`block{hash:"ebc57"}`))
	h.PutFile("/dir/bar", br(`block{hash:"20c27"}`))
	h.requireOperationInvariant(t, addDeleteFile)
	h.requireOperationInvariant(t, addDeleteDir)
	h.requireOperationInvariant(t, addDeleteSubFile)
}

// Given a directory D, test that deleting and then adding a file/directory to
// D does not change D.
func TestDeleteAddReverts(t *testing.T) {
	h := HashTreeProto{}
	deleteAddFile := func() {
		h.DeleteFile("/dir/__NEW_FILE__")
		h.PutFile("/dir/__NEW_FILE__", br(`block{hash:"8e02c"}`))
	}
	deleteAddDir := func() {
		h.DeleteFile("/dir/__NEW_DIR__")
		h.PutDir("/dir/__NEW_DIR__")
	}
	deleteAddSubFile := func() {
		h.DeleteFile("/dir/__NEW_DIR__")
		h.PutFile("/dir/__NEW_DIR__/__NEW_FILE__", br(`block{hash:"8e02c"}`))
	}

	h.PutFile("/dir/__NEW_FILE__", br(`block{hash:"8e02c"}`))
	h.requireOperationInvariant(t, deleteAddFile)
	h.PutDir("/dir/__NEW_DIR__")
	h.requireOperationInvariant(t, deleteAddDir)
	h.PutFile("/dir/__NEW_DIR__/__NEW_FILE__", br(`block{hash:"8e02c"}`))
	h.requireOperationInvariant(t, deleteAddSubFile)

	// Add some files to make sure the test still passes when D already has files
	// in it.
	h = HashTreeProto{}
	h.PutFile("/dir/foo", br(`block{hash:"ebc57"}`))
	h.PutFile("/dir/bar", br(`block{hash:"20c27"}`))

	h.PutFile("/dir/__NEW_FILE__", br(`block{hash:"8e02c"}`))
	h.requireOperationInvariant(t, deleteAddFile)
	h.PutDir("/dir/__NEW_DIR__")
	h.requireOperationInvariant(t, deleteAddDir)
	h.PutFile("/dir/__NEW_DIR__/__NEW_FILE__", br(`block{hash:"8e02c"}`))
	h.requireOperationInvariant(t, deleteAddSubFile)
}

// The hash of a directory doesn't change no matter what order files are added
// to it.
func TestPutFileCommutative(t *testing.T) {
	h := HashTreeProto{}
	h2 := HashTreeProto{}
	comparePutFiles := func() {
		h.PutFile("/dir/__NEW_FILE_A__", br(`block{hash:"ebc57"}`))
		h.PutFile("/dir/__NEW_FILE_B__", br(`block{hash:"20c27"}`))

		// Get state of both /dir and /, to make sure changes are preserved upwards
		// through the file hierarchy
		dirNodePtr, err := h.Get("/dir")
		require.NoError(t, err)
		rootNodePtr, err := h.Get("/")
		require.NoError(t, err)

		h2.PutFile("/dir/__NEW_FILE_B__", br(`block{hash:"20c27"}`))
		h2.PutFile("/dir/__NEW_FILE_A__", br(`block{hash:"ebc57"}`))

		dirNodePtr2, err := h2.Get("/dir")
		require.NoError(t, err)
		rootNodePtr2, err := h2.Get("/")
		require.NoError(t, err)
		require.Equal(t, *dirNodePtr, *dirNodePtr2)
		require.Equal(t, *rootNodePtr, *rootNodePtr2)

		// Revert 'h' before the next call to deleteAddInspect()
		h.DeleteFile("/dir/__nEw_FiLe__")
	}

	comparePutFiles()
	// Add some files to make sure the test still passes when D already has files
	// in it.
	h, h2 = HashTreeProto{}, HashTreeProto{}
	h.PutFile("/dir/foo", br(`block{hash:"8e02c"}`))
	h2.PutFile("/dir/foo", br(`block{hash:"8e02c"}`))
	h.PutFile("/dir/bar", br(`block{hash:"9d432"}`))
	h2.PutFile("/dir/bar", br(`block{hash:"9d432"}`))
	comparePutFiles()
}

// Given a directory D, renaming (removing and re-adding under a different name)
// a file or directory under D changes the hash of D, even if the contents are
// identical.
func TestRenameChangesHash(t *testing.T) {
	h := HashTreeProto{}
	h.PutFile("/dir/foo", br(`block{hash:"ebc57"}`))

	dirPtr, err := h.Get("/dir")
	require.NoError(t, err)
	rootPtr, err := h.Get("/")
	require.NoError(t, err)
	dirPre, rootPre := proto.Clone(dirPtr).(*NodeProto), proto.Clone(rootPtr).(*NodeProto)

	// rename /dir/foo to /dir/bar
	h.DeleteFile("/dir/foo")
	h.PutFile("/dir/bar", br(`block{hash:"ebc57"}`))

	dirPtr, err = h.Get("/dir")
	require.NoError(t, err)
	rootPtr, err = h.Get("/")
	require.NoError(t, err)

	require.NotEqual(t, (*dirPre).Hash, (*dirPtr).Hash)
	require.NotEqual(t, (*rootPre).Hash, (*rootPtr).Hash)
	require.Equal(t, (*dirPre).SubtreeSize, (*dirPtr).SubtreeSize)
	require.Equal(t, (*rootPre).SubtreeSize, (*rootPtr).SubtreeSize)

	// rename /dir to /dir2
	h.DeleteFile("/dir")
	h.PutFile("/dir2/foo", br(`block{hash:"ebc57"}`))

	dirPtr, err = h.Get("/dir2")
	require.NoError(t, err)
	rootPtr, err = h.Get("/")
	require.NoError(t, err)

	require.Equal(t, dirPre.Hash, (*dirPtr).Hash) // dir == dir2
	require.NotEqual(t, rootPre.Hash, (*rootPtr).Hash)
	require.Equal(t, (*dirPre).SubtreeSize, (*dirPtr).SubtreeSize)
	require.Equal(t, (*rootPre).SubtreeSize, (*rootPtr).SubtreeSize)
}

// Given a directory D, rewriting (removing and re-adding a different file
// under the same name) a file or directory under D changes the hash of D, even
// if the contents are identical.
func TestRewriteChangesHash(t *testing.T) {
	h := HashTreeProto{}
	h.PutFile("/dir/foo", br(`block{hash:"ebc57"}`))

	dirPtr, err := h.Get("/dir")
	require.NoError(t, err)
	rootPtr, err := h.Get("/")
	require.NoError(t, err)
	dirPre, rootPre := proto.Clone(dirPtr).(*NodeProto), proto.Clone(rootPtr).(*NodeProto)

	h.DeleteFile("/dir/foo")
	h.PutFile("/dir/foo", br(`block{hash:"8e02c"}`))

	dirPtr, err = h.Get("/dir")
	require.NoError(t, err)
	rootPtr, err = h.Get("/")
	require.NoError(t, err)

	require.NotEqual(t, dirPre.Hash, (*dirPtr).Hash)
	require.NotEqual(t, rootPre.Hash, (*rootPtr).Hash)
	require.Equal(t, (*dirPre).SubtreeSize, (*dirPtr).SubtreeSize)
	require.Equal(t, (*rootPre).SubtreeSize, (*rootPtr).SubtreeSize)
}

func TestGlobFile(t *testing.T) {
	h := HashTreeProto{}
	h.PutFile("/foo", br(`block{hash:"20c27"}`))
	h.PutFile("/dir/bar", br(`block{hash:"ebc57"}`))
	h.PutFile("/dir/buzz", br(`block{hash:"8e02c"}`))

	// Patterns that match the whole repo ("/")
	for _, pattern := range []string{"", "/"} {
		nodes, err := h.Glob(pattern)
		require.NoError(t, err)
		require.Equal(t, 1, len(nodes))
		for _, node := range nodes {
			require.EqualOneOf(t, i(""), node.Name)
		}
	}

	// patterns that match top-level dirs/files
	for _, pattern := range []string{"*", "/*"} {
		nodes, err := h.Glob(pattern)
		require.NoError(t, err)
		require.Equal(t, 2, len(nodes))
		for _, node := range nodes {
			require.EqualOneOf(t, i("foo", "dir"), node.Name)
		}
	}

	// Patterns that match second-level dirs/files
	for _, pattern := range []string{"dir/*", "/dir/*", "*/*", "/*/*"} {
		nodes, err := h.Glob(pattern)
		require.NoError(t, err)
		require.Equal(t, 2, len(nodes))
		for _, node := range nodes {
			require.EqualOneOf(t, i("bar", "buzz"), node.Name)
		}
	}
}

func TestMerge(t *testing.T) {
	l, r := &HashTreeProto{}, &HashTreeProto{}
	l.PutFile("/foo-left", br(`block{hash:"20c27"}`))
	l.PutFile("/dir-left/bar-left", br(`block{hash:"ebc57"}`))
	l.PutFile("/dir-shared/buzz-left", br(`block{hash:"8e02c"}`))
	l.PutFile("/dir-shared/file-shared", br(`block{hash:"9d432"}`))
	r.PutFile("/foo-right", br(`block{hash:"20c27"}`))
	r.PutFile("/dir-right/bar-right", br(`block{hash:"ebc57"}`))
	r.PutFile("/dir-shared/buzz-right", br(`block{hash:"8e02c"}`))
	r.PutFile("/dir-shared/file-shared", br(`block{hash:"9d432"}`))

	expected := &HashTreeProto{}
	expected.PutFile("/foo-left", br(`block{hash:"20c27"}`))
	expected.PutFile("/dir-left/bar-left", br(`block{hash:"ebc57"}`))
	expected.PutFile("/dir-shared/buzz-left", br(`block{hash:"8e02c"}`))
	expected.PutFile("/dir-shared/file-shared", br(`block{hash:"9d432"}`))
	expected.PutFile("/foo-right", br(`block{hash:"20c27"}`))
	expected.PutFile("/dir-right/bar-right", br(`block{hash:"ebc57"}`))
	expected.PutFile("/dir-shared/buzz-right", br(`block{hash:"8e02c"}`))
	expected.PutFile("/dir-shared/file-shared", br(`block{hash:"9d432"}`))

	h := proto.Clone(l).(*HashTreeProto)
	h.Merge([]HashTree{r})
	expected.requireSame(t, h)

	h = proto.Clone(r).(*HashTreeProto)
	h.Merge([]HashTree{l})
	expected.requireSame(t, h)

	h = &HashTreeProto{}
	h.Merge([]HashTree{l, r})
	expected.requireSame(t, h)
}

// Test that Merge() works with empty hash trees
func TestMergeEmpty(t *testing.T) {
	expected, l, r := HashTreeProto{}, HashTreeProto{}, HashTreeProto{}
	expected.PutFile("/foo", br(`block{hash:"20c27"}`))
	expected.PutFile("/dir/bar", br(`block{hash:"ebc57"}`))

	b, _ := proto.Marshal(&expected)

	// Merge empty tree into full tree
	proto.Unmarshal(b, &l)
	l.Merge([]HashTree{&r})
	expected.requireSame(t, &l)

	// Merge full tree into empty tree
	r.Merge([]HashTree{&l})
	expected.requireSame(t, &r)
}

// Test that HashTree methods return the right error codes
func TestCode(t *testing.T) {
	require.Equal(t, OK, Code(nil))
	require.Equal(t, Unknown, Code(fmt.Errorf("external error")))

	h := HashTreeProto{}
	_, err := h.Get("/path")
	require.Equal(t, PathNotFound, Code(err))

	h.PutFile("/foo", br(`block{hash:"20c27"}`))
	err = h.PutFile("/foo/bar", br(`block{hash:"9d432"}`))
	require.Equal(t, PathConflict, Code(err))

	_, err = h.Glob("/*\\")
	require.Equal(t, MalformedGlob, Code(err))
}
