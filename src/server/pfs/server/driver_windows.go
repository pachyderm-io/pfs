// +build windows

package server

import (
	"io"

	"github.com/pachyderm/pachyderm/src/client"
	"github.com/pachyderm/pachyderm/src/client/pfs"
)

// downloadTree implementation for windows, which doesn't support unlinking a
// file while it's still open, so here we just pass-through the object reader
// (which doesn't use an intermediary buffer, so is less performant).
func (d *driver) downloadTree(pachClient *client.APIClient, object *pfs.Object, prefix string) (io.ReadCloser, error) {
	info, err := pachClient.InspectObject(object.Hash)
	if err != nil {
		return nil, err
	}
	path, err := BlockPathFromEnv(info.BlockRef.Block)
	if err != nil {
		return nil, err
	}
	offset, size, err := getTreeRange(pachClient.Ctx(), d.objClient, path, prefix)
	if err != nil {
		return nil, err
	}
	return d.objClient.Reader(pachClient.Ctx(), path, offset, size)
}
