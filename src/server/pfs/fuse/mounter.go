package fuse

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/pachyderm/pachyderm/src/client"
	pfsclient "github.com/pachyderm/pachyderm/src/client/pfs"
)

const (
	namePrefix = "pfs://"
	subtype    = "pfs"
)

type mounter struct {
	address   string
	apiClient *client.APIClient
}

func newMounter(address string, apiClient *client.APIClient) Mounter {
	return &mounter{
		address,
		apiClient,
	}
}

func (m *mounter) MountAndCreate(
	mountPoint string,
	shard *pfsclient.Shard,
	commitMounts []*CommitMount,
	ready chan bool,
	debug bool,
	allCommits bool,
	oneMount bool,
) error {
	if err := os.MkdirAll(mountPoint, 0777); err != nil {
		return err
	}
	return m.Mount(mountPoint, shard, commitMounts, ready, debug, allCommits, oneMount)
}

func (m *mounter) Mount(
	mountPoint string,
	shard *pfsclient.Shard,
	commitMounts []*CommitMount,
	ready chan bool,
	debug bool,
	allCommits bool,
	oneMount bool,
) (retErr error) {
	var once sync.Once
	defer once.Do(func() {
		if ready != nil {
			close(ready)
		}
	})
	name := namePrefix + m.address
	conn, err := fuse.Mount(
		mountPoint,
		fuse.FSName(name),
		fuse.VolumeName(name),
		fuse.Subtype(subtype),
		fuse.AllowOther(),
		fuse.WritebackCache(),
		fuse.MaxReadahead(1<<32-1),
	)
	if err != nil {
		return err
	}
	defer func() {
		if err := conn.Close(); err != nil && retErr == nil {
			retErr = err
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	go func() {
		<-sigChan
		m.Unmount(mountPoint)
	}()

	once.Do(func() {
		if ready != nil {
			close(ready)
		}
	})
	config := &fs.Config{}
	if debug {
		config.Debug = func(msg interface{}) { log.Printf("%+v", msg) }
	}
	var filesystem fs.FS
	if oneMount {
		if len(commitMounts) != 1 {
			return fmt.Errorf("expect 1 CommitMount, got %d", len(commitMounts))
		}
		filesystem = newRepoFilesystem(m.apiClient, shard, commitMounts[0], allCommits)
	} else {
		filesystem = newFilesystem(m.apiClient, shard, commitMounts, allCommits)
	}
	if err := fs.New(conn, config).Serve(filesystem); err != nil {
		return err
	}
	<-conn.Ready
	return conn.MountError
}

func (m *mounter) Unmount(mountPoint string) error {
	return fuse.Unmount(mountPoint)
}
