// +build !windows

package cmds

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"strings"
	"syscall"

	"github.com/pachyderm/pachyderm/src/client"
	"github.com/pachyderm/pachyderm/src/server/pfs/fuse"
	"github.com/pachyderm/pachyderm/src/server/pkg/cmdutil"
	"github.com/spf13/cobra"
)

func loadMountCommands(address string, metrics bool) []*cobra.Command {
	var debug bool
	var allCommits bool
	mount := &cobra.Command{
		Use:   "mount path/to/mount/point",
		Short: "Mount pfs locally. This command blocks.",
		Long:  "Mount pfs locally. This command blocks.",
		Run: cmdutil.RunFixedArgs(1, func(args []string) error {
			client, err := client.NewMetricsClientFromAddress(address, metrics, "fuse")
			if err != nil {
				return err
			}
			go func() { client.KeepConnected(nil) }()
			mounter := fuse.NewMounter(address, client)
			mountPoint := args[0]
			ready := make(chan bool)
			go func() {
				<-ready
				fmt.Println("Filesystem mounted, CTRL-C to exit.")
			}()
			err = mounter.Mount(mountPoint, nil, ready, debug, false)
			if err != nil {
				return err
			}
			return nil
		}),
	}
	mount.Flags().BoolVarP(&debug, "debug", "d", false, "Turn on debug messages.")
	mount.Flags().BoolVarP(&allCommits, "all-commits", "a", false, "Show archived and cancelled commits.")

	var all bool
	unmount := &cobra.Command{
		Use:   "unmount path/to/mount/point",
		Short: "Unmount pfs.",
		Long:  "Unmount pfs.",
		Run: cmdutil.RunBoundedArgs(0, 1, func(args []string) error {
			if len(args) == 1 {
				return syscall.Unmount(args[0], 0)
			}

			if all {
				stdin := strings.NewReader(`
	mount | grep pfs:// | cut -f 3 -d " "
	`)
				var stdout bytes.Buffer
				if err := cmdutil.RunIO(cmdutil.IO{
					Stdin:  stdin,
					Stdout: &stdout,
					Stderr: os.Stderr,
				}, "sh"); err != nil {
					return err
				}
				scanner := bufio.NewScanner(&stdout)
				var mounts []string
				for scanner.Scan() {
					mounts = append(mounts, scanner.Text())
				}
				if len(mounts) == 0 {
					fmt.Println("No mounts found.")
					return nil
				}
				fmt.Printf("Unmount the following filesystems? yN\n")
				for _, mount := range mounts {
					fmt.Printf("%s\n", mount)
				}
				r := bufio.NewReader(os.Stdin)
				bytes, err := r.ReadBytes('\n')
				if err != nil {
					return err
				}
				if bytes[0] == 'y' || bytes[0] == 'Y' {
					for _, mount := range mounts {
						if err := syscall.Unmount(mount, 0); err != nil {
							return err
						}
					}
				}
			}
			return nil
		}),
	}
	unmount.Flags().BoolVarP(&all, "all", "a", false, "unmount all pfs mounts")

	return []*cobra.Command{
		mount, unmount,
	}
}
