package main

import (
	"github.com/pachyderm/pachyderm/src/server/cmd/pachctl/cmd"
	"github.com/pachyderm/pachyderm/src/server/pkg/cmdutil"

	"github.com/spf13/cobra/doc"
)

type appEnv struct{}

func main() {
	cmdutil.Main(do, &appEnv{})
}

func do(appEnvObj interface{}) error {
	// passing an empty address but that's fine because we're not going to
	// execute the command but print docs with it
	rootCmd, err := cmd.PachctlCmd("")
	if err != nil {
		return err
	}
	return doc.GenMarkdownTree(rootCmd, "./doc/pachctl/")
}
