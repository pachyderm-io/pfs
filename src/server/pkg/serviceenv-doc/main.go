package main

 import (
     "os"

     "github.com/pachyderm/pachyderm/src/server/pkg/serviceenv"
     "github.com/pachyderm/pachyderm/src/server/pkg/cmdutil"

     "github.com/spf13/cobra/doc"
 )

 type appEnv struct{}

 func main() {
     cmdutil.Main(do, &appEnv{})
 }

 func do(appEnvObj interface{}) error {
     // Set 'os.Args[0]' so that examples use the expected command name
//     os.Args[0] = ""

     rootCmd := cmd.PachctlCmd()

     return doc.GenMarkdownTree(rootCmd, "./doc/docs/deploy-manage/")
 }
