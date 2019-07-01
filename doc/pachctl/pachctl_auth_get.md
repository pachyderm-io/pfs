## pachctl auth get

Get the ACL for 'repo' or the access that 'username' has to 'repo'

### Synopsis


Get the ACL for 'repo' or the access that 'username' has to 'repo'. For example, 'pachctl auth get github-alice private-data' prints "reader", "writer", "owner", or "none", depending on the privileges that "github-alice" has in "repo". Currently all Pachyderm authentication uses GitHub OAuth, so 'username' must be a GitHub username

```
pachctl auth get [<username>] <repo>
```

### Options inherited from parent commands

```
  -v, --verbose   Output verbose logs
```

