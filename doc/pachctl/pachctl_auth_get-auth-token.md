## pachctl auth get-auth-token

Get an auth token that authenticates the holder as "username"

### Synopsis


Get an auth token that authenticates the holder as "username"; this can only be called by cluster admins

```
pachctl auth get-auth-token <username>
```

### Options

```
  -q, --quiet   if set, only print the resulting token (if successful). This is useful for scripting, as the output can be piped to use-auth-token
```

### Options inherited from parent commands

```
  -v, --verbose   Output verbose logs
```

