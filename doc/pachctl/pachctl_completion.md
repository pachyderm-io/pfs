## pachctl completion

Print or install the bash completion code.

### Synopsis


Print or install the bash completion code. This should be placed as the file `pachctl` in the bash completion directory (by default this is `/etc/bash_completion.d`. If bash-completion was installed via homebrew, this would be `$(brew --prefix)/etc/bash_completion.d`.)

```
pachctl completion
```

### Options

```
      --install                        Install the completion.
      --path /etc/bash_completion.d/   Path to install the completion to. This will default to /etc/bash_completion.d/ if unspecified. (default "/etc/bash_completion.d/pachctl")
```

### Options inherited from parent commands

```
  -v, --verbose   Output verbose logs
```

