## pachctl list job

Return info about jobs.

### Synopsis


Return info about jobs.

```
pachctl list job
```

### Examples

```
```sh

# Return all jobs
$ pachctl list job

# Return all jobs from the most recent version of pipeline "foo"
$ pachctl list job -p foo

# Return all jobs from all versions of pipeline "foo"
$ pachctl list job -p foo --history all

# Return all jobs whose input commits include foo@XXX and bar@YYY
$ pachctl list job -i foo@XXX -i bar@YYY

# Return all jobs in pipeline foo and whose input commits include bar@YYY
$ pachctl list job -p foo -i bar@YYY
```
```

### Options

```
      --full-timestamps   Return absolute timestamps (as opposed to the default, relative timestamps).
      --history string    Return jobs from historical versions of pipelines. (default "none")
  -i, --input strings     List jobs with a specific set of input commits. format: <repo>@<branch-or-commit>
  -o, --output string     List jobs with a specific output commit. format: <repo>@<branch-or-commit>
  -p, --pipeline string   Limit to jobs made by pipeline.
      --raw               disable pretty printing, print raw json
```

### Options inherited from parent commands

```
      --no-metrics           Don't report user metrics for this command
      --no-port-forwarding   Disable implicit port forwarding
  -v, --verbose              Output verbose logs
```

