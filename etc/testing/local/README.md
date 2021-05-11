## Local Mode Testing

This directory has scripts to support "local mode" development on OS X. In this mode we run all of Pachyderm's dependencies (etcd, postgres, worker pods) in minikube, and run pachd itself in-memory as part of integration tests. 

The advantage of this approach is that the pachd container doesn't need to be rebuilt and deployed for every code change. We can also run multiple tests in parallel because these in-memory pachds have dedicated etcd prefixes, postgres databases, etc. 

Worker pods started by pachd will run in minikube using the most recently-built pachd sidecar and worker images.

### Setup

Run `etc/testing/local/start-minikube.sh` to launch minikube with the virtualbox driver. The virtualbox driver is used so we can mount a shared data directory from the host into the k8s worker pods.

Run `etc/testing/local/setup.sh` to deploy dependencies to minikube and build the worker docker images. You can re-run this script to re-build the worker images that run in minikube, if your code changes affect them.

### Testing

Run your tests with `etc/testing/local/run.sh <go test command>`. The run script sets environment variables that point the tests to k8s, etcd, postgres, etc. 

Logs from the test runs are stored in `/tmp/pachd/<test run ID>/pachd.log`.

### TODO

- These scripts might also work on Linux, but they haven't been tested (we might want the none driver instead)
- Some logs are still printed to stdout
- Some goroutines aren't cancelled when the in-memory server is stopped
- The postgres database isn't torn down when a test ends because workers may still be connected
- Only tests in `src/server/auth/server/testing` have been rewritten to support local testing mode (so far)
- Tests that rely on a web server don't work (yet)
- Tests that rely on running `pachctl` on the CLI don't work (yet)
