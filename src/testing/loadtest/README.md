## Loadtest

This directory contains various Pachyderm load tests

# k6

The tests in the k6 directory make gRPC requests to a pachd running on port 30650. They require k6,
which you can install with `go get github.com/loadimpact/k6` or by visiting https://k6.io

You might also want to `npm install -g @types/k6` to provide editor support for k6's built-in types.
