#!/bin/bash
# This script builds protos using the pachyderm_proto image.
# Before generating any code, it verifies that no backward-incompatible changes have been introduced 
# to the .proto definition files.

set -e

protolock status

find src -regex ".*\.proto" -print0 \
| xargs -0 tar cf - \
| docker run -i pachyderm_proto \
| tar xf -
