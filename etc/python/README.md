# python-pachyderm-proto

Generates protobuf and gRPC code for the [Python client](https://github.com/pachyderm/python-pachyderm).

To generate the package from project root directory:

```
make python-proto
```

How to use:

```python
from python_pachyderm.proto.v2.pfs import pfs_pb2
from python_pachyderm.proto.v2.pfs import pfs_pb2_grpc
```
