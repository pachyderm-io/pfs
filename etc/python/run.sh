#!/bin/bash
set -e

# will extract from stdin and to ./src
tar xf /dev/stdin

# TODO programmatically set VERSION
VERSION=2  # the MajorVersion of pachyderm
GOGO_VERSION='v1.3.1'
OUTDIR=python_pachyderm/proto/v${VERSION}

mkdir -p ${OUTDIR} ${OUTDIR}/gogoproto

mv src/* ${OUTDIR} && rmdir src

curl https://raw.githubusercontent.com/gogo/protobuf/${GOGO_VERSION}/gogoproto/gogo.proto > ${OUTDIR}/gogoproto/gogo.proto

# fix imports to be relative to OUTDIR
for i in $(find ${OUTDIR} -name "*.proto"); do
    perl -pi -e "s/import \"((?!google).*)\"/import \"python_pachyderm\/proto\/v${VERSION}\/\$1\"/" ${i}
done

find ${OUTDIR} -name '*.proto' | xargs python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=.
find ${OUTDIR} -name '*.proto' | xargs rm

tar cf - ${OUTDIR}
