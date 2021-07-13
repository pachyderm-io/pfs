#!/bin/sh
set -e
bash --version
cd /pfs/source
test -f requirements.sh && bash requirements.sh
cp /app/run.sh /pfs/out/run.sh
