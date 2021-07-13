#!/bin/sh

set -ex

openai-ft \
-t /pfs/data/train.jsonl \
--val /pfs/data/test.jsonl \
-e $ENGINE \
-m ada \
--batch-size 4 \
--val-batch-size 4 \
--num-epochs 10000
