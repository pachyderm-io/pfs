#!/bin/sh

set -ex

PROMPT=$(ls /pfs/prompts)
openai -v api completions.create \
-e $ENGINE \
-p "$(cat /pfs/prompts/$PROMPT)" \
-M 10 >/pfs/out/$PROMPT
