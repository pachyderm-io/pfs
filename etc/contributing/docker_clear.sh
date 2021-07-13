#!/bin/sh

docker kill $(docker container ls -q)
