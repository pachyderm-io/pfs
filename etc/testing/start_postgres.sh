#!/bin/sh

if ! docker ps | grep -q postgres_test
then
    echo "starting postgres..."
    docker run -d --rm \
    --name postgres_test \
    -e POSTGRES_HOST_AUTH_METHOD=trust \
    -p 5432:5432 \
    postgres:13.0-alpine
else
    echo "postgres already started"
fi
