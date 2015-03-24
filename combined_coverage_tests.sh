#!/bin/bash

set -e

echo "mode: count" > coverage.out
for Package in $(go list ./...)
do
    go test -v -race -covermode=count -coverprofile=profile.out "$Package"
    if [[ -f profile.out ]]
    then
	if [[ "$(cat profile.out)" != "mode: count" ]]
	then
	    grep -v "mode: count" profile.out >> coverage.out
	fi
    fi
done

if [[ "$COVERALLS_TOKEN" != "" ]]
then
    go get -v github.com/mattn/goveralls
    goveralls -coverprofile=coverage.out -service=drone.io -repotoken $COVERALLS_TOKEN
fi
