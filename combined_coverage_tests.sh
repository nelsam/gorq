#!/bin/bash

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
