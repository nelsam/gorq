#!/bin/bash
set -e

# Update to the most recent version of Go
go get gopkg.in/niemeyer/godeb.v1/cmd/godeb
godeb install 1.5.3
