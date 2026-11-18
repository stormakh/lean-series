#!/bin/bash
# runs the project Go toolchain offline (module cache only)
R=$HOME/code/prometheus-rs
export PATH=$R/tools/go/bin:$PATH GOPATH=$R/tools/gopath GOCACHE=$R/tools/gocache GOTMPDIR=$R/tools/gotmp GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off GOFLAGS=-mod=mod
mkdir -p "$GOTMPDIR"
exec go "$@"
