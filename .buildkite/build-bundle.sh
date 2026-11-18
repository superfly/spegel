#!/bin/bash
# Build bundle.tar.gz from this checkout: bin/spegel, a static linux/amd64
# binary, and VERSION, which names the commit it was built from.
#
# The compiler runs in a container as the calling user, so the checkout is the
# only thing the build needs on the machine and nothing in it ends up owned by
# root.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

GO_IMAGE=golang:1.24

rm -rf bundle bundle.tar.gz
mkdir -p bundle/bin

docker run --rm \
  --user "$(id -u):$(id -g)" \
  --volume "$PWD:/src" --workdir /src \
  --env HOME=/tmp --env GOCACHE=/tmp/gocache --env GOMODCACHE=/tmp/gomodcache \
  --env CGO_ENABLED=0 --env GOOS=linux --env GOARCH=amd64 --env GOFLAGS=-buildvcs=false \
  "$GO_IMAGE" \
  go build -trimpath -o bundle/bin/spegel .

git describe --tags --always > bundle/VERSION
test -s bundle/VERSION

tar --sort=name --owner=0 --group=0 --numeric-owner --mode=go-w \
  -C bundle -czf bundle.tar.gz bin VERSION
