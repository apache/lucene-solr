#!/bin/bash
#
set -euo pipefail

TEST_DIR="${TEST_DIR:-$(dirname -- "${BASH_SOURCE[0]}")}"
source "${TEST_DIR}/../../shared.sh"

echo "Running $container_name"
if docker run --name "$container_name" "$tag" solr-create -c 'bad/core?name:here'; then
  echo "Bad core creation did not return a failure"
  exit 1
fi

container_cleanup "$container_name"

echo "Test $TEST_NAME $tag succeeded"
