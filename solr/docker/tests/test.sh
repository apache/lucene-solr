#!/bin/bash

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

if (( $# == 0 )); then
  echo "Usage: ${[BASH_SOURCE[0]]} tag"
  exit
fi

if [[ -z "${IMAGE_NAME:-}" ]]; then
  IMAGE_NAME="apache/solr"
fi

tag=$1
if ! grep -q : <<<"$tag"; then
  tag="$IMAGE_NAME:$tag"
fi

test_dir=cases

echo "Running all tests for $tag"

for d in $(find "$test_dir" -mindepth 1 -maxdepth 1 -type d | sed -E -e 's/^\.\///'); do
  if [ -f "$d/test.sh" ]; then
    echo "Starting $d/test.sh $tag"
    (cd "$d"; ./test.sh "$tag")
    echo "Finished $d/test.sh $tag"
    echo
  fi
done
echo "Completed all tests for $tag"
