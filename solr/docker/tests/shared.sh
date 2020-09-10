#!/bin/bash
#
# Shared functions for testing

function container_cleanup {
  local container_name
  container_name=$1
  previous=$(docker inspect "$container_name" --format '{{.ID}}' 2>/dev/null || true)
  if [[ -n $previous ]]; then
    container_status=$(docker inspect --format='{{.State.Status}}' "$previous" 2>/dev/null)
    if [[ $container_status == 'running' ]]; then
      echo "killing $previous"
      docker kill "$previous" 2>/dev/null || true
      sleep 2
    fi
    echo "removing $previous"
    docker rm "$previous" 2>/dev/null || true
  fi
}

function wait_for_container_and_solr {
  local container_name
  container_name=$1
  wait_for_server_started "$container_name" 0

  printf '\nWaiting for Solr...\n'
  local status
  status=$(docker exec "$container_name" /opt/docker-solr/scripts/wait-for-solr.sh --max-attempts 60 --wait-seconds 1)
#  echo "Got status from Solr: $status"
  if ! grep -E -i -q 'Solr is running' <<<"$status"; then
    echo "Solr did not start"
    container_cleanup "$container_name"
    exit 1
  else
    echo "Solr is running"
  fi
  sleep 4
}

function wait_for_server_started {
  local container_name
  container_name=$1
  local sleep_time
  sleep_time=5
  if [ -n "${2:-}" ]; then
    sleep_time=$2
  fi
  echo "Waiting for container start: $container_name"
  local TIMEOUT_SECONDS
  TIMEOUT_SECONDS=$(( 5 * 60 ))
  local started
  started=$(date +%s)
  local log
  log="${BUILD_DIR}/${container_name}.log"
  while true; do
    docker logs "$container_name" > "${log}" 2>&1
    if grep -E -q '(o\.e\.j\.s\.Server Started|Started SocketConnector)' "${log}" ; then
      break
    fi

    local container_status
    container_status=$(docker inspect --format='{{.State.Status}}' "$container_name")
    if [[ $container_status == 'exited' ]]; then
      echo "container exited"
      exit 1
    fi

    if (( $(date +%s) > started + TIMEOUT_SECONDS )); then
      echo "giving up after $TIMEOUT_SECONDS seconds"
      exit 1
    fi
    printf '.'
    sleep 2
  done
  echo "Server started"
  rm "${log}"
  sleep "$sleep_time"
}

function prepare_dir_to_mount {
  local userid
  userid=8983
  local folder
  folder="${BUILD_DIR}/myvarsolr"
  if [ -n "$1" ]; then
    userid=$1
  fi
  if [ -n "$2" ]; then
    folder=$2
  fi
  rm -fr "$folder" >/dev/null 2>&1
  mkdir "$folder"
  #echo "***** Created varsolr folder $BUILD_DIR / $folder"

  # The /var/solr mountpoint is owned by solr, so when we bind mount there our directory,
  # owned by the current user in the host, will show as owned by solr, and our attempts
  # to write to it as the user will fail. To deal with that, set the ACL to allow that.
  # If you can't use setfacl (eg on macOS), you'll have to chown the directory to 8983, or apply world
  # write permissions.
  if command -v setfacl &> /dev/null; then
    setfacl -m "u:$userid:rwx" "$folder"
  fi
}


# Shared setup

if (( $# == 0 )); then
  echo "Usage: ${TEST_DIR}/test.sh <tag>"
  exit
fi
tag=$1

if [[ -n "${DEBUG:-}" ]]; then
  set -x
fi

TEST_NAME="$(basename -- "${TEST_DIR}")"

# Create build directory if it hasn't been provided already
if [[ -z ${BUILD_DIR:-} ]]; then
  BASE_DIR="$(dirname -- "${BASH_SOURCE-$0}")"
  BASE_DIR="$(cd "${BASE_DIR}/.." && pwd)"
  BUILD_DIR="${BASE_DIR}/build/tmp/tests/${TEST_NAME}"
fi
mkdir -p "${BUILD_DIR}"

echo "Test $TEST_DIR $tag"
echo "Test logs and build files can be found at: ${BUILD_DIR}"
container_name="test-$(echo "${TEST_NAME}" | tr ':/-' '_')-$(echo "${tag}" | tr ':/-' '_')"

echo "Cleaning up left-over containers from previous runs"
container_cleanup "${container_name}"