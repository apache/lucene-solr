#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

script_dir=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

. "${script_dir}/setup-script-for-test.sh" || { echo "Could not source setup-script-for-test.sh"; exit 1; }

skip_build_image=""

OPTIND=1  # Reset in case getopts has been used previously in the shell.

while getopts ":c:s" opt; do
    case "$opt" in
    c)
      host_count=$OPTARG
      ;;
    s)
      skip_build_image="true"
      ;;
    h)
      exit 0
      ;;
    *)  
      echo -e "\n-----> Invalid arg: $OPTARG  If it is a valid option, does it take an argument?"
      usage
      exit 1
      ;;
    esac
done

shift $((OPTIND-1))

[ "$1" = "--" ] && shift

if [ -z ${CONTAINER_NAME} ]; then
  export CONTAINER_NAME="lucenesolr"
fi

bash ${script_dir}/stop.sh

if [ ! "${skip_build_image}" = "true" ]; then

  echo "docker build -f "${script_dir}/Dockerfile" -t ${CONTAINER_NAME} ."
  docker build -f "${script_dir}/Dockerfile" -t ${CONTAINER_NAME} . || { "docker build failed!"; exit 1; }
fi


echo "Starting the container ... binding project from ${script_dir}/../../.."

# we don't currently use :cached or :delegated for osx perf becuase of issues getting things to work on linux and osx - ideally we use linux because osx will likely be very slow
# this will also likley download a lot on first run

docker run -itd --user ${UID} --name=${CONTAINER_NAME} -v "${script_dir}/../../../../":/home/lucene/project -v ~/.gradle/caches/modules-2/files-2.1:/root/.gradle/caches/modules-2/files-2.1 -h ${CONTAINER_NAME} ${CONTAINER_NAME} || { exit 1; }

