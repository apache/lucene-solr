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

command -v docker >/dev/null 2>&1 || { echo "docker must be installed to run this test"; exit 1; }

skip_rm_image=""

OPTIND=1  # Reset in case getopts has been used previously in the shell.

while getopts ":c:s" opt; do
    case "$opt" in
    c)
      host_count=$OPTARG
      ;;
    s)
      skip_rm_image="true"
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

echo "Stopping containers ..."

if [ -z ${CONTAINER_NAME} ]; then
  export CONTAINER_NAME="lucenesolr"
fi


docker stop ${CONTAINER_NAME} || { echo "No container that must stop"; } &

wait

docker rm ${CONTAINER_NAME} || { echo "No container that must be removed"; } &

wait

if [ ! "${skip_rm_image}" = "true" ]; then
  # we don't clean up the image (see clean-all-docker.sh for that), but make sure any dangling images are removed
  images=$(docker images --quiet --filter="dangling=true")
  if [ ! -z "${images}" ]; then
    echo ${images} | xargs docker rmi -f
  fi 
fi

wait
