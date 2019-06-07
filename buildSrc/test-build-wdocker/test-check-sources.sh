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

command -v docker >/dev/null 2>&1 || { echo "docker must be installed to run this test"; exit 1; }

# make any non 0 exit fail script
. "test-build-wdocker/setup-script-for-test.sh" || { echo "Could not source setup-script-for-test.sh"; exit 1; }

OPTIND=1  # Reset in case getopts has been used previously in the shell.

while getopts ":" opt; do
    case "$opt" in
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

exec() {
  docker exec --user ${UID} $2 -t ${CONTAINER_NAME} bash -c "$1"
}

set -x

# NOTE: we don't clean right now, as it would wipe out buildSrc/build on us for the host

# first check that checkSourcePatterns passes
cmd="cd /home/lucene/project;./gradlew checkSourcePatterns"
exec "${cmd}" "${exec_args}" || { exit 1; }

# create an xml file with no license in lucene
cmd="ls /home/lucene/project;echo \\"\t\\" >> /home/lucene/project/solr/contrib/clustering/src/java/org/tab_file.xml"
exec "${cmd}" "${exec_args}" || { exit 1; }

# test that checkSourcePatterns fails on our test file
cmd="cd /home/lucene/project;./gradlew checkSourcePatterns"
if [ exec "${cmd}" "${exec_args}" ]; then
  echocheckSourcePatterns should fail!
  exit 1
fi

# clean test file
cmd="rm /home/lucene/project/solr/contrib/clustering/src/java/org/tab_file.xml"
exec "${cmd}" "${exec_args}" || { exit 1; }
