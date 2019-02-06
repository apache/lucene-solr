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


# Invoke Yetus locally to validate a patch against Lucene/Solr, and 
# (optionally) post a validation report comment on the passed-in JIRA issue
# from which the patch was downloaded.
#
# NB 1: The Lucene/Solr Yetus personality currently performs the equivalent
# of "ant precommit" and "ant test" in modified modules; instead of using
# this script to test your changes, please consider invoking those targets
# directly.
#
# NB 2: The Jenkins job "PreCommit-Admin" automatically detects new patches
# posted to LUCENE and SOLR JIRA issues that are in the "Patch Available"
# state, and then queues the appropriate "PreCommit-LUCENE-Build" or 
# "PreCommit-SOLR-Build" job pointing to the JIRA hosting the new patch.
# Those jobs perform the same checks as this script, and like this script,
# will post a comment on the JIRA issue.  As a result, manual invocation
# (e.g. via this script) should ordinarily not be necessary.
# 
# Environment variable ${YETUS_HOME} must point to the separately installed
# Yetus home directory, e.g. the "rel/0.7.0" tag checked out from a local
# Yetus Git repository. 
#
# Environment variable ${PROJECT_DIR} must point to a local Lucene/Solr git
# workspace dir.
#
# The sole cmdline param can be a JIRA issue, a local patch file, 
# or a URL to a patch file.
#
# If the cmdline param is a JIRA issue, the patch to download and validate
# will be the most recently uploaded patch on the issue.  See the patch
# naming schema that Yetus recognizes: 
# https://yetus.apache.org/documentation/in-progress/precommit-patchnames/ 
#
# If the cmdline param is a JIRA issue and you provide JIRA user/password via
# environment variables ${JIRA_USER} and ${JIRA_PASSWORD}, a patch validation
# report will be posted as a comment on the JIRA issue.

help () {
  echo "Usage 1: [ JIRA_USER=xxx JIRA_PASSWORD=yyy ] PROJECT_DIR=/path/to/lucene-solr YETUS_HOME=/path/to/yetus $0 SOLR-12345"
  echo "Usage 2: [ JIRA_USER=xxx JIRA_PASSWORD=yyy ] PROJECT_DIR=/path/to/lucene-solr YETUS_HOME=/path/to/yetus $0 LUCENE-12345"
  echo "Usage 3: PROJECT_DIR=/path/to/lucene-solr YETUS_HOME=/path/to/yetus $0 ../local.patch"
  echo "Usage 4: PROJECT_DIR=/path/to/lucene-solr YETUS_HOME=/path/to/yetus $0 http://example.com/remote.patch"
}

if [[ -z "${PROJECT_DIR}" || -z "${YETUS_HOME}" || -z "${1}" || "${1}" =~ ^-(-?h(elp)?|\?)$ ]] ; then
  help
  exit 1
fi

PATCH_REF=${1}
TEST_PATCH_BIN="${YETUS_HOME}/precommit/test-patch.sh"
SCRIPT_DIR="$( cd "$( dirname "${0}" )" && pwd )"
declare -a YETUS_ARGS

if [[ ${PATCH_REF} =~ ^(LUCENE|SOLR)- ]]; then
  JIRA_PROJECT=${BASH_REMATCH[0]}
  YETUS_ARGS+=("--project=${JIRA_PROJECT}")
  
  if [[ -n "${JIRA_USER}" ]] && [[ -n "${JIRA_PASSWORD}" ]] ; then 
    YETUS_ARGS+=("--jira-user=${JIRA_USER}")
    YETUS_ARGS+=("--jira-password=${JIRA_PASSWORD}")
    YETUS_ARGS+=("--bugcomments=jira")
  fi
fi

YETUS_ARGS+=("--basedir=${PROJECT_DIR}")
YETUS_ARGS+=("--personality=${SCRIPT_DIR}/lucene-solr-yetus-personality.sh")
YETUS_ARGS+=("--skip-dirs=dev-tools")
YETUS_ARGS+=("--resetrepo")
YETUS_ARGS+=("--run-tests")
YETUS_ARGS+=("--debug")
YETUS_ARGS+=("--robot")
YETUS_ARGS+=("${PATCH_REF}")

/bin/bash ${TEST_PATCH_BIN} "${YETUS_ARGS[@]}"
