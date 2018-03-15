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
#
# ----------------------------------------------------------------------------------
#
# This script is a copy of the one used by ASF Jenkins's PreCommit-LUCENE-Build job.
#
# See the script "test-patch.sh" in this directory for the script to use for
# local manual patch validation.
#
# For other examples of scripts used to invoke Yetus, see the configuration on the
# PreCommit jobs on ASF Jenkins: https://builds.apache.org/view/PreCommit+Builds/
#
# ------------>8-------------------------->8-------------------------->8------------
 
#!/usr/bin/env bash

# This is a modified copy of the script from Jenkins project "PreCommit-HADOOP-Build"

YETUSDIR=${WORKSPACE}/yetus
TESTPATCHBIN=${YETUSDIR}/precommit/test-patch.sh
ARTIFACTS_SUBDIR=out
ARTIFACTS=${WORKSPACE}/${ARTIFACTS_SUBDIR}
BASEDIR=${WORKSPACE}/sourcedir
rm -rf "${ARTIFACTS}"
mkdir -p "${ARTIFACTS}"

PIDMAX=10000 # Arbitrary limit; may need to revisit

YETUS_RELEASE=0.7.0
YETUS_TARBALL="yetus-${YETUS_RELEASE}.tar.gz"
echo "Downloading Yetus ${YETUS_RELEASE}"
curl -L "https://api.github.com/repos/apache/yetus/tarball/rel/${YETUS_RELEASE}" -o "${YETUS_TARBALL}"
rm -rf "${YETUSDIR}"
mkdir -p "${YETUSDIR}"
gunzip -c "${YETUS_TARBALL}" | tar xpf - -C "${YETUSDIR}" --strip-components 1

YETUS_ARGS+=("--project=LUCENE")
YETUS_ARGS+=("--basedir=${BASEDIR}")
YETUS_ARGS+=("--patch-dir=${ARTIFACTS}")
YETUS_ARGS+=("--build-url-artifacts=artifact/${ARTIFACTS_SUBDIR}")
YETUS_ARGS+=("--personality=${BASEDIR}/dev-tools/test-patch/lucene-solr-yetus-personality.sh")
YETUS_ARGS+=("--jira-user=lucenesolrqa")
YETUS_ARGS+=("--jira-password=$JIRA_PASSWORD")
YETUS_ARGS+=("--brief-report-file=${ARTIFACTS}/email-report.txt")
YETUS_ARGS+=("--console-report-file=${ARTIFACTS}/console-report.txt")
YETUS_ARGS+=("--html-report-file=${ARTIFACTS}/console-report.html")
YETUS_ARGS+=("--proclimit=${PIDMAX}")
YETUS_ARGS+=("--console-urls")
YETUS_ARGS+=("--debug")
YETUS_ARGS+=("--skip-dirs=dev-tools")
YETUS_ARGS+=("--bugcomments=jira")
YETUS_ARGS+=("--resetrepo")
YETUS_ARGS+=("--run-tests")
YETUS_ARGS+=("--contrib-guide=https://wiki.apache.org/lucene-java/HowToContribute#Contributing_your_work")
YETUS_ARGS+=("--jenkins")
YETUS_ARGS+=("LUCENE-${ISSUE_NUM}")

/bin/bash ${TESTPATCHBIN} "${YETUS_ARGS[@]}"
