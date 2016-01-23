#!/bin/bash

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

####

# Convenience script to generates the SVN command line for you to run in order to 
# remove old (archived) versions of the solr ref guide from the dist repo, and 
# (if necessary) clean up any old RC files.
#
# See: https://cwiki.apache.org/confluence/display/solr/Internal+-+How+To+Publish+This+Documentation

if [ $# -ne 1 ] ; then
    echo "Usage: $0 <X.Y>"
    echo ""
    echo "Where X.Y is the version of the ref guide that has recently been"
    echo "published, and all previous versions have already been archived"
    echo ""
    echo "Example: "
    echo "    $0 4.5"
    exit 1
fi

VERSION="$1"
DIST_PREFIX="apache-solr-ref-guide-$VERSION."

DIST_DIR_URL="https://dist.apache.org/repos/dist/release/lucene/solr/ref-guide"
RC_DIR_URL="https://dist.apache.org/repos/dist/dev/lucene/solr/ref-guide"

RM_OLD_CMD="svn rm -m 'removing archived ref guide files prior to $VERSION' "
RM_RC_CMD="svn rm -m 'cleaning up old RCs now that $VERSION has been released' "

NEW_COUNT=0
OLD_COUNT=0
RC_COUNT=0

for FILE in `svn list $DIST_DIR_URL`; do
  if [[ $FILE == $DIST_PREFIX* ]] ; then
    NEW_COUNT=`expr $NEW_COUNT + 1`
    continue # one of our just published files
  fi
  OLD_COUNT=`expr $OLD_COUNT + 1`
  RM_OLD_CMD="$RM_OLD_CMD $DIST_DIR_URL/$FILE"
done

if [ $NEW_COUNT -eq 0 ] ; then
   echo "FAIL: Unable to find any current (ie: $DIST_PREFIX*) release files in $DIST_DIR_URL"
   exit -1;
fi

if [ $OLD_COUNT -eq 0 ] ; then
   echo "FAIL: Unable to find any releases to clean up in $DIST_DIR_URL"
   exit -1;
fi

for FILE in `svn list $RC_DIR_URL`; do
  RC_COUNT=`expr $RC_COUNT + 1`
  RM_RC_CMD="$RM_RC_CMD $RC_DIR_URL/$FILE"
done

echo "## Run the following commands when ready..."
echo "# Delete old releases"
echo $RM_OLD_CMD

if [ $RC_COUNT -eq 0 ] ; then
   echo "# No old RC files to clean up"
else
   echo "# Delete old RC files"
   echo $RM_RC_CMD
fi




