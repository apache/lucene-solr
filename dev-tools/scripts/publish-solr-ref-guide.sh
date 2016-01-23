#!/bin/sh

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

# Convinience script to generates the SVN command line for you to run in order to 
# publish an RC of the Solr Ref Guide that is already in the dist/dev repo (so that
# all of the files for that RC will be moved into the dist/release hierarchy).
#
# See: https://cwiki.apache.org/confluence/display/solr/Internal+-+How+To+Publish+This+Documentation

if [ $# -ne 1 ] ; then
    echo "Usage: $0 <X.Y-RCZ>"
    echo ""
    echo "Example: "
    echo "    $0 4.5-RC0"
    echo ""
    echo "An RC with that version & RC# must already exist"
    exit 1
fi

PREFIX="apache-solr-ref-guide"
RC="$PREFIX-$1"
RC_URL="https://dist.apache.org/repos/dist/dev/lucene/solr/ref-guide/$RC"
MV_CMD="svn move -m 'publishing $RC'"
RM_CMD="svn rm -m 'cleaning up $RC' $RC_URL"
COUNT=0

for FILE in `svn list $RC_URL`; do
  COUNT=`expr $COUNT + 1`
  MV_CMD="$MV_CMD $RC_URL/$FILE"
done

if [ $COUNT -eq 0 ] ; then
   echo "Unable to find any files in RC Directory: $RC_URL"
   exit 0;
fi

#destination of move...
MV_CMD="$MV_CMD https://dist.apache.org/repos/dist/release/lucene/solr/ref-guide/"

echo "## Run the following commands when ready..."
echo $MV_CMD
echo $RM_CMD
