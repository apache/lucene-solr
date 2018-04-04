#!/bin/sh
#
# Crawls all Maven release distribution artifacts at the given release RC URL
# and downloads them to ./lucene/ and ./solr/ after first creating these
# two directories in the current directory.
#
#
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

if [ -z "$1" ] ; then
    echo "Usage: $0 <RC-URL>"
    echo ""
    echo "Example: $0 http://s.apache.org/lusolr36rc1"
    exit 1;
fi

# Resolve redirects, e.g. from URL shortening, e.g. http://s.apache.org/lusolr36rc1
# Also trim trailing slashes, if any, from the resolved URL.
RC_URL=`(echo "Location: $1" ; wget -l 1 --spider "$1" 2>&1) \
        | perl -ne '$url=$1 if (/Location:\s*(\S+)/); END { $url =~ s~/+$~~; print $url; }'`

if [ -d lucene ] ; then
    echo "Please remove directory ./lucene/ before running this script."
    exit 1;
elif [ -d solr ] ; then
    echo "Please remove directory ./solr/ before running this script."
    exit 1;
fi

mkdir lucene
cd lucene

# -r : recurse
# -np : "no parents": only download below the given URL
# -l 0 : infinite recursion (no limit on recursive crawling depth)
# -nH : "no Hostname" output directory - use only path elements
# -erobots=off : ignore robots.txt
# --cut-dirs=5: Don't create output directories for the first 5 path elements, e.g.
#    /~acct/staging_area/lucene-solr-X.Y.Z-RCM-revNNNNNNN/lucene/maven/org/apache/lucene/...
#    1     2            3                                4      5     6   7      8      9
#                                                                     ^- Dirs start here     
wget -r -np -l 0 -nH -erobots=off --cut-dirs=5 \
     --reject="*.md5,*.sha1,*.sha512,maven-metadata.xml*,index.html*" "${RC_URL}/lucene/maven/"

cd ..

mkdir solr
cd solr

wget -r -np -l 0 -nH -erobots=off --cut-dirs=5 \
     --reject="*.md5,*.sha1,*.sha512,maven-metadata.xml*,index.html*" "${RC_URL}/solr/maven/"

cd ..

