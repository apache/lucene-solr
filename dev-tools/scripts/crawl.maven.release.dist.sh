#!/bin/sh
#
# Crawls all Maven release distribution artifacts at the given URL
# and downloads them to the current directory.
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
    echo "Usage: $0 <RC-url-to-lucene-or-solr-maven-dist-dir>"
    echo "Example: $0 'http://people.apache.org/~rmuir/staging_area/lucene-solr-3.6RC0-rev1309642/solr/maven/'"
    exit 1;
fi

wget -r -np -l 0 -nH -erobots=off --cut-dirs=8 --reject="*.md5,*.sha1,maven-metadata.xml*,index.html*" "$1/"