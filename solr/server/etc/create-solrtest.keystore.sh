#!/bin/bash -ex

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

############
 
# This script shows how the solrtest.keystore file used for solr tests 
# and these example configs was generated.
#
# Running this script should only be necessary if the keystore file
# needs to be replaced, which shouldn't be required until sometime around
# the year 4751.
#
# NOTE: the "-ext" option used in the "keytool" command requires that you have
# the java7 version of keytool, but the generated key will work with any 
# version of java

echo "### remove old keystore"
rm -f solrtest.keystore

echo "### create keystore and keys"
keytool -keystore solrtest.keystore -storepass "secret" -alias solrtest -keypass "secret" -genkey -keyalg RSA -dname "cn=localhost, ou=SolrTest, o=lucene.apache.org, c=US" -ext "san=ip:127.0.0.1" -validity 999999


