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
 
# This script shows how the keystore files used for solr tests were generated.
#
# Running this script should only be necessary if the keystore files need to be
# replaced, which shouldn't be required until sometime around the year 4751.

# NOTE: if anything below changes, sanity check SSLTestConfig constructor javadocs

echo "### remove old keystores"
rm -f SSLTestConfig.testing.keystore SSLTestConfig.hostname-and-ip-missmatch.keystore

echo "### create 'localhost' keystore and keys"
keytool -keystore SSLTestConfig.testing.keystore -storepass "secret" -alias solrtest -keypass "secret" -genkey -keyalg RSA -dname "cn=localhost, ou=SolrTest, o=lucene.apache.org, c=US" -ext "san=dns:localhost,ip:127.0.0.1" -validity 999999

# See https://tools.ietf.org/html/rfc5737
echo "### create 'Bogus Host' keystore and keys"
keytool -keystore SSLTestConfig.hostname-and-ip-missmatch.keystore -storepass "secret" -alias solrtest -keypass "secret" -genkey -keyalg RSA -dname "cn=bogus.hostname.tld, ou=SolrTest, o=lucene.apache.org, c=US" -ext "san=dns:bogus.hostname.tld,ip:192.0.2.0" -validity 999999


