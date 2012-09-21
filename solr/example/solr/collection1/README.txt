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


Example SolrCore Instance Directory
=============================

This directory is provided as an example of what an "Instance Directory"
should look like for a SolrCore

It's not strictly necessary that you copy all of the files in this
directory when setting up a new SolrCores, but it is recommended.


Basic Directory Structure
-------------------------

The Solr Home directory typically contains the following sub-directories...

   conf/
        This directory is mandatory and must contain your solrconfig.xml
        and schema.xml.  Any other optional configuration files would also 
        be kept here.

   data/
        This directory is the default location where Solr will keep your
        index, and is used by the replication scripts for dealing with
        snapshots.  You can override this location in the 
        conf/solrconfig.xml.  Solr will create this directory if it does not 
        already exist.

   lib/
        This directory is optional.  If it exists, Solr will load any Jars
        found in this directory and use them to resolve any "plugins"
        specified in your solrconfig.xml or schema.xml (ie: Analyzers,
        Request Handlers, etc...).  Alternatively you can use the <lib>
        syntax in conf/solrconfig.xml to direct Solr to your plugins.  See 
        the example conf/solrconfig.xml file for details.
