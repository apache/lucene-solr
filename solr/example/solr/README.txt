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


Example Solr Home Directory
=============================

This directory is provided as an example of what a "Solr Home" directory
should look like.

It's not strictly necessary that you copy all of the files in this
directory when setting up a new instance of Solr, but it is recommended.


Basic Directory Structure
-------------------------

The Solr Home directory typically contains the following...

* solr.xml *

This is the primary configuration file Solr looks for when starting.
This file specifies the list of "SolrCores" it should load, and high 
level configuration options that should be used for all SolrCores.

Please see the comments in ./solr.xml for more details.

If no solr.xml file is found, then Solr assumes that there should be
a single SolrCore named "collection1" and that the "Instance Directory" 
for collection1 should be the same as the Solr Home Directory.

* Individual SolrCore Instance Directories *

Although solr.xml can be configured to look for SolrCore Instance Directories 
in any path, simple sub-directories of the Solr Home Dir using relative paths 
are common for many installations.  In this directory you can see the 
"./collection1" Instance Directory.

* A Shared 'lib' Directory *

Although solr.xml can be configured with an optional "sharedLib" attribute 
that can point to any path, it is common to use a "./lib" sub-directory of the 
Solr Home Directory.

* ZooKeeper Files *

When using SolrCloud using the embedded ZooKeeper option for Solr, it is 
common to have a "zoo.cfg" file and "zoo_data" directories in the Solr Home 
Directory.  Please see the SolrCloud wiki page for more details...

https://wiki.apache.org/solr/SolrCloud
