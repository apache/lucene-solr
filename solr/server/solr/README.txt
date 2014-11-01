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


Default Solr Home Directory
=============================

This directory is the default Solr home directory which holds 
configuration files and Solr indexes (called cores).


Basic Directory Structure
-------------------------

The Solr Home directory typically contains the following...

* solr.xml *

This is the primary configuration file Solr looks for when starting;
it specifies high-level configuration options that apply to all
of your Solr cores, such as cluster-wide SolrCloud settings like
the ZooKeeper client timeout.

In addition, you can also declare Solr cores in this file, however
it is recommended to just use automatic core discovery instead of
listing cores in solr.xml.

If no solr.xml file is found, then Solr assumes that there should be
a single SolrCore named "collection1" and that the "Instance Directory" 
for collection1 should be the same as the Solr Home Directory.

For more information about solr.xml, please see:
https://cwiki.apache.org/confluence/display/solr/Solr+Cores+and+solr.xml

* Individual SolrCore Instance Directories *

Although solr.xml can be configured to look for SolrCore Instance Directories 
in any path, simple sub-directories of the Solr Home Dir using relative paths 
are common for many installations.  

* Core Discovery *

During startup, Solr will scan sub-directories of Solr home looking for
a specific file named core.properties. If core.properties is found in a
sub-directory (at any depth), Solr will initialize a core using the properties
defined in core.properties. For an example of core.properties, please see:

example/solr/collection1/core.properties

For more information about core discovery, please see:
https://cwiki.apache.org/confluence/display/solr/Moving+to+the+New+solr.xml+Format 

* A Shared 'lib' Directory *

Although solr.xml can be configured with an optional "sharedLib" attribute 
that can point to any path, it is common to use a "./lib" sub-directory of the 
Solr Home Directory.

* ZooKeeper Files *

When using SolrCloud using the embedded ZooKeeper option for Solr, it is 
common to have a "zoo.cfg" file and "zoo_data" directories in the Solr Home 
Directory.  Please see the SolrCloud wiki page for more details...

https://wiki.apache.org/solr/SolrCloud
