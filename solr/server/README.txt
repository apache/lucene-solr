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

Solr server
------------

This directory contains an instance of the Jetty Servlet container setup to 
run Solr.

To run Solr:

  cd $SOLR_INSTALL
  bin/solr start

where $SOLR_INSTALL is the location where you extracted the Solr installation bundle.

Server directory layout
-----------------------

server/contexts

  This directory contains the Jetty Web application deployment descriptor for the Solr Web app.

server/etc

  Jetty configuration and example SSL keystore

server/lib

  Jetty and other 3rd party libraries

server/logs

  Solr log files

server/resources

  Contains configuration files, such as the Log4j configuration (log4j2.xml) for configuring Solr loggers.

server/scripts/cloud-scripts

  Command-line utility for working with ZooKeeper when running in SolrCloud mode, see zkcli.sh / .cmd for
  usage information.

server/solr

  Default solr.solr.home directory where Solr will create core directories; must contain solr.xml

server/solr/configsets

  Directories containing different configuration options for running Solr.

    _default                    : Bare minimum configurations with field-guessing and managed schema turned
                                  on by default, so as to start indexing data in Solr without having to design
                                  a schema upfront. You can use the REST API to manage your schema as you refine your index
                                  requirements. You can turn off the field (for a collection, say mycollection) guessing by:
                                  curl http://host:8983/solr/mycollection/config -d '{"set-user-property": {"update.autoCreateFields":"false"}}'

    sample_techproducts_configs : Comprehensive example configuration that demonstrates many of the powerful
                                  features of Solr, based on the use case of building a search solution for
                                  tech products.

server/solr-webapp

  Contains files used by the Solr server; do not edit files in this directory (Solr is not a Java Web application).


Notes About Solr Examples
--------------------------

* SolrHome *

By default, start.jar starts Solr in Jetty using the default Solr Home
directory of "./solr/" (relative to the working directory of the servlet 
container).

* References to Jar Files Outside This Directory *

Various example SolrHome dirs contained in this directory may use "<lib>"
statements in the solrconfig.xml file to reference plugin jars outside of 
this directory for loading "contrib" plugins via relative paths.  

If you make a copy of this example server and wish to use the 
ExtractingRequestHandler (SolrCell), DataImportHandler (DIH), the 
clustering component, or any other modules in "contrib", you will need to 
copy the required jars or update the paths to those jars in your 
solrconfig.xml.

* Logging *

By default, Jetty & Solr will log to the console and logs/solr.log. This can
be convenient when first getting started, but eventually you will want to
log just to a file. To configure logging, edit the log4j2.xml file in
"resources".
 
It is also possible to setup log4j or other popular logging frameworks.

