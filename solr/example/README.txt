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

Solr example configuration
--------------------------

To run this example configuration, use 

  java -jar start.jar

in this directory, and when Solr is started connect to 

  http://localhost:8983/solr/admin/

To add documents to the index, use the post.sh script in the exampledocs
subdirectory (while Solr is running), for example:

  cd exampledocs
  sh post.sh *.xml

See also README.txt in the solr subdirectory, and check
http://wiki.apache.org/solr/SolrResources for a list of tutorials and
introductory articles.

NOTE: This Solr example server references certain Solr jars outside of
this server directory for non-core modules with <lib> statements in
solrconfig.xml.  If you make a copy of this example server and wish
to use the ExtractingRequestHandler (SolrCell), DataImportHandler (DIH),
UIMA, the clustering component, or other modules in "contrib",
you will need to copy the required jars into solr/lib or update the paths to
the jars in your solrconfig.xml.

By default, start.jar starts Solr in Jetty using the default solr home
directory of "./solr/" -- To run other example configurations, you can
speciy the solr.solr.home system property when starting jetty...

  java -Dsolr.solr.home=multicore -jar start.jar
  java -Dsolr.solr.home=example-DIH -jar start.jar

