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

Solr example
------------

This directory contains an instance of the Jetty Servlet container setup to 
run Solr using an example configuration.

To run this example:

  java -jar start.jar

in this example directory, and when Solr is started connect to 

  http://localhost:8983/solr/

To add documents to the index, use the post.jar (or post.sh script) in
the example/exampledocs subdirectory (while Solr is running), for example:

     cd exampledocs
     java -jar post.jar *.xml
Or:  sh post.sh *.xml

For more information about this example please read...

 * example/solr/README.txt
   For more information about the "Solr Home" and Solr specific configuration
 * http://lucene.apache.org/solr/tutorial.html
   For a Tutorial using this example configuration
 * http://wiki.apache.org/solr/SolrResources 
   For a list of other tutorials and introductory articles.

Notes About These Examples
--------------------------

* SolrHome *

By default, start.jar starts Solr in Jetty using the default Solr Home
directory of "./solr/" (relative to the working directory of hte servlet 
container).  To run other example configurations, you can specify the 
solr.solr.home system property when starting jetty...

  java -Dsolr.solr.home=multicore -jar start.jar
  java -Dsolr.solr.home=example-DIH/solr -jar start.jar

* References to Jar Files Outside This Directory *

Various example SolrHome dirs contained in this directory may use "<lib>"
statements in the solrconfig.xml file to reference plugin jars outside of 
this directory for loading "contrib" plugins via relative paths.  

If you make a copy of this example server and wish to use the 
ExtractingRequestHandler (SolrCell), DataImportHandler (DIH), UIMA, the 
clustering component, or any other modules in "contrib", you will need to 
copy the required jars or update the paths to those jars in your 
solrconfig.xml.

* Logging *

By default, Jetty & Solr will log to the console a logs/solr.log. This can be convenient when 
first getting started, but eventually you will want to log just to a file. To 
configure logging, edit the log4j.properties file in "resources".
 
It is also possible to setup log4j or other popular logging frameworks.

