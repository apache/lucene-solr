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

This directory contains Solr examples. Each example is contained in a 
separate directory. To run a specific example, do:

  bin/solr -e <EXAMPLE> where <EXAMPLE> is one of:
  
    cloud        : SolrCloud example
    dih          : Data Import Handler (rdbms, mail, atom, tika)
    schemaless   : Schema-less example (schema is inferred from data during indexing)
    techproducts : Kitchen sink example providing comprehensive examples of Solr features

For instance, if you want to run the Solr Data Import Handler example, do:

  bin/solr -e dih
  
To see all the options available when starting Solr:

  bin/solr start -help

After starting a Solr example, direct your Web browser to:

  http://localhost:8983/solr/

To add documents to the index, use bin/post, for example:

     bin/post -c techproducts example/exampledocs/*.xml

(where "techproducts" is the Solr core name)

For more information about this example please read...

 * example/solr/README.txt
   For more information about the "Solr Home" and Solr specific configuration
 * http://lucene.apache.org/solr/quickstart.html
   For a Tutorial using this example configuration
 * http://wiki.apache.org/solr/SolrResources 
   For a list of other tutorials and introductory articles.

Notes About These Examples
--------------------------

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

By default, Jetty & Solr will log to the console and logs/solr.log. This can
be convenient when first getting started, but eventually you will want to
log just to a file. To configure logging, edit the log4j.properties file in
"resources".
It is also possible to setup log4j or other popular logging frameworks.

