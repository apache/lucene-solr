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


Welcome to the Apache Solr project!
-----------------------------------

Solr is the popular, blazing fast open source enterprise search platform
from the Apache Lucene project.

For a complete description of the Solr project, team composition, source
code repositories, and other details, please see the Solr web site at
http://lucene.apache.org/solr


Getting Started
---------------

To start Solr for the first time after installation, simply do:

  bin/solr start

This will launch a standalone Solr server in the background of your shell,
listening on port 8983. Alternatively, you can launch Solr in "cloud" mode,
which allows you to scale out using sharding and replication. To launch Solr
in cloud mode, do:

  bin/solr start -cloud

To see all available options for starting Solr, please do:

  bin/solr start -help

After starting Solr, create either a core or collection depending on whether
Solr is running in standalone (core) or SolrCloud mode (collection) by doing:

  bin/solr create -c <name>

This will create a collection that uses a data-driven schema which tries to guess
the correct field type when you add documents to the index. To see all available
options for creating a new collection, execute:

  bin/solr create -help

After starting Solr, direct your Web browser to the Solr Admin Console at:

  http://localhost:8983/solr/

When finished with your Solr installation, shut it down by executing:

  bin/solr stop -all

The `-p PORT` option can also be used to identify the Solr instance to shutdown,
where more than one Solr is running on the machine.


Solr Examples
---------------

Solr includes a few examples to help you get started. To run a specific example, do:

  bin/solr -e <EXAMPLE> where <EXAMPLE> is one of:

    cloud        : SolrCloud example
    dih          : Data Import Handler (rdbms, mail, atom, tika)
    schemaless   : Schema-less example (schema is inferred from data during indexing)
    techproducts : Kitchen sink example providing comprehensive examples of Solr features

For instance, if you want to run the Solr Data Import Handler example, do:

  bin/solr -e dih


Indexing Documents
---------------

To add documents to the index, use bin/post.  For example:

     bin/post -c <collection_name> example/exampledocs/*.xml

For more information about Solr examples please read...

 * example/README.txt
   For more information about the "Solr Home" and Solr specific configuration
 * https://lucene.apache.org/solr/guide/solr-tutorial.html
   For a Solr tutorial
 * http://lucene.apache.org/solr/resources.html
   For a list of other tutorials and introductory articles.

or linked from "docs/index.html" in a binary distribution.

Also, there are Solr clients for many programming languages, see
   http://wiki.apache.org/solr/IntegratingSolr


Files included in an Apache Solr binary distribution
----------------------------------------------------

server/
  A self-contained Solr instance, complete with a sample
  configuration and documents to index. Please see: bin/solr start -help
  for more information about starting a Solr server.

example/
  Contains example documents and an alternative Solr home
  directory containing examples of how to use the Data Import Handler,
  see example/example-DIH/README.txt for more information.

dist/solr-<component>-XX.jar
  The Apache Solr libraries.  To compile Apache Solr Plugins,
  one or more of these will be required.  The core library is
  required at a minimum. (see http://wiki.apache.org/solr/SolrPlugins
  for more information).

docs/index.html
  A link to the online version of Apache Solr Javadoc API documentation and Tutorial


Instructions for Building Apache Solr from Source
-------------------------------------------------

1. Download the Java SE 8 JDK (Java Development Kit) or later from http://www.oracle.com/java/
   You will need the JDK installed, and the $JAVA_HOME/bin (Windows: %JAVA_HOME%\bin)
   folder included on your command path. To test this, issue a "java -version" command
   from your shell (command prompt) and verify that the Java version is 1.8 or later.

2. Download the Apache Ant binary distribution (1.8.2+) from
   http://ant.apache.org/  You will need Ant installed and the $ANT_HOME/bin (Windows:
   %ANT_HOME%\bin) folder included on your command path. To test this, issue a
   "ant -version" command from your shell (command prompt) and verify that Ant is
   available.

   You will also need to install Apache Ivy binary distribution (2.2.0) from
   http://ant.apache.org/ivy/ and place ivy-2.2.0.jar file in ~/.ant/lib -- if you skip
   this step, the Solr build system will offer to do it for you.

3. Download the Apache Solr distribution, linked from the above web site.
   Unzip the distribution to a folder of your choice, e.g. C:\solr or ~/solr
   Alternately, you can obtain a copy of the latest Apache Solr source code
   directly from the GIT repository:

     http://lucene.apache.org/solr/versioncontrol.html

4. Navigate to the "solr" folder and issue an "ant" command to see the available options
   for building, testing, and packaging Solr.

   NOTE:
   To see Solr in action, you may want to use the "ant server" command to build
   and package Solr into the server directory. See also server/README.txt.


Export control
-------------------------------------------------
This distribution includes cryptographic software.  The country in
which you currently reside may have restrictions on the import,
possession, use, and/or re-export to another country, of
encryption software.  BEFORE using any encryption software, please
check your country's laws, regulations and policies concerning the
import, possession, or use, and re-export of encryption software, to
see if this is permitted.  See <http://www.wassenaar.org/> for more
information.

The U.S. Government Department of Commerce, Bureau of Industry and
Security (BIS), has classified this software as Export Commodity
Control Number (ECCN) 5D002.C.1, which includes information security
software using or performing cryptographic functions with asymmetric
algorithms.  The form and manner of this Apache Software Foundation
distribution makes it eligible for export under the License Exception
ENC Technology Software Unrestricted (TSU) exception (see the BIS
Export Administration Regulations, Section 740.13) for both object
code and source code.

The following provides more details on the included cryptographic
software:
    Apache Solr uses the Apache Tika which uses the Bouncy Castle generic encryption libraries for
    extracting text content and metadata from encrypted PDF files.
    See http://www.bouncycastle.org/ for more details on Bouncy Castle.
