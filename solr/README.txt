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

See the "example" directory for an example Solr setup.  A tutorial
using the example setup can be found at
http://lucene.apache.org/solr/tutorial.html
or in in "docs/tutorial.html" in a binary distribution.



Files included in an Apache Solr binary distribution
----------------------------------------------------

example/
  A self-contained example Solr instance, complete with a sample
  configuration, documents to index, and the Jetty Servlet container.
  Please see example/README.txt for information about running this
  example.

dist/apache-solr-XX.war
  The Apache Solr Application.  Deploy this WAR file to any servlet
  container to run Apache Solr.

dist/apache-solr-XX.jar
  The Apache Solr Libraries.  This JAR file is needed to compile
  Apache Solr Plugins (see http://wiki.apache.org/solr/SolrPlugins for
  more information).

docs/index.html
  The contents of the Apache Solr website.
  
docs/api/index.html
  The Apache Solr Javadoc API documentation.



Instructions for Building Apache Solr from Source
-------------------------------------------------

1. Download the Java SE 6 JDK (Java Development Kit) or later from http://java.sun.com/
   You will need the JDK installed, and the $JAVA_HOME/bin (Windows: %JAVA_HOME%\bin) 
   folder included on your command path. To test this, issue a "java -version" command 
   from your shell (command prompt) and verify that the Java version is 1.6 or later.

2. Download the Apache Ant binary distribution (1.7.x, not 1.6.x, not 1.8.x) from http://ant.apache.org/
   You will need Ant installed and the $ANT_HOME/bin (Windows: %ANT_HOME%\bin) folder 
   included on your command path. To test this, issue a "ant -version" command from your
   shell (command prompt) and verify that Ant is available.

3. Download the Apache Solr distribution, linked from the above web site. 
   Unzip the distribution to a folder of your choice, e.g. C:\solr or ~/solr
   Alternately, you can obtain a copy of the latest Apache Solr source code
   directly from the Subversion repository:

     http://lucene.apache.org/solr/version_control.html

4. Navigate to the "solr" folder and issue an "ant" command to see the available options
   for building, testing, and packaging Solr.
  
   NOTE: 
   To see Solr in action, you may want to use the "ant example" command to build
   and package Solr into the example/webapps directory. See also example/README.txt.


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
