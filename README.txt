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

Apache Solr is a search server based on the Apache Lucene search
library. 

For a complete description of the Solr project, team composition, source
code repositories, and other details, please see the Solr web site at
http://lucene.apache.org/solr.html.


Getting Started
---------------

See the "example" directory for an example Solr setup.  A tutorial
using the example setup can be found in "docs/tutorial.html" 



Files Included In Apache Solr Distributions
-------------------------------------------

dist/apache-solr-XX.war
  The Apache Solr Application.  Deploy this WAR file to any servlet
  container to run Apache Solr.

dist/apache-solr-XXg.jar
  The Apache Solr Libraries.  This JAR file is needed to compile
  Apache Solr Plugins (see http://wiki.apache.org/solr/SolrPlugins for
  more information).

example/
  A self-contained example Solr instance, complete with a sample
  configuration, documents to index, and the Jetty Servlet container.
  Please see example/README.txt for information about running this
  example.

docs/index.html
  The contents of the Apache Solr website.
  
docs/api/index.html
  The Apache Solr Javadoc API documentation.

src/
  The Apache Solr source code.



Instructions for Building Apache Solr from Source
-------------------------------------------------

1. Download the J2SE 5.0 JDK (Java Development Kit) or later from http://java.sun.com.
   You will need the JDK installed, and the %JAVA_HOME%\bin directory included
   on your command path.  To test this, issue a "java -version" command from your
   shell and verify that the Java version is 5.0 or later.

2. Download the Apache Ant binary distribution from http://ant.apache.org.
   You will need Ant installed and the %ANT_HOME%\bin directory included on your
   command path.  To test this, issue a "ant -version" command from your
   shell and verify that Ant is available.

3. Download the Apache Solr distribution, linked from the above incubator
   web site.  Expand the distribution to a folder of your choice, e.g. c:\solr.   
   Alternately, you can obtain a copy of the latest Apache Solr source code
   directly from the Subversion repository:

     http://incubator.apache.org/solr/version_control.html

4. Navigate to that folder and issue an "ant" command to see the available options
   for building, testing, and packaging Solr.
  
   NOTE:
   Solr uses JUnit (http://www.junit.org/index.htm) for unit testing.  You will need
   the JUnit library in your Ant classpath in order to compile and run the unit tests.
   See http://ant.apache.org/manual/OptionalTasks/junit.html for instructions.

   NOTE: 
   To see Solr in action, you may want to use the "ant example" command to build
   and package Solr into the example/webapps directory. See also example/README.txt.
