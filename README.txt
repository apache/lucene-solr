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
library.  Solr is currently under incubation.

For a complete description of the Solr project, team composition, source
code repositories, and other details, please see the Solr incubation web site at
http://incubator.apache.org/projects/solr.html.

See the "example" directory for an example Solr setup. 


Instructions for Building Apache Solr
-------------------------------------

1. Download the J2SE 5.0 JDK (Java Development Kit) or later from http://java.sun.com.
   You will need the JDK installed, and the %JAVA_HOME%\bin directory included
   on your command path.  To test this, issue a "java -version" command from your
   shell and verify that the Java version is 5.0 or later.

2. Download the Apache Ant binary distribution from http://ant.apache.org.
   You will need Ant installed and the %ANT_HOME%\bin directory included on your
   command path.  To test this, issue a "ant -version" command from your
   shell and verify that Ant is available.

3. Download the Apache Solr source distribution, linked from the above incubator
   web site.  Expand the distribution to a folder of your choice, e.g. c:\solr.

4. Navigate to that folder and issue an "ant" command to see the available options
   for building, testing, and packaging solr.
