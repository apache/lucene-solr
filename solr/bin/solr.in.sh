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

# By default the script will use JAVA_HOME to determine which java
# to use, but you can set a specific path for Solr to use without
# affecting other Java applications on your server/workstation.
#SOLR_JAVA_HOME=""

# Increase Java Min/Max Heap as needed to support your indexing / query needs
SOLR_JAVA_MEM="-Xms512m -Xmx512m"

# Enable verbose GC logging
GC_LOG_OPTS="-verbose:gc -XX:+PrintHeapAtGC -XX:+PrintGCDetails \
-XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps -XX:+PrintTenuringDistribution -XX:+PrintGCApplicationStoppedTime"

# These GC settings have shown to work well for a number of common Solr workloads
GC_TUNE="-XX:NewRatio=3 \
-XX:SurvivorRatio=4 \
-XX:TargetSurvivorRatio=90 \
-XX:MaxTenuringThreshold=8 \
-XX:+UseConcMarkSweepGC \
-XX:+UseParNewGC \
-XX:ConcGCThreads=4 -XX:ParallelGCThreads=4 \
-XX:+CMSScavengeBeforeRemark \
-XX:PretenureSizeThreshold=64m \
-XX:+UseCMSInitiatingOccupancyOnly \
-XX:CMSInitiatingOccupancyFraction=50 \
-XX:+PerfDisableSharedMem \
-XX:CMSMaxAbortablePrecleanTime=6000 \
-XX:+CMSParallelRemarkEnabled \
-XX:+ParallelRefProcEnabled"

# Set the ZooKeeper connection string if using an external ZooKeeper ensemble
# e.g. host1:2181,host2:2181/chroot
# Leave empty if not using SolrCloud
#ZK_HOST=""

# Set the ZooKeeper client timeout (for SolrCloud mode)
#ZK_CLIENT_TIMEOUT="15000"

# By default the start script uses "localhost"; override the hostname here
# for production SolrCloud environments to control the hostname exposed to cluster state
#SOLR_HOST="192.168.1.1"

# By default the start script uses UTC; override the timezone if needed
#SOLR_TIMEZONE="UTC"

# Set to true to activate the JMX RMI connector to allow remote JMX client applications
# to monitor the JVM hosting Solr; set to "false" to disable that behavior
# (false is recommended in production environments)
ENABLE_REMOTE_JMX_OPTS="false"

# The script will use SOLR_PORT+10000 for the RMI_PORT or you can set it here
# RMI_PORT=18983

# Anything you add to the SOLR_OPTS variable will be included in the java
# start command line as-is, in ADDITION to other options. If you specify the
# -a option on start script, those options will be appended as well. Examples:
#SOLR_OPTS="$SOLR_OPTS -Dsolr.autoSoftCommit.maxTime=3000"
#SOLR_OPTS="$SOLR_OPTS -Dsolr.autoCommit.maxTime=60000"
#SOLR_OPTS="$SOLR_OPTS -Dsolr.clustering.enabled=true"

# Location where the bin/solr script will save PID files for running instances
# If not set, the script will create PID files in $SOLR_TIP/bin
#SOLR_PID_DIR=

# Path to a directory where Solr creates index files, the specified directory
# must contain a solr.xml; by default, Solr will use server/solr
#SOLR_HOME=

# Solr provides a default Log4J configuration properties file in server/resources
# however, you may want to customize the log settings and file appender location
# so you can point the script to use a different log4j.properties file
#LOG4J_PROPS=/var/solr/log4j.properties

# Location where Solr should write logs to; should agree with the file appender
# settings in server/resources/log4j.properties
#SOLR_LOGS_DIR=

# Sets the port Solr binds to, default is 8983
#SOLR_PORT=8983

# Uncomment to set SSL-related system properties
# Be sure to update the paths to the correct keystore for your environment
#SOLR_SSL_OPTS="-Djavax.net.ssl.keyStore=etc/solr-ssl.keystore.jks \
#-Djavax.net.ssl.keyStorePassword=secret \
#-Djavax.net.ssl.trustStore=etc/solr-ssl.keystore.jks \
#-Djavax.net.ssl.trustStorePassword=secret"

# Uncomment to set a specific SSL port (-Djetty.ssl.port=N); if not set
# and you are using SSL, then the start script will use SOLR_PORT for the SSL port
#SOLR_SSL_PORT=
