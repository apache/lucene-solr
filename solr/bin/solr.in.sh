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

# Settings here will override settings in existing env vars or in bin/solr.  The default shipped state
# of this file is completely commented.

# By default the script will use JAVA_HOME to determine which java
# to use, but you can set a specific path for Solr to use without
# affecting other Java applications on your server/workstation.
#SOLR_JAVA_HOME=""

# This controls the number of seconds that the solr script will wait for
# Solr to stop gracefully.  If the graceful stop fails, the script will
# forcibly stop Solr.
#SOLR_STOP_WAIT="180"

# This controls the number of seconds that the solr script will wait for
# Solr to start.  If the start fails, the script will give up waiting and
# display the last few lines of the logfile.
#SOLR_START_WAIT="$SOLR_STOP_WAIT"

# Increase Java Heap as needed to support your indexing / query needs
#SOLR_HEAP="512m"

# Expert: If you want finer control over memory options, specify them directly
# Comment out SOLR_HEAP if you are using this though, that takes precedence
#SOLR_JAVA_MEM="-Xms512m -Xmx512m"

# Enable verbose GC logging...
#  * If this is unset, various default options will be selected depending on which JVM version is in use
#  * For Java 8: if this is set, additional params will be added to specify the log file & rotation
#  * For Java 9 or higher: each included opt param that starts with '-Xlog:gc', but does not include an
#    output specifier, will have a 'file' output specifier (as well as formatting & rollover options)
#    appended, using the effective value of the SOLR_LOGS_DIR.
#
#GC_LOG_OPTS='-Xlog:gc*'  # (Java 9+)
#GC_LOG_OPTS="-verbose:gc -XX:+PrintHeapAtGC -XX:+PrintGCDetails \
#  -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps -XX:+PrintTenuringDistribution -XX:+PrintGCApplicationStoppedTime"

# These GC settings have shown to work well for a number of common Solr workloads
#GC_TUNE=" \
#-XX:+ExplicitGCInvokesConcurrent \
#-XX:SurvivorRatio=4 \
#-XX:TargetSurvivorRatio=90 \
#-XX:MaxTenuringThreshold=8 \
#-XX:+UseConcMarkSweepGC \
#-XX:ConcGCThreads=4 -XX:ParallelGCThreads=4 \
#-XX:+CMSScavengeBeforeRemark \
#-XX:PretenureSizeThreshold=64m \
#-XX:+UseCMSInitiatingOccupancyOnly \
#-XX:CMSInitiatingOccupancyFraction=50 \
#-XX:CMSMaxAbortablePrecleanTime=6000 \
#-XX:+CMSParallelRemarkEnabled \
#-XX:+ParallelRefProcEnabled        etc.

# Set the ZooKeeper connection string if using an external ZooKeeper ensemble
# e.g. host1:2181,host2:2181/chroot
# Leave empty if not using SolrCloud
#ZK_HOST=""

# Set the ZooKeeper client timeout (for SolrCloud mode)
#ZK_CLIENT_TIMEOUT="30000"

# By default the start script uses "localhost"; override the hostname here
# for production SolrCloud environments to control the hostname exposed to cluster state
#SOLR_HOST="192.168.1.1"

# By default Solr will try to connect to Zookeeper with 30 seconds in timeout; override the timeout if needed
#SOLR_WAIT_FOR_ZK="30"

# By default the start script uses UTC; override the timezone if needed
#SOLR_TIMEZONE="UTC"

# Set to true to activate the JMX RMI connector to allow remote JMX client applications
# to monitor the JVM hosting Solr; set to "false" to disable that behavior
# (false is recommended in production environments)
#ENABLE_REMOTE_JMX_OPTS="false"

# The script will use SOLR_PORT+10000 for the RMI_PORT or you can set it here
# RMI_PORT=18983

# Anything you add to the SOLR_OPTS variable will be included in the java
# start command line as-is, in ADDITION to other options. If you specify the
# -a option on start script, those options will be appended as well. Examples:
#SOLR_OPTS="$SOLR_OPTS -Dsolr.autoSoftCommit.maxTime=3000"
#SOLR_OPTS="$SOLR_OPTS -Dsolr.autoCommit.maxTime=60000"

# Location where the bin/solr script will save PID files for running instances
# If not set, the script will create PID files in $SOLR_TIP/bin
#SOLR_PID_DIR=

# Path to a directory for Solr to store cores and their data. By default, Solr will use server/solr
# If solr.xml is not stored in ZooKeeper, this directory needs to contain solr.xml
#SOLR_HOME=

# Path to a directory that Solr will use as root for data folders for each core.
# If not set, defaults to <instance_dir>/data. Overridable per core through 'dataDir' core property
#SOLR_DATA_HOME=

# Solr provides a default Log4J configuration xml file in server/resources
# however, you may want to customize the log settings and file appender location
# so you can point the script to use a different log4j2.xml file
#LOG4J_PROPS=/var/solr/log4j2.xml

# Changes the logging level. Valid values: ALL, TRACE, DEBUG, INFO, WARN, ERROR, FATAL, OFF. Default is INFO
# This is an alternative to changing the rootLogger in log4j2.xml
#SOLR_LOG_LEVEL=INFO

# Location where Solr should write logs to. Absolute or relative to solr start dir
#SOLR_LOGS_DIR=logs

# Enables log rotation before starting Solr. Setting SOLR_LOG_PRESTART_ROTATION=true will let Solr take care of pre
# start rotation of logs. This is false by default as log4j2 handles this for us. If you choose to use another log
# framework that cannot do startup rotation, you may want to enable this to let Solr rotate logs on startup.
#SOLR_LOG_PRESTART_ROTATION=false

# Enables jetty request log for all requests
#SOLR_REQUESTLOG_ENABLED=false

# Sets the port Solr binds to, default is 8983
#SOLR_PORT=8983

# Restrict access to solr by IP address.
# Specify a comma-separated list of addresses or networks, for example:
#   127.0.0.1, 192.168.0.0/24, [::1], [2000:123:4:5::]/64
#SOLR_IP_WHITELIST=

# Block access to solr from specific IP addresses.
# Specify a comma-separated list of addresses or networks, for example:
#   127.0.0.1, 192.168.0.0/24, [::1], [2000:123:4:5::]/64
#SOLR_IP_BLACKLIST=

# Enables HTTPS. It is implictly true if you set SOLR_SSL_KEY_STORE. Use this config
# to enable https module with custom jetty configuration.
#SOLR_SSL_ENABLED=true
# Uncomment to set SSL-related system properties
# Be sure to update the paths to the correct keystore for your environment
#SOLR_SSL_KEY_STORE=etc/solr-ssl.keystore.p12
#SOLR_SSL_KEY_STORE_PASSWORD=secret
#SOLR_SSL_TRUST_STORE=etc/solr-ssl.keystore.p12
#SOLR_SSL_TRUST_STORE_PASSWORD=secret
# Require clients to authenticate
#SOLR_SSL_NEED_CLIENT_AUTH=false
# Enable clients to authenticate (but not require)
#SOLR_SSL_WANT_CLIENT_AUTH=false
# Verify client's hostname during SSL handshake
#SOLR_SSL_CLIENT_HOSTNAME_VERIFICATION=false
# SSL Certificates contain host/ip "peer name" information that is validated by default. Setting
# this to false can be useful to disable these checks when re-using a certificate on many hosts
#SOLR_SSL_CHECK_PEER_NAME=true
# Override Key/Trust Store types if necessary
#SOLR_SSL_KEY_STORE_TYPE=PKCS12
#SOLR_SSL_TRUST_STORE_TYPE=PKCS12

# Uncomment if you want to override previously defined SSL values for HTTP client
# otherwise keep them commented and the above values will automatically be set for HTTP clients
#SOLR_SSL_CLIENT_KEY_STORE=
#SOLR_SSL_CLIENT_KEY_STORE_PASSWORD=
#SOLR_SSL_CLIENT_TRUST_STORE=
#SOLR_SSL_CLIENT_TRUST_STORE_PASSWORD=
#SOLR_SSL_CLIENT_KEY_STORE_TYPE=
#SOLR_SSL_CLIENT_TRUST_STORE_TYPE=

# Sets path of Hadoop credential provider (hadoop.security.credential.provider.path property) and
# enables usage of credential store.
# Credential provider should store the following keys:
# * solr.jetty.keystore.password
# * solr.jetty.truststore.password
# Set the two below if you want to set specific store passwords for HTTP client
# * javax.net.ssl.keyStorePassword
# * javax.net.ssl.trustStorePassword
# More info: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/CredentialProviderAPI.html
#SOLR_HADOOP_CREDENTIAL_PROVIDER_PATH=localjceks://file/home/solr/hadoop-credential-provider.jceks
#SOLR_OPTS=" -Dsolr.ssl.credential.provider.chain=hadoop"

# Settings for authentication
# Please configure only one of SOLR_AUTHENTICATION_CLIENT_BUILDER or SOLR_AUTH_TYPE parameters
#SOLR_AUTHENTICATION_CLIENT_BUILDER="org.apache.solr.client.solrj.impl.PreemptiveBasicAuthClientBuilderFactory"
#SOLR_AUTH_TYPE="basic"
#SOLR_AUTHENTICATION_OPTS="-Dbasicauth=solr:SolrRocks"

# Settings for ZK ACL
#SOLR_ZK_CREDS_AND_ACLS="-DzkACLProvider=org.apache.solr.common.cloud.VMParamsAllAndReadonlyDigestZkACLProvider \
#  -DzkCredentialsProvider=org.apache.solr.common.cloud.VMParamsSingleSetCredentialsDigestZkCredentialsProvider \
#  -DzkDigestUsername=admin-user -DzkDigestPassword=CHANGEME-ADMIN-PASSWORD \
#  -DzkDigestReadonlyUsername=readonly-user -DzkDigestReadonlyPassword=CHANGEME-READONLY-PASSWORD"
#SOLR_OPTS="$SOLR_OPTS $SOLR_ZK_CREDS_AND_ACLS"


# Settings for common system values that may cause operational imparement when system defaults are used.
# Solr can use many processes and many file handles. On modern operating systems the savings by leaving
# these settings low is minuscule, while the consequence can be Solr instability. To turn these checks off, set
# SOLR_ULIMIT_CHECKS=false either here or as part of your profile.

# Different limits can be set in solr.in.sh or your profile if you prefer as well.
#SOLR_RECOMMENDED_OPEN_FILES=
#SOLR_RECOMMENDED_MAX_PROCESSES=
#SOLR_ULIMIT_CHECKS=

# When running Solr in non-cloud mode and if planning to do distributed search (using the "shards" parameter), the
# list of hosts needs to be whitelisted or Solr will forbid the request. The whitelist can be configured in solr.xml,
# or if you are using the OOTB solr.xml, can be specified using the system property "solr.shardsWhitelist". Alternatively
# host checking can be disabled by using the system property "solr.disable.shardsWhitelist"
#SOLR_OPTS="$SOLR_OPTS -Dsolr.shardsWhitelist=http://localhost:8983,http://localhost:8984"

# For a visual indication in the Admin UI of what type of environment this cluster is, configure
# a -Dsolr.environment property below. Valid values are prod, stage, test, dev, with an optional
# label or color, e.g. -Dsolr.environment=test,label=Functional+test,color=brown
#SOLR_OPTS="$SOLR_OPTS -Dsolr.environment=prod"

# Specifies the path to a common library directory that will be shared across all cores.
# Any JAR files in this directory will be added to the search path for Solr plugins.
# If the specified path is not absolute, it will be relative to `$SOLR_HOME`.
#SOLR_OPTS="$SOLR_OPTS -Dsolr.sharedLib=/path/to/lib"

# Runs solr in java security manager sandbox. This can protect against some attacks.
# Runtime properties are passed to the security policy file (server/etc/security.policy)
# You can also tweak via standard JDK files such as ~/.java.policy, see https://s.apache.org/java8policy
# This is experimental! It may not work at all with Hadoop/HDFS features.
#SOLR_SECURITY_MANAGER_ENABLED=false

# Solr is by default allowed to read and write data from/to SOLR_HOME and a few other well defined locations
# Sometimes it may be necessary to place a core or a backup on a different location or a different disk
# This parameter lets you specify file system path(s) to explicitly allow. The special value of '*' will allow any path
#SOLR_OPTS="$SOLR_OPTS -Dsolr.allowPaths=/mnt/bigdisk,/other/path"

# Solr can attempt to take a heap dump on out of memory errors. To enable this, uncomment the line setting
# SOLR_HEAP_DUMP below. Heap dumps will be saved to SOLR_LOG_DIR/dumps by default. Alternatively, you can specify any
# other directory, which will implicitly enable heap dumping. Dump name pattern will be solr-[timestamp]-pid[###].hprof
# When using this feature, it is recommended to have an external service monitoring the given dir.
# If more fine grained control is required, you can manually add the appropriate flags to SOLR_OPTS
# See https://docs.oracle.com/en/java/javase/11/troubleshoot/command-line-options1.html
# You can test this behaviour by setting SOLR_HEAP=25m
#SOLR_HEAP_DUMP=true
#SOLR_HEAP_DUMP_DIR=/var/log/dumps
