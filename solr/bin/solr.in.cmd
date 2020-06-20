@REM
@REM  Licensed to the Apache Software Foundation (ASF) under one or more
@REM  contributor license agreements.  See the NOTICE file distributed with
@REM  this work for additional information regarding copyright ownership.
@REM  The ASF licenses this file to You under the Apache License, Version 2.0
@REM  (the "License"); you may not use this file except in compliance with
@REM  the License.  You may obtain a copy of the License at
@REM
@REM      http://www.apache.org/licenses/LICENSE-2.0
@REM
@REM  Unless required by applicable law or agreed to in writing, software
@REM  distributed under the License is distributed on an "AS IS" BASIS,
@REM  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@REM  See the License for the specific language governing permissions and
@REM  limitations under the License.

@echo off

REM Settings here will override settings in existing env vars or in bin/solr.  The default shipped state
REM of this file is completely commented.

REM By default the script will use JAVA_HOME to determine which java
REM to use, but you can set a specific path for Solr to use without
REM affecting other Java applications on your server/workstation.
REM set SOLR_JAVA_HOME=

REM Increase Java Min/Max Heap as needed to support your indexing / query needs
REM set SOLR_JAVA_MEM=-Xms512m -Xmx512m

REM Configure verbose GC logging:
REM For Java 8: if this is set, additional params will be added to specify the log file & rotation
REM For Java 9 or higher: GC_LOG_OPTS is currently not supported. If you set it, the startup script will exit with failure.
REM set GC_LOG_OPTS=-verbose:gc -XX:+PrintHeapAtGC -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps -XX:+PrintTenuringDistribution -XX:+PrintGCApplicationStoppedTime

REM Various GC settings have shown to work well for a number of common Solr workloads.
REM See solr.cmd GC_TUNE for the default list.
REM set GC_TUNE=-XX:+ExplicitGCInvokesConcurrent
REM set GC_TUNE=-XX:SurvivorRatio=4
REM set GC_TUNE=%GC_TUNE% -XX:TargetSurvivorRatio=90
REM set GC_TUNE=%GC_TUNE% -XX:MaxTenuringThreshold=8
REM set GC_TUNE=%GC_TUNE% -XX:+UseConcMarkSweepGC
REM set GC_TUNE=%GC_TUNE% -XX:ConcGCThreads=4
REM set GC_TUNE=%GC_TUNE% -XX:ParallelGCThreads=4
REM set GC_TUNE=%GC_TUNE% -XX:+CMSScavengeBeforeRemark
REM set GC_TUNE=%GC_TUNE% -XX:PretenureSizeThreshold=64m
REM set GC_TUNE=%GC_TUNE% -XX:+UseCMSInitiatingOccupancyOnly
REM set GC_TUNE=%GC_TUNE% -XX:CMSInitiatingOccupancyFraction=50
REM set GC_TUNE=%GC_TUNE% -XX:CMSMaxAbortablePrecleanTime=6000
REM set GC_TUNE=%GC_TUNE% -XX:+CMSParallelRemarkEnabled
REM set GC_TUNE=%GC_TUNE% -XX:+ParallelRefProcEnabled      etc.

REM Set the ZooKeeper connection string if using an external ZooKeeper ensemble
REM e.g. host1:2181,host2:2181/chroot
REM Leave empty if not using SolrCloud
REM set ZK_HOST=

REM Set the ZooKeeper client timeout (for SolrCloud mode)
REM set ZK_CLIENT_TIMEOUT=15000

REM By default the start script uses "localhost"; override the hostname here
REM for production SolrCloud environments to control the hostname exposed to cluster state
REM set SOLR_HOST=192.168.1.1

REM By default Solr will try to connect to Zookeeper with 30 seconds in timeout; override the timeout if needed
REM set SOLR_WAIT_FOR_ZK=30

REM By default the start script uses UTC; override the timezone if needed
REM set SOLR_TIMEZONE=UTC

REM Set to true to activate the JMX RMI connector to allow remote JMX client applications
REM to monitor the JVM hosting Solr; set to "false" to disable that behavior
REM (false is recommended in production environments)
REM set ENABLE_REMOTE_JMX_OPTS=false

REM The script will use SOLR_PORT+10000 for the RMI_PORT or you can set it here
REM set RMI_PORT=18983

REM Anything you add to the SOLR_OPTS variable will be included in the java
REM start command line as-is, in ADDITION to other options. If you specify the
REM -a option on start script, those options will be appended as well. Examples:
REM set SOLR_OPTS=%SOLR_OPTS% -Dsolr.autoSoftCommit.maxTime=3000
REM set SOLR_OPTS=%SOLR_OPTS% -Dsolr.autoCommit.maxTime=60000
REM set SOLR_OPTS=%SOLR_OPTS% -Dsolr.clustering.enabled=true

REM Path to a directory for Solr to store cores and their data. By default, Solr will use server\solr
REM If solr.xml is not stored in ZooKeeper, this directory needs to contain solr.xml
REM set SOLR_HOME=

REM Path to a directory that Solr will use as root for data folders for each core.
REM If not set, defaults to <instance_dir>/data. Overridable per core through 'dataDir' core property
REM set SOLR_DATA_HOME=

REM Changes the logging level. Valid values: ALL, TRACE, DEBUG, INFO, WARN, ERROR, FATAL, OFF. Default is INFO
REM This is an alternative to changing the rootLogger in log4j2.xml
REM set SOLR_LOG_LEVEL=INFO

REM Location where Solr should write logs to. Absolute or relative to solr start dir
REM set SOLR_LOGS_DIR=logs

REM Enables log rotation before starting Solr. Setting SOLR_LOG_PRESTART_ROTATION=true will let Solr take care of pre
REM start rotation of logs. This is false by default as log4j2 handles this for us. If you choose to use another log
REM framework that cannot do startup rotation, you may want to enable this to let Solr rotate logs on startup.
REM set SOLR_LOG_PRESTART_ROTATION=false

REM Enables jetty request log for all requests
REM set SOLR_REQUESTLOG_ENABLED=false

REM Set the host interface to listen on. Jetty will listen on all interfaces (0.0.0.0) by default.
REM This must be an IPv4 ("a.b.c.d") or bracketed IPv6 ("[x::y]") address, not a hostname!
REM set SOLR_JETTY_HOST=0.0.0.0

REM Sets the port Solr binds to, default is 8983
REM set SOLR_PORT=8983

REM Restrict access to solr by IP address.
REM Specify a comma-separated list of addresses or networks, for example:
REM   127.0.0.1, 192.168.0.0/24, [::1], [2000:123:4:5::]/64
REM set SOLR_IP_WHITELIST=

REM Block access to solr from specific IP addresses.
REM Specify a comma-separated list of addresses or networks, for example:
REM   127.0.0.1, 192.168.0.0/24, [::1], [2000:123:4:5::]/64
REM set SOLR_IP_BLACKLIST=

REM Enables HTTPS. It is implictly true if you set SOLR_SSL_KEY_STORE. Use this config
REM to enable https module with custom jetty configuration.
REM set SOLR_SSL_ENABLED=true
REM Uncomment to set SSL-related system properties
REM Be sure to update the paths to the correct keystore for your environment
REM set SOLR_SSL_KEY_STORE=etc/solr-ssl.keystore.p12
REM set SOLR_SSL_KEY_STORE_PASSWORD=secret
REM set SOLR_SSL_TRUST_STORE=etc/solr-ssl.keystore.p12
REM set SOLR_SSL_TRUST_STORE_PASSWORD=secret
REM Require clients to authenticate
REM set SOLR_SSL_NEED_CLIENT_AUTH=false
REM Enable clients to authenticate (but not require)
REM set SOLR_SSL_WANT_CLIENT_AUTH=false
REM Verify client hostname during SSL handshake
REM set SOLR_SSL_CLIENT_HOSTNAME_VERIFICATION=false
REM SSL Certificates contain host/ip "peer name" information that is validated by default. Setting
REM this to false can be useful to disable these checks when re-using a certificate on many hosts
REM set SOLR_SSL_CHECK_PEER_NAME=true
REM Override Key/Trust Store types if necessary
REM set SOLR_SSL_KEY_STORE_TYPE=PKCS12
REM set SOLR_SSL_TRUST_STORE_TYPE=PKCS12

REM Uncomment if you want to override previously defined SSL values for HTTP client
REM otherwise keep them commented and the above values will automatically be set for HTTP clients
REM set SOLR_SSL_CLIENT_KEY_STORE=
REM set SOLR_SSL_CLIENT_KEY_STORE_PASSWORD=
REM set SOLR_SSL_CLIENT_TRUST_STORE=
REM set SOLR_SSL_CLIENT_TRUST_STORE_PASSWORD=
REM set SOLR_SSL_CLIENT_KEY_STORE_TYPE=
REM set SOLR_SSL_CLIENT_TRUST_STORE_TYPE=

REM Sets path of Hadoop credential provider (hadoop.security.credential.provider.path property) and
REM enables usage of credential store.
REM Credential provider should store the following keys:
REM * solr.jetty.keystore.password
REM * solr.jetty.truststore.password
REM Set the two below if you want to set specific store passwords for HTTP client
REM * javax.net.ssl.keyStorePassword
REM * javax.net.ssl.trustStorePassword
REM More info: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/CredentialProviderAPI.html
REM set SOLR_HADOOP_CREDENTIAL_PROVIDER_PATH=localjceks://file/home/solr/hadoop-credential-provider.jceks
REM set SOLR_OPTS=%SOLR_OPTS% -Dsolr.ssl.credential.provider.chain=hadoop

REM Settings for authentication
REM Please configure only one of SOLR_AUTHENTICATION_CLIENT_BUILDER or SOLR_AUTH_TYPE parameters
REM set SOLR_AUTHENTICATION_CLIENT_BUILDER=org.apache.solr.client.solrj.impl.PreemptiveBasicAuthClientBuilderFactory
REM set SOLR_AUTH_TYPE=basic
REM set SOLR_AUTHENTICATION_OPTS=-Dbasicauth=solr:SolrRocks

REM Settings for ZK ACL
REM set SOLR_ZK_CREDS_AND_ACLS=-DzkACLProvider=org.apache.solr.common.cloud.VMParamsAllAndReadonlyDigestZkACLProvider ^
REM  -DzkCredentialsProvider=org.apache.solr.common.cloud.VMParamsSingleSetCredentialsDigestZkCredentialsProvider ^
REM  -DzkDigestUsername=admin-user -DzkDigestPassword=CHANGEME-ADMIN-PASSWORD ^
REM  -DzkDigestReadonlyUsername=readonly-user -DzkDigestReadonlyPassword=CHANGEME-READONLY-PASSWORD
REM set SOLR_OPTS=%SOLR_OPTS% %SOLR_ZK_CREDS_AND_ACLS%

REM When running Solr in non-cloud mode and if planning to do distributed search (using the "shards" parameter), the
REM list of hosts needs to be whitelisted or Solr will forbid the request. The whitelist can be configured in solr.xml,
REM or if you are using the OOTB solr.xml, can be specified using the system property "solr.shardsWhitelist". Alternatively
REM host checking can be disabled by using the system property "solr.disable.shardsWhitelist"
REM set SOLR_OPTS=%SOLR_OPTS% -Dsolr.shardsWhitelist=http://localhost:8983,http://localhost:8984

REM For a visual indication in the Admin UI of what type of environment this cluster is, configure
REM a -Dsolr.environment property below. Valid values are prod, stage, test, dev, with an optional
REM label or color, e.g. -Dsolr.environment=test,label=Functional+test,color=brown
REM set SOLR_OPTS=%SOLR_OPTS% -Dsolr.environment=prod

REM Specifies the path to a common library directory that will be shared across all cores.
REM Any JAR files in this directory will be added to the search path for Solr plugins.
REM If the specified path is not absolute, it will be relative to `%SOLR_HOME%`.
REM set SOLR_OPTS=%SOLR_OPTS% -Dsolr.sharedLib=/path/to/lib

REM Runs solr in a java security manager sandbox. This can protect against some attacks.
REM Runtime properties are passed to the security policy file (server\etc\security.policy)
REM You can also tweak via standard JDK files such as ~\.java.policy, see https://s.apache.org/java8policy
REM This is experimental! It may not work at all with Hadoop/HDFS features.
REM set SOLR_SECURITY_MANAGER_ENABLED=false

REM Solr is by default allowed to read and write data from/to SOLR_HOME and a few other well defined locations
REM Sometimes it may be necessary to place a core or a backup on a different location or a different disk
REM This parameter lets you specify file system path(s) to explicitly allow. The special value of '*' will allow any path
REM SOLR_OPTS="%SOLR_OPTS% -Dsolr.allowPaths=D:\,E:\other\path"
