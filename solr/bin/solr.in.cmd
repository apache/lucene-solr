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

REM By default the script will use JAVA_HOME to determine which java
REM to use, but you can set a specific path for Solr to use without
REM affecting other Java applications on your server/workstation.
REM set SOLR_JAVA_HOME=

REM Increase Java Min/Max Heap as needed to support your indexing / query needs
set SOLR_JAVA_MEM=-Xms512m -Xmx512m

REM Enable verbose GC logging
set GC_LOG_OPTS=-verbose:gc -XX:+PrintHeapAtGC -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps -XX:+PrintTenuringDistribution -XX:+PrintGCApplicationStoppedTime

REM These GC settings have shown to work well for a number of common Solr workloads
set GC_TUNE=-XX:NewRatio=3 ^
 -XX:SurvivorRatio=4 ^
 -XX:TargetSurvivorRatio=90 ^
 -XX:MaxTenuringThreshold=8 ^
 -XX:+UseConcMarkSweepGC ^
 -XX:+UseParNewGC ^
 -XX:ConcGCThreads=4 -XX:ParallelGCThreads=4 ^
 -XX:+CMSScavengeBeforeRemark ^
 -XX:PretenureSizeThreshold=64m ^
 -XX:+UseCMSInitiatingOccupancyOnly ^
 -XX:CMSInitiatingOccupancyFraction=50 ^
 -XX:CMSMaxAbortablePrecleanTime=6000 ^
 -XX:+CMSParallelRemarkEnabled ^
 -XX:+ParallelRefProcEnabled

REM Set the ZooKeeper connection string if using an external ZooKeeper ensemble
REM e.g. host1:2181,host2:2181/chroot
REM Leave empty if not using SolrCloud
REM set ZK_HOST=

REM Set the ZooKeeper client timeout (for SolrCloud mode)
REM set ZK_CLIENT_TIMEOUT=15000

REM By default the start script uses "localhost"; override the hostname here
REM for production SolrCloud environments to control the hostname exposed to cluster state
REM set SOLR_HOST=192.168.1.1

REM By default the start script uses UTC; override the timezone if needed
REM set SOLR_TIMEZONE=UTC

REM Set to true to activate the JMX RMI connector to allow remote JMX client applications
REM to monitor the JVM hosting Solr; set to "false" to disable that behavior
REM (false is recommended in production environments)
set ENABLE_REMOTE_JMX_OPTS=false

REM The script will use SOLR_PORT+10000 for the RMI_PORT or you can set it here
REM set RMI_PORT=18983

REM Set the host interface to listen on. Jetty will listen on all interfaces (0.0.0.0) by default.
REM This must be an IPv4 ("a.b.c.d") or bracketed IPv6 ("[x::y]") address, not a hostname!
set SOLR_JETTY_HOST=0.0.0.0

set SOLR_OPTS=%SOLR_OPTS% -Djetty.host=%SOLR_JETTY_HOST%

REM Set the thread stack size
set SOLR_OPTS=%SOLR_OPTS% -Xss256k

REM Anything you add to the SOLR_OPTS variable will be included in the java
REM start command line as-is, in ADDITION to other options. If you specify the
REM -a option on start script, those options will be appended as well. Examples:
REM set SOLR_OPTS=%SOLR_OPTS% -Dsolr.autoSoftCommit.maxTime=3000
REM set SOLR_OPTS=%SOLR_OPTS% -Dsolr.autoCommit.maxTime=60000
REM set SOLR_OPTS=%SOLR_OPTS% -Dsolr.clustering.enabled=true

REM Path to a directory for Solr to store cores and their data. By default, Solr will use server\solr
REM If solr.xml is not stored in ZooKeeper, this directory needs to contain solr.xml
REM set SOLR_HOME=

REM Sets the port Solr binds to, default is 8983
REM set SOLR_PORT=8983

REM Uncomment to set SSL-related system properties
REM Be sure to update the paths to the correct keystore for your environment
REM set SOLR_SSL_KEY_STORE=etc/solr-ssl.keystore.jks
REM set SOLR_SSL_KEY_STORE_PASSWORD=secret
REM set SOLR_SSL_TRUST_STORE=etc/solr-ssl.keystore.jks
REM set SOLR_SSL_TRUST_STORE_PASSWORD=secret
REM set SOLR_SSL_NEED_CLIENT_AUTH=false
REM set SOLR_SSL_WANT_CLIENT_AUTH=false

REM Uncomment if you want to override previously defined SSL values for HTTP client
REM otherwise keep them commented and the above values will automatically be set for HTTP clients
REM set SOLR_SSL_CLIENT_KEY_STORE=
REM set SOLR_SSL_CLIENT_KEY_STORE_PASSWORD=
REM set SOLR_SSL_CLIENT_TRUST_STORE=
REM setSOLR_SSL_CLIENT_TRUST_STORE_PASSWORD=

REM Settings for ZK ACL
REM set SOLR_ZK_CREDS_AND_ACLS=-DzkACLProvider=org.apache.solr.common.cloud.VMParamsAllAndReadonlyDigestZkACLProvider ^
REM  -DzkCredentialsProvider=org.apache.solr.common.cloud.VMParamsSingleSetCredentialsDigestZkCredentialsProvider ^
REM  -DzkDigestUsername=admin-user -DzkDigestPassword=CHANGEME-ADMIN-PASSWORD ^
REM  -DzkDigestReadonlyUsername=readonly-user -DzkDigestReadonlyPassword=CHANGEME-READONLY-PASSWORD
REM set SOLR_OPTS=%SOLR_OPTS% %SOLR_ZK_CREDS_AND_ACLS%
