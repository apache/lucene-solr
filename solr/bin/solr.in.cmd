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
set SOLR_JAVA_MEM=-Xms512m -Xmx512m -XX:MaxPermSize=256m -XX:PermSize=256m

REM Enable verbose GC logging
set GC_LOG_OPTS=-verbose:gc -XX:+PrintHeapAtGC -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps -XX:+PrintTenuringDistribution -XX:+PrintGCApplicationStoppedTime

REM These GC settings have shown to work well for a number of common Solr workloads
set GC_TUNE=-XX:-UseSuperWord ^
 -XX:NewRatio=3 ^
 -XX:SurvivorRatio=4 ^
 -XX:TargetSurvivorRatio=90 ^
 -XX:MaxTenuringThreshold=8 ^
 -XX:+UseConcMarkSweepGC ^
 -XX:+UseParNewGC ^
 -XX:ConcGCThreads=4 -XX:ParallelGCThreads=4 ^
 -XX:+CMSScavengeBeforeRemark ^
 -XX:PretenureSizeThreshold=64m ^
 -XX:CMSFullGCsBeforeCompaction=1 ^
 -XX:+UseCMSInitiatingOccupancyOnly ^
 -XX:CMSInitiatingOccupancyFraction=50 ^
 -XX:CMSTriggerPermRatio=80 ^
 -XX:CMSMaxAbortablePrecleanTime=6000 ^
 -XX:+CMSParallelRemarkEnabled ^
 -XX:+ParallelRefProcEnabled ^
 -XX:+AggressiveOpts

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

REM By default the start script enables some RMI related parameters to allow attaching
REM JMX savvy tools like VisualVM remotely, set to "false" to disable that behavior
REM (recommended in production environments)
set ENABLE_REMOTE_JMX_OPTS=true

