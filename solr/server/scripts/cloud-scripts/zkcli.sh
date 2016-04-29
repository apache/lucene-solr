#!/usr/bin/env bash

# You can override pass the following parameters to this script:
# 

JVM="java"

# Find location of this script

sdir="`dirname \"$0\"`"

if [ -n "$LOG4J_PROPS" ]; then
  log4j_config="file:$LOG4J_PROPS"
else
  log4j_config="file:$sdir/log4j.properties"
fi

PATH=$JAVA_HOME/bin:$PATH $JVM -Dlog4j.configuration=$log4j_config -classpath "$sdir/../../solr-webapp/webapp/WEB-INF/lib/*:$sdir/../../lib/ext/*" org.apache.solr.cloud.ZkCLI ${1+"$@"}

