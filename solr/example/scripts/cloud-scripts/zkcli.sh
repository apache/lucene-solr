#!/usr/bin/env bash

# You can override pass the following parameters to this script:
# 

JVM="java"

# Find location of this script

sdir="`dirname \"$0\"`"

if [ ! -d "$sdir/../../solr-webapp/webapp" ]; then
  unzip $sdir/../../webapps/solr.war -d $sdir/../../solr-webapp/webapp
fi

PATH=$JAVA_HOME/bin:$PATH $JVM -Dlog4j.configuration=file:$sdir/log4j.properties -classpath "$sdir/../../solr-webapp/webapp/WEB-INF/lib/*:$sdir/../../lib/ext/*" org.apache.solr.cloud.ZkCLI ${1+"$@"}

