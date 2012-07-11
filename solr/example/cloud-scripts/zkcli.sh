#!/usr/bin/env bash

# You can override pass the following parameters to this script:
# 

JVM="java"

# Find location of this script

sdir="`dirname \"$0\"`"


$JVM  -classpath "$sdir/../solr-webapp/webapp/WEB-INF/lib/*" org.apache.solr.cloud.ZkCLI ${1+"$@"}

