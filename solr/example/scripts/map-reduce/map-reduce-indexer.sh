#!/usr/bin/env bash

JVM="java"

# Find location of this script

sdir="`dirname \"$0\"`"

PATH=$JAVA_HOME/bin:$PATH $JVM -cp "$sdir/../../../dist/*:$sdir/../../../contrib/map-reduce/lib/*:$sdir/../../../contrib/morphlines-core/lib/*:$sdir/../../../contrib/morphlines-cell/lib/*:$sdir/../../../contrib/extraction/lib/*:$sdir/../../solr-webapp/webapp/WEB-INF/lib/*:$sdir/../../lib/ext/*" org.apache.solr.hadoop.MapReduceIndexerTool ${1+"$@"}

