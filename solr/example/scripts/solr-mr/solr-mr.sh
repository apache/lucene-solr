#!/usr/bin/env bash

JVM="java"

# Find location of this script

sdir="`dirname \"$0\"`"

PATH=$JAVA_HOME/bin:$PATH $JVM -cp "$sdir/../../../dist/*:$sdir/../../../contrib/solr-mr/lib/*:$sdir/../../../contrib/solr-morphlines-core/lib/*:$sdir/../../../contrib/solr-morphlines-cell/lib/*:$sdir/../../../contrib/extraction/lib/*:$sdir/../../solr-webapp/solr/WEB-INF/lib/*:$sdir/../../lib/ext/*" org.apache.solr.hadoop.MapReduceIndexerTool ${1+"$@"}

