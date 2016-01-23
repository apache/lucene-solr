#!/usr/bin/env bash

######################################################################
#
# Running this script will set two environment variables:
# HADOOP_CLASSPATH
# HADOOP_LIBJAR: pass this to the -libjar MapReduceIndexBuilder option
#
######################################################################

# return absolute path
function absPath {
  echo $(cd $(dirname "$1"); pwd)/$(basename "$1")
}


# Find location of this script

sdir="`cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd`"

solr_distrib="$sdir/../../.."

echo `absPath $solr_distrib`

# Setup env variables for MapReduceIndexerTool

# Setup HADOOP_CLASSPATH

dir1=`absPath "$solr_distrib/dist"`
dir2=`absPath "$solr_distrib/dist/solrj-lib"`
dir3=`absPath "$solr_distrib/contrib/map-reduce/lib"`
dir4=`absPath "$solr_distrib/contrib/morphlines-core/lib"`
dir5=`absPath "$solr_distrib/contrib/morphlines-cell/lib"`
dir6=`absPath "$solr_distrib/contrib/extraction/lib"`
dir7=`absPath "$solr_distrib/server/solr-webapp/webapp/WEB-INF/lib"`

# Setup -libjar

lib1=`ls -m $dir1/*.jar | tr -d ' \n'`
lib2=`ls -m $dir2/*.jar | tr -d ' \n' | sed 's/\,[^\,]*\(log4j\|slf4j\)[^\,]*//g'`
lib3=`ls -m $dir3/*.jar | tr -d ' \n'`
lib4=`ls -m $dir4/*.jar | tr -d ' \n'`
lib5=`ls -m $dir5/*.jar | tr -d ' \n'`
lib6=`ls -m $dir6/*.jar | tr -d ' \n'`
lib7=`ls -m $dir7/*.jar | tr -d ' \n'`

export HADOOP_CLASSPATH="$dir1/*:$dir2/*:$dir3/*:$dir4/*:$dir5/*:$dir6/*:$dir7/*"
export HADOOP_LIBJAR="$lib1,$lib2,$lib3,$lib4,$lib5,$lib6,$lib7"

#echo $HADOOP_CLASSPATH
#echo $HADOOP_LIBJAR

