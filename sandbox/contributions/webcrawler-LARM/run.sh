#!/bin/sh

#
# $Id$
#

BASE_DIR=./runtime
LOG_DIR=$BASE_DIR/logs
CACHE_DIR=$BASE_DIR/cachingqueue
CLASSPATH=build/classes:libs/jakarta-oro-2.0.5.jar:libs/HTTPClient.zip:/usr/local/jakarta-lucene/lucene.jar
SLEEP_TIME=2

if [ $# -lt 4 ]
then
    echo "Usage: `basename $0` <start url> <score regex> <# threads> <max mem>" >&2
    exit 1
fi

START_URL=$1
SCOPE_REGEX=$2
THREAD_COUNT=$3
MAX_MEM=$4


echo Removing $LOG_DIR...
sleep $SLEEP_TIME
rm -r $LOG_DIR
echo Removing $CACHE_DIR...
sleep $SLEEP_TIME
rm -r $CACHE_DIR
echo Creating $LOG_DIR
sleep $SLEEP_TIME
mkdir -p $LOG_DIR
echo Creating $CACHE_DIR
sleep $SLEEP_TIME
mkdir -p $CACHE_DIR

CMD="java -server -Xmx$MAX_MEM -classpath $CLASSPATH de.lanlab.larm.fetcher.FetcherMain -start $START_URL -restrictto $SCOPE_REGEX -threads $THREAD_COUNT"
echo Starting LARM with: $CMD

$CMD
