#!/bin/sh

BASE_DIR=./runtime
LOG_DIR=$BASE_DIR/logs
CACHE_DIR=$BASE_DIR/cachingqueue
SLEEP_TIME=2

echo Removing $LOG_DIR...
sleep $SLEEP_TIME
rm -r $LOG_DIR
echo Removing $CACHE_DIR...
sleep $SLEEP_TIME
rm -r $CACHE_DIR
