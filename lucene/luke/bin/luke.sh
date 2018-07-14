#!/bin/bash

LUKE_PATH=$(cd `dirname $0`/.. && pwd)
JAVA_OPTS="$JAVA_OPTS -Xmx512m -Xms512m"

nohup java ${JAVA_OPTS} -classpath "${LUKE_PATH}/lib/*" org.apache.lucene.luke.app.desktop.LukeMain > /dev/null 2>&1 &

