#!/bin/bash

JAVA_OPTS="$JAVA_OPTS -Xmx512m -Xms512m"

if [[ -d `echo $LUKE_PATH` ]]; then
  nohup java ${JAVA_OPTS} -classpath "${LUKE_PATH}/lib/*" org.apache.lucene.luke.app.desktop.LukeMain > /dev/null 2&>1 &
else
  echo "Unable to find the LUKE_PATH environnement variable..."
  echo "Assuming you're running from the root folder of luke..."
  nohup java ${JAVA_OPTS} -classpath "./lib/*" org.apache.lucene.luke.app.desktop.LukeMain > /dev/null 2&>1 &
fi
