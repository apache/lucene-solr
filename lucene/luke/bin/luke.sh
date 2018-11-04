#!/bin/bash

JAVA_OPTIONS="-Xmx1024m -Xms512m -XX:MaxMetaspaceSize=256m"
if [[ ! -d `echo $LUKE_PATH` ]]; then
  LUKE_PATH=$(cd $(dirname $0) && pwd)
  echo "Unable to find the LUCENE_PATH environment variable."
  echo "Set LUKE_PATH to $LUKE_PATH"
fi

cd ${LUKE_PATH}

CLASSPATHS="./*:./lib/*:../core/*:../codecs/*:../backward-codecs/*:../queries/*:../queryparser/*:../misc/*"
for dir in `ls ../analysis`; do
  CLASSPATHS="${CLASSPATHS}:../analysis/${dir}/*:../analysis/${dir}/lib/*"
done

java -cp ${CLASSPATHS} ${JAVA_OPTIONS} org.apache.lucene.luke.app.desktop.LukeMain > ${HOME}/.luke.d/luke_out.log 2>&1 &
