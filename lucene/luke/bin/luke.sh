#!/bin/bash

LUKE_HOME=$(cd $(dirname $0) && pwd)
cd ${LUKE_HOME}

JAVA_OPTIONS="${JAVA_OPTIONS} -Xmx1024m -Xms512m -XX:MaxMetaspaceSize=256m"

CLASSPATHS="./*:./lib/*:../core/*:../codecs/*:../backward-codecs/*:../queries/*:../queryparser/*:../suggest/*:../misc/*"
for dir in `ls ../analysis`; do
  CLASSPATHS="${CLASSPATHS}:../analysis/${dir}/*:../analysis/${dir}/lib/*"
done

LOG_DIR=${HOME}/.luke.d/
 if [[ ! -d ${LOG_DIR} ]]; then
   mkdir ${LOG_DIR}
 fi

nohup java -cp ${CLASSPATHS} ${JAVA_OPTIONS} org.apache.lucene.luke.app.desktop.LukeMain > ${LOG_DIR}/luke_out.log 2>&1 &
