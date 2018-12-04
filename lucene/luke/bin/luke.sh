#!/bin/bash

ADD_OPENS_OPTION="--add-opens java.base/java.lang=ALL-UNNAMED"
java ${ADD_OPENS_OPTION} -version > /dev/null 2>&1
if [[ $? -eq 0 ]]; then
  # running on jdk9+
  JAVA_OPTIONS="${ADD_OPENS_OPTION}"
else
  # running on jdk8
  JAVA_OPTIONS=""
fi

LUKE_HOME=$(cd $(dirname $0) && pwd)
cd ${LUKE_HOME}

JAVA_OPTIONS="${JAVA_OPTIONS} -Xmx1024m -Xms512m -XX:MaxMetaspaceSize=256m"

CLASSPATHS="./*:./lib/*:../core/*:../codecs/*:../backward-codecs/*:../queries/*:../queryparser/*:../misc/*"
for dir in `ls ../analysis`; do
  CLASSPATHS="${CLASSPATHS}:../analysis/${dir}/*:../analysis/${dir}/lib/*"
done

nohup java -cp ${CLASSPATHS} ${JAVA_OPTIONS} org.apache.lucene.luke.app.desktop.LukeMain > ${HOME}/.luke.d/luke_out.log 2>&1 &
