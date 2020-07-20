#!/bin/bash

tgzfile=apache-zookeeper-3.5.8-bin
if [ ! -d "${tgzfile}" ]; then
  wget http://apache.mirrors.hoobly.com/zookeeper/zookeeper-3.5.8/${tgzfile}.tar.gz
  tar -xzvf ${tgzfile}.tar.gz
fi

export ZOO_LOG_DIR="logs"

echo '1' > ${tgzfile}/myid
echo -e "tickTime = 2000\ndataDir = ../data\nclientPort = 2181\ninitLimit = 5\nsyncLimit = 2" > ${tgzfile}/conf/zoo.cfg
cd ${tgzfile}
bin/zkServer.sh start

  tgzfile=apache-zookeeper-3.5.8-bin
  if [ ! -d "${tgzfile}" ]; then
    wget http://apache.mirrors.hoobly.com/zookeeper/zookeeper-3.5.8/${tgzfile}.tar.gz
    tar -xzvf ${tgzfile}.tar.gz
  fi

  export ZOO_LOG_DIR="logs"

  echo '1' > ${tgzfile}/myid
  echo -e "tickTime = 2000\ndataDir = ../data\nclientPort = 2181\ninitLimit = 5\nsyncLimit = 2" > ${tgzfile}/conf/zoo.cfg
  cd ${tgzfile}
  bin/zkServer.sh start &