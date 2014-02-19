#!/bin/bash

numServers=$1
baseJettyPort=7572
baseStopPort=6572

die () {
    echo >&2 "$@"
    exit 1
}

[ "$#" -eq 1 ] || die "1 argument required, $# provided, usage: solrcloud-start-exisiting.sh {numServers}"


cd ..

cd example1
echo "starting example1"
java -DzkRun -DSTOP.PORT=7983 -DSTOP.KEY=key -jar start.jar 1>example1.log 2>&1 &


for (( i=2; i <= $numServers; i++ ))
do
  echo "starting example$i"
  cd ../example$i
  stopPort=`expr $baseStopPort + $i`
  jettyPort=`expr $baseJettyPort + $i`
  java -Xmx1g -Djetty.port=$jettyPort -DzkHost=localhost:9983 -DnumShards=1 -DSTOP.PORT=$stopPort -DSTOP.KEY=key -jar start.jar 1>example$i.log 2>&1 &
done
