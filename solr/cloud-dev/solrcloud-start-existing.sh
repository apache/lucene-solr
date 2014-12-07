#!/bin/bash

numServers=$1

baseJettyPort=8900
baseStopPort=9900

ZK_CHROOT="solr"

die () {
    echo >&2 "$@"
    exit 1
}

[ "$#" -eq 1 ] || die "1 argument required, $# provided, usage: solrcloud-start-exisiting.sh [numServers]"


cd ..

cd serverzk
stopPort=1313
jettyPort=8900
exec -a jettyzk java -Xmx512m $JAVA_OPTS -Djetty.port=$jettyPort -DhostPort=$jettyPort -DzkRun -DzkHost=localhost:9900/$ZK_CHROOT -DzkRunOnly=true -DSTOP.PORT=$stopPort -DSTOP.KEY=key -jar start.jar 1>serverzk.log 2>&1 &

cd ..

cd server

for (( i=1; i <= $numServers; i++ ))
do
  echo "starting server$i"
  cd ../server$i
  stopPort=`expr $baseStopPort + $i`
  jettyPort=`expr $baseJettyPort + $i`
  exec -a jetty java -Xmx1g $JAVA_OPTS -Djetty.port=$jettyPort -DzkHost=localhost:9900/$ZK_CHROOT -DSTOP.PORT=$stopPort -DSTOP.KEY=key -jar start.jar 1>server$i.log 2>&1 &
done
