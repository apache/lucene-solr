#!/bin/bash

numServers=$1
baseJettyPort=8900
baseStopPort=9900

die () {
    echo >&2 "$@"
    exit 1
}

[ "$#" -eq 1 ] || die "1 argument required, $# provided, usage: stop.sh {numServers}"

cd ../server

for (( i=1; i <= $numServers; i++ ))
do
  stopPort=`expr $baseStopPort + $i`
  echo "stopping server$i, stop port is $stopPort"
  cd ../server$i
  java -jar start.jar --module=http STOP.PORT=$stopPort STOP.KEY=key --stop
done


mkdir ../server-lastlogs

for (( i=1; i <= $numServers; i++ ))
do
   cd ../server$i

  jettyPort=`expr $baseJettyPort + $i`
  echo "Make sure jetty stops and wait for it: $jettyPort"

  pid=`lsof -i:$jettyPort -sTCP:LISTEN -t`
  echo "pid:$pid"
  #kill $pid
  #wait $pid
  if [ ! -z "$pid" ]
  then
    while [ -e /proc/$pid ]; do sleep 1; done
  fi
  
  # save the last shutdown logs
  echo "copy server$i.log to lastlogs"
  cp -r -f server$i.log ../server-lastlogs/server-last$i.log
done

# stop zk runner
java -jar start.jar --module=http STOP.PORT=1313 STOP.KEY=key --stop

echo "wait for port to be available: $baseJettyPort"

pid=`lsof -i:$baseJettyPort -sTCP:LISTEN -t`
echo "pid:$pid"
#kill $pid
#wait $pid
if [ ! -z "$pid" ]
then
  while [ -e /proc/$pid ]; do sleep 0.1; done
fi
nc -w 30 127.0.0.1 $baseJettyPort

sleep 5
 