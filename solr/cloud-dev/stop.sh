#!/bin/bash

numServers=$1
baseStopPort=6572

die () {
    echo >&2 "$@"
    exit 1
}

[ "$#" -eq 1 ] || die "1 argument required, $# provided, usage: stop.sh {numServers}"

cd ../example

java -DSTOP.PORT=7983 -DSTOP.KEY=key -jar start.jar --stop


for (( i=2; i <= $numServers; i++ ))
do
  echo "stopping example$i"
  cd ../example$i
  stopPort=`expr $baseStopPort + $i`
  java -DSTOP.PORT=$stopPort -DSTOP.KEY=key -jar start.jar --stop
done
