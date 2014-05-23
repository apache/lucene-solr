#!/bin/bash

numServers=$1
numShards=$2

baseJettyPort=7572
baseStopPort=6572

zkaddress = localhost:2181/solr

die () {
    echo >&2 "$@"
    exit 1
}

[ "$#" -eq 2 ] || die "2 arguments required, $# provided, usage: solrcloud-start.sh {numServers} {numShards}"

cd ..

for (( i=1; i <= $numServers; i++ ))
do
 rm -r -f example$i
done


rm -r -f dist
rm -r -f build
rm -r -f example/solr/zoo_data
rm -r -f example/solr/collection1/data
rm -f example/example.log

ant example dist

rm -r example/solr-webapp/*
unzip example/webapps/solr.war -d example/solr-webapp/webapp

for (( i=1; i <= $numServers; i++ ))
do
 echo "create example$i"
 cp -r -f example example$i
done


java -classpath "example1/solr-webapp/webapp/WEB-INF/lib/*:example/lib/ext/*" org.apache.solr.cloud.ZkCLI -cmd bootstrap -zkhost 127.0.0.1:9983 -solrhome example1/solr -runzk 8983

echo "starting example1"

cd example1
java -Xmx1g -DzkRun -DnumShards=$numShards -DSTOP.PORT=7983 -DSTOP.KEY=key -jar start.jar 1>example1.log 2>&1 &



for (( i=2; i <= $numServers; i++ ))
do
  echo "starting example$i"
  cd ../example$i
  stopPort=`expr $baseStopPort + $i`
  jettyPort=`expr $baseJettyPort + $i`
  java -Xmx1g -Djetty.port=$jettyPort -DzkHost=localhost:9983 -DnumShards=1 -DSTOP.PORT=$stopPort -DSTOP.KEY=key -jar start.jar 1>example$i.log 2>&1 &
done



