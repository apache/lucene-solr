#!/bin/bash

# These scripts are best effort developer scripts. No promises.

# To run on hdfs, try something along the lines of:
# export JAVA_OPTS="-Dsolr.directoryFactory=solr.HdfsDirectoryFactory -Dsolr.lock.type=hdfs -Dsolr.hdfs.home=hdfs://localhost:8020/solr -Dsolr.hdfs.confdir=/etc/hadoop_conf/conf"

# To use ZooKeeper security, try:
# export JAVA_OPTS="-DzkACLProvider=org.apache.solr.common.cloud.VMParamsAllAndReadonlyDigestZkACLProvider -DzkCredentialsProvider=org.apache.solr.common.cloud.VMParamsSingleSetCredentialsDigestZkCredentialsProvider -DzkDigestUsername=admin-user -DzkDigestPassword=admin-password -DzkDigestReadonlyUsername=readonly-user -DzkDigestReadonlyPassword=readonly-password"
#
# To create a collection, curl "localhost:8901/solr/admin/collections?action=CREATE&name=collection1&numShards=2&replicationFactor=1&maxShardsPerNode=10"
# To add a document, curl http://localhost:8901/solr/collection1/update -H 'Content-type:application/json' -d '[{"id" : "book1"}]'

numServers=$1
numShards=$2

baseJettyPort=8900
baseStopPort=9900

zkAddress=localhost:9900/solr

die () {
    echo >&2 "$@"
    exit 1
}

[ "$#" -eq 1 ] || die "1 argument required, $# provided, usage: solrcloud-start.sh [numServers]"

cd ..

for (( i=1; i <= $numServers; i++ ))
do
 echo "try to remove existing directory: server$i"
 rm -r -f server$i
done


rm -r -f dist
rm -r -f build
rm -r -f server/solr/zoo_data
rm -f server/server.log

ant -f ../build.xml clean
ant server dist

rm -r server/solr-webapp/*
unzip server/webapps/solr.war -d server/solr-webapp/webapp

for (( i=1; i <= $numServers; i++ ))
do
 echo "create server$i"
 cp -r -f server server$i
done
  
rm -r -f serverzk
cp -r -f server serverzk
cp core/src/test-files/solr/solr-no-core.xml serverzk/solr/solr.xml
rm -r -f serverzk/solr/collection1/core.properties
cd serverzk
stopPort=1313
jettyPort=8900
exec -a jettyzk java -Xmx512m $JAVA_OPTS -Djetty.port=$jettyPort -DhostPort=$jettyPort -DzkRun=localhost:9900/solr -DzkHost=$zkAddress -DzkRunOnly=true -jar start.jar --module=http STOP.PORT=$stopPort STOP.KEY=key jetty.base=. 1>serverzk.log 2>&1 &
cd ..

# upload config files
java -classpath "server/solr-webapp/webapp/WEB-INF/lib/*:server/lib/ext/*" $JAVA_OPTS org.apache.solr.cloud.ZkCLI -zkhost $zkAddress -cmd upconfig --confdir server/solr/configsets/basic_configs/conf --confname basic_configs
  
cd server

for (( i=1; i <= $numServers; i++ ))
do
  echo "starting server$i"
  cd ../server$i
  stopPort=`expr $baseStopPort + $i`
  jettyPort=`expr $baseJettyPort + $i`
  exec -a jetty java -Xmx1g $JAVA_OPTS -Djetty.port=$jettyPort -DzkHost=$zkAddress -jar start.jar --module=http STOP.PORT=$stopPort STOP.KEY=key jetty.base=. 1>server$i.log 2>&1 &
done
