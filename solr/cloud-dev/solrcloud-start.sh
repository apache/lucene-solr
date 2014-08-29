#!/bin/bash

# To run on hdfs, try something along the lines of:
# export JAVA_OPTS="-Dsolr.directoryFactory=solr.HdfsDirectoryFactory -Dsolr.lock.type=hdfs -Dsolr.hdfs.home=hdfs://localhost:8020/solr -Dsolr.hdfs.confdir=/etc/hadoop_conf/conf"

# To use ZooKeeper security, try:
# export JAVA_OPTS="-DzkACLProvider=org.apache.solr.common.cloud.VMParamsAllAndReadonlyDigestZkACLProvider -DzkCredentialsProvider=org.apache.solr.common.cloud.VMParamsSingleSetCredentialsDigestZkCredentialsProvider -DzkDigestUsername=admin-user -DzkDigestPassword=admin-password -DzkDigestReadonlyUsername=readonly-user -DzkDigestReadonlyPassword=readonly-password"

numServers=$1
numShards=$2

baseJettyPort=8900
baseStopPort=9900

zkAddress=localhost:9900/solr

die () {
    echo >&2 "$@"
    exit 1
}

[ "$#" -eq 2 ] || die "2 arguments required, $# provided, usage: solrcloud-start.sh [numServers] [numShards]"

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

ant -f ../build.xml clean
ant example dist

rm -r example/solr-webapp/*
unzip example/webapps/solr.war -d example/solr-webapp/webapp

for (( i=1; i <= $numServers; i++ ))
do
 echo "create example$i"
 cp -r -f example example$i
done
  

rm -r -f examplezk
cp -r -f example examplezk
cp core/src/test-files/solr/solr-no-core.xml examplezk/solr/solr.xml
rm -r -f examplezk/solr/collection1/core.properties
cd examplezk
stopPort=1313
jettyPort=8900
exec -a jettyzk java -Xmx512m $JAVA_OPTS -Djetty.port=$jettyPort -DhostPort=$jettyPort -DzkRun=localhost:9900/solr -DzkHost=$zkAddress -DzkRunOnly=true -DSTOP.PORT=$stopPort -DSTOP.KEY=key -jar start.jar 1>examplezk.log 2>&1 &
cd ..

# upload config files
java -classpath "example1/solr-webapp/webapp/WEB-INF/lib/*:example/lib/ext/*" $JAVA_OPTS org.apache.solr.cloud.ZkCLI -cmd bootstrap -zkhost $zkAddress -solrhome example1/solr
  
cd example

for (( i=1; i <= $numServers; i++ ))
do
  echo "starting example$i"
  cd ../example$i
  stopPort=`expr $baseStopPort + $i`
  jettyPort=`expr $baseJettyPort + $i`
  exec -a jetty java -Xmx1g $JAVA_OPTS -DnumShards=$numShards -Djetty.port=$jettyPort -DzkHost=$zkAddress -DSTOP.PORT=$stopPort -DSTOP.KEY=key -jar start.jar 1>example$i.log 2>&1 &
done



