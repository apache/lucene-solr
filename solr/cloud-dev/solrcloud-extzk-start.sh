#!/bin/bash

# TODO: !OUT OF DATE!

zkaddress = localhost:2181/solr

cd ..

rm -r -f server2
rm -r -f server3
rm -r -f server4
rm -r -f server5
rm -r -f server6

rm -r -f dist
rm -r -f build
rm -r -f server/solr/zoo_data
rm -r -f server/solr/collection1/data
rm -f server/server.log

ant server dist

cp -r -f server server2
cp -r -f server server3
cp -r -f server server4
cp -r -f server server5
cp -r -f server server6

java -classpath "server/solr-webapp/webapp/WEB-INF/lib/*:server/lib/ext/" org.apache.solr.cloud.ZkController "$zkaddress" 8983 server/solr/conf conf1

cd server
java -DzkHost="$zkaddress" -DnumShards=2 -DSTOP.PORT=7983 -DSTOP.KEY=key -jar start.jar 1>server.log 2>&1 &

cd ../server2
java -Djetty.port=7574 -DzkHost="$zkaddress" -DSTOP.PORT=6574 -DSTOP.KEY=key -jar start.jar 1>server2.log 2>&1 &

cd ../server3
java -Djetty.port=7575 -DzkHost="$zkaddress" -DSTOP.PORT=6575 -DSTOP.KEY=key -jar start.jar 1>server3.log 2>&1 &

cd ../server4
java -Djetty.port=7576 -DzkHost="$zkaddress" -DSTOP.PORT=6576 -DSTOP.KEY=key -jar start.jar 1>server4.log 2>&1 &

cd ../server5
java -Djetty.port=7577 -DzkHost="$zkaddress" -DSTOP.PORT=6577 -DSTOP.KEY=key -jar start.jar 1>server5.log 2>&1 &

cd ../server6
java -Djetty.port=7578 -DzkHost="$zkaddress" -DSTOP.PORT=6578 -DSTOP.KEY=key -jar start.jar 1>server6.log 2>&1 &
