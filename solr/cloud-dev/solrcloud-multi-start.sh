#!/bin/bash

# TODO: !OUT OF DATE!

# starts up the multicore example

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

java -classpath "server/solr-webapp/webapp/WEB-INF/lib/*:server/lib/ext/" org.apache.solr.cloud.ZkCLI -cmd upconf -zkhost 127.0.0.1:9983 -solrhome example/multicore -runzk 8983

cd server
java -DzkRun -DnumShards=2 -DSTOP.PORT=7983 -DSTOP.KEY=key -Dsolr.solr.home=../example/multicore -jar start.jar 1>server.log 2>&1 &

cd ../server2
java -Djetty.port=7574 -DzkHost=localhost:9983 -DnumShards=2 -DSTOP.PORT=6574 -DSTOP.KEY=key -Dsolr.solr.home=multicore -jar start.jar 1>server2.log 2>&1 &

cd ../server3
java -Djetty.port=7575 -DzkHost=localhost:9983 -DnumShards=2 -DSTOP.PORT=6575 -DSTOP.KEY=key -Dsolr.solr.home=multicore -jar start.jar 1>server3.log 2>&1 &

cd ../server4
java -Djetty.port=7576 -DzkHost=localhost:9983 -DnumShards=2 -DSTOP.PORT=6576 -DSTOP.KEY=key -Dsolr.solr.home=multicore -jar start.jar 1>server4.log 2>&1 &

cd ../server5
java -Djetty.port=7577 -DzkHost=localhost:9983 -DnumShards=2 -DSTOP.PORT=6577 -DSTOP.KEY=key -Dsolr.solr.home=multicore -jar start.jar 1>server5.log 2>&1 &

cd ../server6
java -Djetty.port=7578 -DzkHost=localhost:9983 -DnumShards=2 -DSTOP.PORT=6578 -DSTOP.KEY=key -Dsolr.solr.home=multicore -jar start.jar 1>server6.log 2>&1 &
