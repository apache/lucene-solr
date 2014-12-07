#!/usr/bin/env bash

# TODO: !OUT OF DATE!

cd ..

rm -r -f server2
rm -r -f server3
rm -r -f server4
rm -r -f server5
rm -r -f server6

rm -r -f dist
rm -r -f build
rm -r -f server/solr/zoo_data
rm -r -f server/solr/data
rm -f server/server.log

ant example dist

cp -r -f server server2
cp -r -f server server3
cp -r -f server server4
cp -r -f server server5
cp -r -f server server6

# first try uploading a conf dir
java -classpath lib/*:dist/*:build/lucene-libs/* org.apache.solr.cloud.ZkCLI -cmd upconfig -zkhost 127.0.0.1:9983 -confdir server/solr/collection1/conf -confname conf1 -solrhome server/solr -runzk 8983

# upload a second conf set so we avoid single conf auto linking
java -classpath lib/*:dist/*:build/lucene-libs/* org.apache.solr.cloud.ZkCLI -cmd upconfig -zkhost 127.0.0.1:9983 -confdir server/solr/collection1/conf -confname conf2 -solrhome server/solr -runzk 8983

# now try linking a collection to a conf set
java -classpath lib/*:dist/*:build/lucene-libs/* org.apache.solr.cloud.ZkCLI -cmd linkconfig -zkhost 127.0.0.1:9983 -collection collection1 -confname conf1 -solrhome server/solr -runzk 8983


cd server
java -DzkRun -DnumShards=2 -DSTOP.PORT=7983 -DSTOP.KEY=key -jar start.jar 1>server.log 2>&1 &

cd ../server2
java -Djetty.port=7574 -DzkHost=localhost:9983 -DnumShards=2 -DSTOP.PORT=6574 -DSTOP.KEY=key -jar start.jar 1>server2.log 2>&1 &

cd ../server3
java -Djetty.port=7575 -DzkHost=localhost:9983 -DnumShards=2 -DSTOP.PORT=6575 -DSTOP.KEY=key -jar start.jar 1>server3.log 2>&1 &

cd ../server4
java -Djetty.port=7576 -DzkHost=localhost:9983 -DnumShards=2 -DSTOP.PORT=6576 -DSTOP.KEY=key -jar start.jar 1>server4.log 2>&1 &

cd ../server5
java -Djetty.port=7577 -DzkHost=localhost:9983 -DnumShards=2 -DSTOP.PORT=6577 -DSTOP.KEY=key -jar start.jar 1>server5.log 2>&1 &

cd ../server6
java -Djetty.port=7578 -DzkHost=localhost:9983 -DnumShards=2 -DSTOP.PORT=6578 -DSTOP.KEY=key -jar start.jar 1>server6.log 2>&1 &
