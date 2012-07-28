#!/usr/bin/env bash

cd ..

rm -r -f example2
rm -r -f example3
rm -r -f example4
rm -r -f example5
rm -r -f example6

rm -r -f dist
rm -r -f build
rm -r -f example/solr/zoo_data
rm -r -f example/solr/data
rm -f example/example.log

ant example dist

cp -r -f example example2
cp -r -f example example3
cp -r -f example example4
cp -r -f example example5
cp -r -f example example6

# first try uploading a conf dir
java -classpath lib/*:dist/*:build/lucene-libs/* org.apache.solr.cloud.ZkCLI -cmd upconfig -zkhost 127.0.0.1:9983 -confdir example/solr/collection1/conf -confname conf1 -solrhome example/solr -runzk 8983

# upload a second conf set so we avoid single conf auto linking
java -classpath lib/*:dist/*:build/lucene-libs/* org.apache.solr.cloud.ZkCLI -cmd upconfig -zkhost 127.0.0.1:9983 -confdir example/solr/collection1/conf -confname conf2 -solrhome example/solr -runzk 8983

# now try linking a collection to a conf set
java -classpath lib/*:dist/*:build/lucene-libs/* org.apache.solr.cloud.ZkCLI -cmd linkconfig -zkhost 127.0.0.1:9983 -collection collection1 -confname conf1 -solrhome example/solr -runzk 8983


cd example
java -DzkRun -DnumShards=2 -DSTOP.PORT=7983 -DSTOP.KEY=key -jar start.jar 1>example.log 2>&1 &

cd ../example2
java -Djetty.port=7574 -DzkHost=localhost:9983 -DnumShards=2 -DSTOP.PORT=6574 -DSTOP.KEY=key -jar start.jar 1>example2.log 2>&1 &

cd ../example3
java -Djetty.port=7575 -DzkHost=localhost:9983 -DnumShards=2 -DSTOP.PORT=6575 -DSTOP.KEY=key -jar start.jar 1>example3.log 2>&1 &

cd ../example4
java -Djetty.port=7576 -DzkHost=localhost:9983 -DnumShards=2 -DSTOP.PORT=6576 -DSTOP.KEY=key -jar start.jar 1>example4.log 2>&1 &

cd ../example5
java -Djetty.port=7577 -DzkHost=localhost:9983 -DnumShards=2 -DSTOP.PORT=6577 -DSTOP.KEY=key -jar start.jar 1>example5.log 2>&1 &

cd ../example6
java -Djetty.port=7578 -DzkHost=localhost:9983 -DnumShards=2 -DSTOP.PORT=6578 -DSTOP.KEY=key -jar start.jar 1>example6.log 2>&1 &
