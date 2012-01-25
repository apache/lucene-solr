#!/usr/bin/env bash

cd ..

rm -r -f example2
rm -r -f example3
rm -r -f example4

rm -r -f dist
rm -r -f build
rm -r -f example/solr/zoo_data
rm -f example/example.log

ant example dist

cp -r -f example example2
cp -r -f example example3
cp -r -f example example4


cd example
java -DzkRun -DnumShards=2 -DSTOP.PORT=7983 -DSTOP.KEY=key -Dbootstrap_confdir=solr/conf -DzkHost=localhost:9983,localhost:14574,localhost:14585 -jar start.jar 1>example.log 2>&1 &

sleep 10

cd ../example2
java -Djetty.port=13574 -DzkRun -DzkHost=localhost:9983,localhost:14574,localhost:14575 -DnumShards=2 -DSTOP.PORT=6574 -DSTOP.KEY=key -jar start.jar 1>example2.log 2>&1 &

cd ../example3
java -Djetty.port=13585 -DzkRun -DzkHost=localhost:9983,localhost:14574,localhost:14585 -DnumShards=2 -DSTOP.PORT=6575 -DSTOP.KEY=key -jar start.jar 1>example3.log 2>&1 &

cd ../example4
java -Djetty.port=13596 -DzkHost=localhost:9983,localhost:14574,localhost:14585 -DnumShards=2 -DSTOP.PORT=6576 -DSTOP.KEY=key -jar start.jar 1>example4.log 2>&1 &
