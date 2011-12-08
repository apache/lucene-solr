#!/usr/bin/env bash

cd ..

rm -r -f example2
rm -r -f example3
rm -r -f example4
rm -r -f example5
rm -r -f example6

cp -r example example2
cp -r example example3
cp -r example example4
cp -r example example5
cp -r example example6

rm -r -f dist
rm -r -f build
rm -r -f example/solr/zoo_data

ant example dist

java -classpath lib/*:dist/*:build/lucene-libs/* org.apache.solr.cloud.ZkController 127.0.0.1:9983 example/solr 8983 example/solr/conf conf1

cd example
java -DzkRun -jar start.jar &

cd ../example2
java -Djetty.port=7574 -DzkHost=localhost:9983 -jar start.jar &

cd ../example3
java -Djetty.port=7575 -DzkHost=localhost:9983 -jar start.jar &

cd ../example4
java -Djetty.port=7576 -DzkHost=localhost:9983 -jar start.jar &

cd ../example5
java -Djetty.port=7577 -DzkHost=localhost:9983 -jar start.jar &

cd ../example6
java -Djetty.port=7578 -DzkHost=localhost:9983 -jar start.jar &
