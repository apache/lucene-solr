#!/usr/bin/env bash

cd ..
cp -r example example2
cp -r example example3
cp -r example example4
cp -r example example5

ant example dist

java -classpath lib/*:dist/* org.apache.solr.cloud.ZkController 127.0.0.1:9983 127.0.0.1 8983 solr ../example/solr/conf conf1 example/solr/zoo_data

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
