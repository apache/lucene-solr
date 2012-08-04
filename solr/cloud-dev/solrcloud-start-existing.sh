#!/usr/bin/env bash

cd ..

cd example
java -DzkRun -DSTOP.PORT=7983 -DSTOP.KEY=key -jar start.jar 1>example.log 2>&1 &

cd ../example2
java -Djetty.port=7574 -DzkHost=localhost:9983 -DSTOP.PORT=6574 -DSTOP.KEY=key -jar start.jar 1>example2.log 2>&1 &

cd ../example3
java -Djetty.port=7575 -DzkHost=localhost:9983 -DSTOP.PORT=6575 -DSTOP.KEY=key -jar start.jar 1>example3.log 2>&1 &

cd ../example4
java -Djetty.port=7576 -DzkHost=localhost:9983 -DSTOP.PORT=6576 -DSTOP.KEY=key -jar start.jar 1>example4.log 2>&1 &

cd ../example5
java -Djetty.port=7577 -DzkHost=localhost:9983 -DSTOP.PORT=6577 -DSTOP.KEY=key -jar start.jar 1>example5.log 2>&1 &

cd ../example6
java -Djetty.port=7578 -DzkHost=localhost:9983 -DSTOP.PORT=6578 -DSTOP.KEY=key -jar start.jar 1>example6.log 2>&1 &
