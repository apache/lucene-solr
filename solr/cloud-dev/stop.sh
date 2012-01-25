#!/usr/bin/env bash

cd ../example

java -DSTOP.PORT=7983 -DSTOP.KEY=key -jar start.jar --stop
java -DSTOP.PORT=6574 -DSTOP.KEY=key -jar start.jar --stop
java -DSTOP.PORT=6575 -DSTOP.KEY=key -jar start.jar --stop
java -DSTOP.PORT=6576 -DSTOP.KEY=key -jar start.jar --stop
java -DSTOP.PORT=6577 -DSTOP.KEY=key -jar start.jar --stop
java -DSTOP.PORT=6578 -DSTOP.KEY=key -jar start.jar --stop