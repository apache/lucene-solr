INT_JAVA_OPTS="-server -Xms256M -Xmx256M"
BASE_PORT=8900
BASE_STOP_PORT=9900
ZK_PORT="2414"

rebuild() {
	echo "Rebuilding"
	cd ..
	rm -r -f dist
	rm -r -f build
	rm -r -f example/solr/zoo_data
	rm -f example/example.log
	ant example dist
}

setports() {
  PORT="$(( $BASE_PORT + $1 ))"
  STOP_PORT="$(( $BASE_STOP_PORT + $1 ))"
}

reinstall() {
	echo "Reinstalling instance $1"
	cd ..
	rm -rf  example$1
	cp -r -f example example$1
}

start() {
	OPT="-DzkHost=localhost:$ZK_PORT -DzkRun"
	NUMSHARDS=$2

	echo "Starting instance $1"
	if [ "1" = "$1" ]; then
		if [ "" = "$NUMSHARDS" ]; then 
			NUMSHARDS="1"
		fi
        	echo "Instance is running zk, numshards=$NUMSHARDS"
		OPT="-DzkRun -Dbootstrap_conf=true -DnumShards=$NUMSHARDS"
        fi
	setports $1
	cd ../example$1
	java $JAVA_OPTS -Djetty.port=$PORT $OPT -DSTOP.PORT=$STOP_PORT -DSTOP.KEY=key -jar start.jar 1>example$1.log 2>&1 &
}

stop() {
	echo "Stopping instance $1"
	setports $1
	cd ../example$1
	java -DSTOP.PORT=$STOP_PORT -DSTOP.KEY=key -jar start.jar --stop
}

do_kill() {
	echo "Killing instance $1"
	setports $1
	PID=`ps aux|grep "STOP.PORT=$STOP_PORT"|grep -v grep|cut -b 8-15`
	if [ "" = "$PID" ]; then
		echo "not running?"
	else
		kill -9 $PID
	fi
}

status() {
	echo "Status:"
	ps aux|grep "STOP.PORT"|grep -v grep
}

cleanlogs() {
    cd ../example$1
	mv example$1.log example$1.oldlog
}

taillogs() {
	cd ../example$1
	tail -f example$1.log
}

createshard() {
	setports $1
	echo "Creating new shard @instance $1, collection=$2, shard=$3, name=$4"
	curl "http://127.0.0.1:$PORT/solr/admin/cores?action=CREATE&collection=$2&name=$3&shard=$4"
}
