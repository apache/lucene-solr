INT_JAVA_OPTS="-server -Xms256M -Xmx256M"
BASE_PORT=8900
BASE_STOP_PORT=9900
ZK_PORT="2414"
ZK_CHROOT="solr"

rebuild() {
	echo "Rebuilding"
	cd ..
	rm -r -f dist
	rm -r -f build
	rm -r -f server/solr/zoo_data
	rm -f server/server.log
	ant server dist
}

setports() {
  PORT="$(( $BASE_PORT + $1 ))"
  STOP_PORT="$(( $BASE_STOP_PORT + $1 ))"
}

reinstall() {
	echo "Reinstalling instance $1"
	cd ..
	rm -rf  server$1
	cp -r -f server server$1
}

start() {
	OPT="-DzkHost=localhost:$ZK_PORT/$ZK_CHROOT"
	NUMSHARDS=$2

	echo "Starting instance $1"

	setports $1
	cd ../server$1
	java $JAVA_OPTS -Djetty.port=$PORT $OPT -DSTOP.PORT=$STOP_PORT -DSTOP.KEY=key -jar start.jar 1>server$1.log 2>&1 &
}

stop() {
	echo "Stopping instance $1"
	setports $1
	cd ../server$1
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
    cd ../server$1
	mv server$1.log server$1.oldlog
}

taillogs() {
	cd ../server$1
	tail -f server$1.log
}

createshard() {
	setports $1
	echo "Creating new shard @instance $1, collection=$2, shard=$3, name=$4"
	curl "http://127.0.0.1:$PORT/solr/admin/cores?action=CREATE&collection=$2&name=$3&shard=$4"
}
