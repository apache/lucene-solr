Example of Zookeeper and Solr cluster with Docker networking
------------------------------------------------------------

_Note: this article dates from Jan 2016. While this approach would still work, in Jan 2019 this would typically done with Docker cluster and orchestration tools like Kubernetes. See for example [this blog post](https://lucidworks.com/2019/02/07/running-solr-on-kubernetes-part-1/)._

In this example I'll create a cluster with 3 ZooKeeper nodes and 3 Solr nodes, distributed over 3 machines (trinity10, trinity20, trinity30).
I'll use an overlay network, specify fixed IP addresses when creating containers, and I'll pass in explicit `/etc/hosts` entries to make sure they are available even when nodes are down.
I won't show the configuration of the key-value store to configuration to enable networking, see [the docs](https://docs.docker.com/engine/userguide/networking/get-started-overlay/) for that.
I'll not use Docker Swarm in this example, but specifically place and configure containers where I want them by ssh'ing into the appropriate Docker host.

To make this example easier to understand I'll just use shell commands.
For actual use you may want to use a fancier deployment tool like [Fabric](http://www.fabfile.org).

Note: this example requires Docker 1.10.

I'll run these commands from the first machine, trinity10.

Create a network named "netzksolr" for this cluster. The `--ip-range` specifies the range of
addresses to use for containers, whereas the `--subnet` specifies all possible addresses in this
network. So effectively, addresses in the subnet but outside the range are reserved for containers
that specifically use the `--ip` option.

```
docker network create --driver=overlay --subnet 192.168.22.0/24 --ip-range=192.168.22.128/25 netzksolr
```

As a simple test, check the automatic assignment and specific assignment work:

```
mak@trinity10:~$ docker run -i --rm --net=netzksolr busybox ip -4 addr show eth0 | grep inet
    inet 192.168.23.129/24 scope global eth0
mak@trinity10:~$ docker run -i --rm --net=netzksolr --ip=192.168.22.5 busybox ip -4 addr show eth0 | grep inet
    inet 192.168.22.5/24 scope global eth0
```

So next create containers for ZooKeeper nodes.
First define some environment variables for convenience:

```
# the machine to run the container on
ZK1_HOST=trinity10.lan
ZK2_HOST=trinity20.lan
ZK3_HOST=trinity30.lan

# the IP address for the container
ZK1_IP=192.168.22.10
ZK2_IP=192.168.22.11
ZK3_IP=192.168.22.12

# the Docker image
ZK_IMAGE=jplock/zookeeper
```

Then create the containers:

```
ssh -n $ZK1_HOST "docker pull jplock/zookeeper && docker create --ip=$ZK1_IP --net netzksolr --name zk1 --hostname=zk1 --add-host zk2:$ZK2_IP --add-host zk3:$ZK3_IP -it $ZK_IMAGE"
ssh -n $ZK2_HOST "docker pull jplock/zookeeper && docker create --ip=$ZK2_IP --net netzksolr --name zk2 --hostname=zk2 --add-host zk1:$ZK1_IP --add-host zk3:$ZK3_IP -it $ZK_IMAGE"
ssh -n $ZK3_HOST "docker pull jplock/zookeeper && docker create --ip=$ZK3_IP --net netzksolr --name zk3 --hostname=zk3 --add-host zk1:$ZK1_IP --add-host zk2:$ZK2_IP -it $ZK_IMAGE"
```

Next configure those containers by creating ZooKeeper's `zoo.cfg` and `myid` files:

```
# Add ZooKeeper nodes to the ZooKeeper config.
# If you use hostnames here, ZK will complain with UnknownHostException about the other nodes.
# In ZooKeeper 3.4.6 that stays broken forever; in 3.4.7 that does recover.
# If you use IP addresses you avoid the UnknownHostException and get a quorum more quickly,
# but IP address changes can impact you.
docker cp zk1:/opt/zookeeper/conf/zoo.cfg .
cat >>zoo.cfg <<EOM
server.1=zk1:2888:3888
server.2=zk2:2888:3888
server.3=zk3:2888:3888
EOM

cat zoo.cfg | ssh $ZK1_HOST 'dd of=zoo.cfg.tmp && docker cp zoo.cfg.tmp zk1:/opt/zookeeper/conf/zoo.cfg && rm zoo.cfg.tmp'
cat zoo.cfg | ssh $ZK2_HOST 'dd of=zoo.cfg.tmp && docker cp zoo.cfg.tmp zk2:/opt/zookeeper/conf/zoo.cfg && rm zoo.cfg.tmp'
cat zoo.cfg | ssh $ZK3_HOST 'dd of=zoo.cfg.tmp && docker cp zoo.cfg.tmp zk3:/opt/zookeeper/conf/zoo.cfg && rm zoo.cfg.tmp'
rm zoo.cfg

echo 1 | ssh $ZK1_HOST  'dd of=myid && docker cp myid zk1:/tmp/zookeeper/myid && rm myid'
echo 2 | ssh $ZK2_HOST 'dd of=myid && docker cp myid zk2:/tmp/zookeeper/myid && rm myid'
echo 3 | ssh $ZK3_HOST 'dd of=myid && docker cp myid zk3:/tmp/zookeeper/myid && rm myid'
```

Now start the containers:

```
ssh -n $ZK1_HOST 'docker start zk1'
ssh -n $ZK2_HOST 'docker start zk2'
ssh -n $ZK3_HOST 'docker start zk3'

# Optional: verify containers are running
ssh -n $ZK1_HOST 'docker ps'
ssh -n $ZK2_HOST 'docker ps'
ssh -n $ZK3_HOST 'docker ps'

# Optional: inspect IP addresses of the containers
ssh -n $ZK1_HOST "docker inspect --format '{{ .NetworkSettings.Networks.netzksolr.IPAddress }}' zk1"
ssh -n $ZK2_HOST "docker inspect --format '{{ .NetworkSettings.Networks.netzksolr.IPAddress }}' zk2"
ssh -n $ZK3_HOST "docker inspect --format '{{ .NetworkSettings.Networks.netzksolr.IPAddress }}' zk3"

# Optional: verify connectivity and hostnames
ssh -n $ZK1_HOST 'docker run --rm --net netzksolr -i ubuntu bash -c "echo -n zk1,zk2,zk3 | xargs -n 1 --delimiter=, /bin/ping -c 1"'
ssh -n $ZK2_HOST 'docker run --rm --net netzksolr -i ubuntu bash -c "echo -n zk1,zk2,zk3 | xargs -n 1 --delimiter=, /bin/ping -c 1"'
ssh -n $ZK3_HOST 'docker run --rm --net netzksolr -i ubuntu bash -c "echo -n zk1,zk2,zk3 | xargs -n 1 --delimiter=, /bin/ping -c 1"'

# Optional: verify cluster got a leader
ssh -n $ZK1_HOST "docker exec -i zk1 bash -c 'echo stat | nc localhost 2181'"
ssh -n $ZK2_HOST "docker exec -i zk2 bash -c 'echo stat | nc localhost 2181'"
ssh -n $ZK3_HOST "docker exec -i zk3 bash -c 'echo stat | nc localhost 2181'"

# Optional: verify we can connect a zookeeper client. This should show the `[zookeeper]` znode.
printf "ls /\nquit\n" | ssh $ZK1_HOST docker exec -i zk1 /opt/zookeeper/bin/zkCli.sh
```

That's the ZooKeeper cluster running.

Next, we create Solr containers in much the same way:
```
ZKSOLR1_HOST=trinity10.lan
ZKSOLR2_HOST=trinity20.lan
ZKSOLR3_HOST=trinity30.lan

ZKSOLR1_IP=192.168.22.20
ZKSOLR2_IP=192.168.22.21
ZKSOLR3_IP=192.168.22.22

# the Docker image
SOLR_IMAGE=solr

HOST_OPTIONS="--add-host zk1:$ZK1_IP --add-host zk2:$ZK2_IP --add-host zk3:$ZK3_IP"
ssh -n $ZKSOLR1_HOST "docker pull $SOLR_IMAGE && docker create --ip=$ZKSOLR1_IP --net netzksolr --name zksolr1 --hostname=zksolr1 -it $HOST_OPTIONS $SOLR_IMAGE"
ssh -n $ZKSOLR2_HOST "docker pull $SOLR_IMAGE && docker create --ip=$ZKSOLR2_IP --net netzksolr --name zksolr2 --hostname=zksolr2 -it $HOST_OPTIONS $SOLR_IMAGE"
ssh -n $ZKSOLR3_HOST "docker pull $SOLR_IMAGE && docker create --ip=$ZKSOLR3_IP --net netzksolr --name zksolr3 --hostname=zksolr3 -it $HOST_OPTIONS $SOLR_IMAGE"
```

Now configure Solr to know where its ZooKeeper cluster is, and start the containers:

```
for h in zksolr1 zksolr2 zksolr3; do
  docker cp zksolr1:/opt/solr/bin/solr.in.sh .
  sed -i -e 's/#ZK_HOST=""/ZK_HOST="zk1:2181,zk2:2181,zk3:2181"/' solr.in.sh
  sed -i -e 's/#*SOLR_HOST=.*/SOLR_HOST="'$h'"/' solr.in.sh
  mv solr.in.sh solr.in.sh-$h
done
cat solr.in.sh-zksolr1 | ssh $ZKSOLR1_HOST "dd of=solr.in.sh && docker cp solr.in.sh zksolr1:/opt/solr/bin/solr.in.sh && rm solr.in.sh"
cat solr.in.sh-zksolr2 | ssh $ZKSOLR2_HOST "dd of=solr.in.sh && docker cp solr.in.sh zksolr2:/opt/solr/bin/solr.in.sh && rm solr.in.sh"
cat solr.in.sh-zksolr3 | ssh $ZKSOLR3_HOST "dd of=solr.in.sh && docker cp solr.in.sh zksolr3:/opt/solr/bin/solr.in.sh && rm solr.in.sh"
rm solr.in.sh*

ssh -n $ZKSOLR1_HOST docker start zksolr1
ssh -n $ZKSOLR2_HOST docker start zksolr2
ssh -n $ZKSOLR3_HOST docker start zksolr3

# Optional: print IP addresses to verify
ssh -n $ZKSOLR1_HOST 'docker inspect --format "{{ .NetworkSettings.Networks.netzksolr.IPAddress }}" zksolr1'
ssh -n $ZKSOLR2_HOST 'docker inspect --format "{{ .NetworkSettings.Networks.netzksolr.IPAddress }}" zksolr2'
ssh -n $ZKSOLR3_HOST 'docker inspect --format "{{ .NetworkSettings.Networks.netzksolr.IPAddress }}" zksolr3'

# Optional: check logs
ssh -n $ZKSOLR1_HOST docker logs zksolr1
ssh -n $ZKSOLR2_HOST docker logs zksolr2
ssh -n $ZKSOLR3_HOST docker logs zksolr3

# Optional: check the webserver
ssh -n $ZKSOLR1_HOST "docker exec -i zksolr1 /bin/bash -c 'wget -O -  http://zksolr1:8983/'"
ssh -n $ZKSOLR2_HOST "docker exec -i zksolr2 /bin/bash -c 'wget -O -  http://zksolr2:8983/'"
ssh -n $ZKSOLR3_HOST "docker exec -i zksolr3 /bin/bash -c 'wget -O -  http://zksolr3:8983/'"
```

Next let's create a collection:

```
ssh -n $ZKSOLR1_HOST docker exec -i zksolr1 /opt/solr/bin/solr create_collection -c my_collection1 -shards 2 -p 8983
```

To load data, and see it was split over shards:

```
mak@trinity10:~$ docker exec -it --user=solr zksolr1 bin/post -c my_collection1 example/exampledocs/manufacturers.xml
/usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java -classpath /opt/solr/dist/solr-core-5.4.0.jar -Dauto=yes -Dc=my_collection1 -Ddata=files org.apache.solr.util.SimplePostTool example/exampledocs/manufacturers.xml
SimplePostTool version 5.0.0
Posting files to [base] url http://localhost:8983/solr/my_collection1/update...
Entering auto mode. File endings considered are xml,json,csv,pdf,doc,docx,ppt,pptx,xls,xlsx,odt,odp,ods,ott,otp,ots,rtf,htm,html,txt,log
POSTing file manufacturers.xml (application/xml) to [base]
1 files indexed.
COMMITting Solr index changes to http://localhost:8983/solr/my_collection1/update...
Time spent: 0:00:01.093
mak@trinity10:~$ docker exec -it --user=solr zksolr1 bash -c "wget -q -O - 'http://zksolr1:8983/solr/my_collection1/select?q=*:*&indent=true&rows=100&fl=id' | egrep '<str name=.id.>' |  wc -l"
11
mak@trinity10:~$ docker exec -it --user=solr zksolr1 bash -c "wget -q -O - 'http://zksolr1:8983/solr/my_collection1/select?q=*:*&shards=shard1&rows=100&indent=true&fl=id' | grep '<str name=.id.>' | wc -l"
4
mak@trinity10:~$ docker exec -it --user=solr zksolr1 bash -c "wget -q -O - 'http://zksolr1:8983/solr/my_collection1/select?q=*:*&shards=shard2&rows=100&indent=true&fl=id' | grep '<str name=.id.>' | wc -l"
7
```

Now to get external access to this overlay network from outside we can use a container to proxy the connections.
For a simple TCP proxy container with an exposed port on the Docker host, proxying to a single Solr node, you can use [brandnetworks/tcpproxy](https://github.com/brandnetworks/tcpproxy):

```
ssh -n trinity10.lan "docker pull brandnetworks/tcpproxy && docker run -p 8001 -p 8002 --net netzksolr --name zksolrproxy --hostname=zksolrproxy.netzksolr -tid brandnetworks/tcpproxy --connections 8002:zksolr1:8983"
docker port zksolrproxy 8002
```

Or use a suitably configured HAProxy to round-robin between all Solr nodes. Or, instead of the overlay network, use [Project Calico](http://www.projectcalico.org) and configure L3 routing so you do not need to mess with proxies.

Now I can get to Solr on http://trinity10:32774/solr/#/. In the Cloud -> Tree -> /live_nodes view I see the Solr nodes.

From the Solr UI select the collection1 core, and click on Cloud -> Graph to see how it has created
two shards across our Solr nodes.

Now, by way of test, we'll stop the Solr containers, and start them out-of-order, and verify the IP addresses are unchanged, and check the same results come back:

```
ssh -n $ZKSOLR1_HOST docker kill zksolr1
ssh -n $ZKSOLR2_HOST docker kill zksolr2
ssh -n $ZKSOLR3_HOST docker kill zksolr3

ssh -n $ZKSOLR1_HOST docker start zksolr1
sleep 3
ssh -n $ZKSOLR3_HOST docker start zksolr3
sleep 3
ssh -n $ZKSOLR2_HOST docker start zksolr2

ssh -n $ZKSOLR1_HOST 'docker inspect --format "{{ .NetworkSettings.Networks.netzksolr.IPAddress }}" zksolr1'
ssh -n $ZKSOLR2_HOST 'docker inspect --format "{{ .NetworkSettings.Networks.netzksolr.IPAddress }}" zksolr2'
ssh -n $ZKSOLR3_HOST 'docker inspect --format "{{ .NetworkSettings.Networks.netzksolr.IPAddress }}" zksolr3'

docker exec -it --user=solr zksolr1 bash -c "wget -q -O - 'http://zksolr1:8983/solr/my_collection1/select?q=*:*&indent=true&rows=100&fl=id' | egrep '<str name=.id.>' |  wc -l"
docker exec -it --user=solr zksolr1 bash -c "wget -q -O - 'http://zksolr1:8983/solr/my_collection1/select?q=*:*&shards=shard1&rows=100&indent=true&fl=id' | grep '<str name=.id.>' | wc -l"
docker exec -it --user=solr zksolr1 bash -c "wget -q -O - 'http://zksolr1:8983/solr/my_collection1/select?q=*:*&shards=shard2&rows=100&indent=true&fl=id' | grep '<str name=.id.>' | wc -l"
```

Good, that works.


Finally To clean up this example:
```
ssh -n $ZK1_HOST "docker kill zk1; docker rm zk1"
ssh -n $ZK2_HOST "docker kill zk2; docker rm zk2"
ssh -n $ZK3_HOST "docker kill zk3; docker rm zk3"
ssh -n $ZKSOLR1_HOST "docker kill zksolr1; docker rm zksolr1"
ssh -n $ZKSOLR2_HOST "docker kill zksolr2; docker rm zksolr2"
ssh -n $ZKSOLR3_HOST "docker kill zksolr3; docker rm zksolr3"
ssh -n trinity10.lan "docker kill zksolrproxy; docker rm zksolrproxy"
docker network rm netzksolr
```
