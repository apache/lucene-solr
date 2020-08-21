
Docker Solr FAQ
===============


How do I persist Solr data and config?
--------------------------------------

Your data is persisted already, in your container's filesystem.
If you `docker run`, add data to Solr, then `docker stop` and later
`docker start`, then your data is still there. The same is true for
changes to configuration files.

Equally, if you `docker commit` your container, you can later create a new
container from that image, and that will have your data in it.

For some use-cases it is convenient to provide a modified `solr.in.sh` file to Solr.
For example to point Solr to a ZooKeeper host:

```
docker create --name my_solr -P solr
docker cp my_solr:/opt/solr/bin/solr.in.sh .
sed -i -e 's/#ZK_HOST=.*/ZK_HOST=cylon.lan:2181/' solr.in.sh
docker cp solr.in.sh my_solr:/opt/solr/bin/solr.in.sh
docker start my_solr
# With a browser go to http://cylon.lan:32873/solr/#/ and confirm "-DzkHost=cylon.lan:2181" in the JVM Args section.
```

But usually when people ask this question, what they are after is a way
to store Solr data and config in a separate [Docker Volume](https://docs.docker.com/userguide/dockervolumes/).
That is explained in the next two questions.


How can I mount a host directory as a data volume?
--------------------------------------------------

This is useful if you want to inspect or modify the data in the Docker host
when the container is not running, and later easily run new containers against that data.
This is indeed possible, but there are a few gotchas.

Solr stores its core data in the `server/solr` directory, in sub-directories
for each core. The `server/solr` directory also contains configuration files
that are part of the Solr distribution.
Now, if we mounted volumes for each core individually, then that would
interfere with Solr trying to create those directories. If instead we make
the whole directory a volume, then we need to provide those configuration files
in our volume, which we can do by copying them from a temporary container.
For example:

```
# create a directory to store the server/solr directory
$ mkdir /home/docker-volumes/mysolr1

# make sure its host owner matches the container's solr user
$ sudo chown 8983:8983 /home/docker-volumes/mysolr1

# copy the solr directory from a temporary container to the volume
$ docker run -it --rm -v /home/docker-volumes/mysolr1:/target solr cp -r server/solr /target/

# pass the solr directory to a new container running solr
$ SOLR_CONTAINER=$(docker run -d -P -v /home/docker-volumes/mysolr1/solr:/opt/solr/server/solr solr)

# create a new core
$ docker exec -it --user=solr $SOLR_CONTAINER solr create_core -c gettingstarted

# check the volume on the host:
$ ls /home/docker-volumes/mysolr1/solr/
configsets  gettingstarted  README.txt  solr.xml  zoo.cfg
```

Note that if you add or modify files in that directory from the host, you must `chown 8983:8983` them.


How can I use a Data Volume Container?
--------------------------------------

You can avoid the concerns about UID mismatches above, by using data volumes only from containers.
You can create a container with a volume, then point future containers at that same volume.
This can be handy if you want to modify the solr image, for example if you want to add a program.
By separating the data and the code, you can change the code and re-use the data.

But there are pitfalls:

- if you remove the container that owns the volume, then you lose your data.
  Docker does not even warn you that a running container is dependent on it.
- if you point multiple solr containers at the same volume, you will have multiple instances
  write to the same files, which will undoubtedly lead to corruption
- if you do want to remove that volume, you must do `docker rm -v containername`;
  if you forget the `-v` there will be a dangling volume which you can not easily clean up.

Here is an example:

```
# create a container with a volume on the path that solr uses to store data.
docker create -v /opt/solr/server/solr --name mysolr1data solr /bin/true

# pass the volume to a new container running solr
SOLR_CONTAINER=$(docker run -d -P --volumes-from=mysolr1data solr)

# create a new core
$ docker exec -it --user=solr $SOLR_CONTAINER solr create_core -c gettingstarted

# make a change to the config, using the config API
docker exec -it --user=solr $SOLR_CONTAINER curl http://localhost:8983/solr/gettingstarted/config -H 'Content-type:application/json' -d'{
    "set-property" : {"query.filterCache.autowarmCount":1000},
    "unset-property" :"query.filterCache.size"}'

# verify the change took effect
docker exec -it --user=solr $SOLR_CONTAINER curl http://localhost:8983/solr/gettingstarted/config/overlay?omitHeader=true

# stop the solr container
docker exec -it --user=solr $SOLR_CONTAINER bash -c 'cd server; java -DSTOP.PORT=7983 -DSTOP.KEY=solrrocks -jar start.jar --stop'

# create a new container
SOLR_CONTAINER=$(docker run -d -P --volumes-from=mysolr1data solr)

# check our core is still there:
docker exec -it --user=solr $SOLR_CONTAINER ls server/solr/gettingstarted

# check the config modification is still there:
docker exec -it --user=solr $SOLR_CONTAINER curl http://localhost:8983/solr/gettingstarted/config/overlay?omitHeader=true
```


Can I use volumes with SOLR_HOME?
---------------------------------

Solr supports a SOLR_HOME environment variable to point to a non-standard location of the Solr home directory.
You can use this in docker-solr, in combination with volumes:

```
docker run -it -v $PWD/mysolrhome:/mysolrhome -e SOLR_HOME=/mysolrhome solr
```

This does need a pre-configured directory at that location.

To make this easier, docker-solr supports a INIT_SOLR_HOME setting, which copies the contents
from the default directory in the image to the SOLR_HOME (if it is empty).

```
mkdir mysolrhome
sudo chown 8983:8983 mysolrhome
docker run -it -v $PWD/mysolrhome:/mysolrhome -e SOLR_HOME=/mysolrhome -e INIT_SOLR_HOME=yes solr
```

Note: If SOLR_HOME is set, the "solr-precreate" command will put the created core in the SOLR_HOME directory
rather than the "mycores" directory.


Can I run ZooKeeper and Solr clusters under Docker?
---------------------------------------------------

At the network level the ZooKeeper nodes need to be able to talk to eachother,
and the Solr nodes need to be able to talk to the ZooKeeper nodes and to each other.
At the application level, different nodes need to be able to identify and locate each other.
In ZooKeeper that is done with a configuration file that lists hostnames or IP addresses for each node.
In Solr that is done with a parameter that specifies a host or IP address, which is then stored in ZooKeeper.

In typical clusters, those hostnames/IP addresses are pre-defined and remain static through the lifetime of the cluster.
In Docker, inter-container communication and multi-host networking can be facilitated by [Docker Networks](https://docs.docker.com/engine/userguide/networking/).
But, crucially, Docker does not normally guarantee that IP addresses of containers remain static during the lifetime of a container.
In non-networked Docker, the IP address seems to change everytime you stop/start.
In a networked Docker, containers can lose their IP address in certain sequences of starting/stopping, unless you take steps to prevent that.

IP changes causes problems:

- If you use hardcoded IP addresses in configuration, and the addresses of your containers change after a stops/start, then your cluster will stop working and may corrupt itself.
- If you use hostnames in configuration, and the addresses of your containers change, then you might run into problems with cached hostname lookups.
- And if you use hostnames there is another problem: the names are not defined until the respective container is running,
So when for example the first ZooKeeper node starts up, it will attempt a hostname lookup for the other nodes, and that will fail.
This is especially a problem for ZooKeeper 3.4.6; future versions are better at recovering.

Docker 1.10 has a new `--ip` configuration option that allows you to specify an IP address for a container.
It also has a `--ip-range` option that allows you to specify the range that other containers get addresses from.
Used together, you can implement static addresses. See [this example](docs/docker-networking.md).


Can I run ZooKeeper and Solr with Docker Links?
-----------------------------------------------

Docker's [Legacy container links](https://docs.docker.com/engine/userguide/networking/default_network/dockerlinks/) provide a way to
pass connection configuration between containers. It only works on a single machine, on the default bridge.
It provides no facilities for static IPs.
Note: this feature is expected to be deprecated and removed in a future release.
So really, see the "Can I run ZooKeeper and Solr clusters under Docker?" option above instead.

But for some use-cases, such as quick demos or one-shot automated testing, it can be convenient.

Run ZooKeeper, and define a name so we can link to it:

```console
$ docker run --name zookeeper -d -p 2181:2181 -p 2888:2888 -p 3888:3888 jplock/zookeeper
```

Run two Solr nodes, linked to the zookeeper container:

```console
$ docker run --name solr1 --link zookeeper:ZK -d -p 8983:8983 \
      solr \
      bash -c 'solr start -f -z $ZK_PORT_2181_TCP_ADDR:$ZK_PORT_2181_TCP_PORT'

$ docker run --name solr2 --link zookeeper:ZK -d -p 8984:8983 \
      solr \
      bash -c 'solr start -f -z $ZK_PORT_2181_TCP_ADDR:$ZK_PORT_2181_TCP_PORT'
```

Create a collection:

```console
$ docker exec -i -t solr1 solr create_collection \
        -c gettingstarted -shards 2 -p 8983
```

Then go to `http://localhost:8983/solr/#/~cloud` (adjust the hostname for your docker host) to see the two shards and Solr nodes.


How can I run ZooKeeper and Solr with Docker Compose?
-----------------------------------------------------

https://github.com/docker-solr/docker-solr-examples/blob/master/docker-compose/docker-compose.yml


I'm confused about the different invocations of solr -- help?
-------------------------------------------------------------

The different invocations of the docker-solr image can looks confusing, because the name of the
image is "solr" and the Solr command is also "solr", and the image interprets various arguments in
special ways. I'll illustrate the various invocations:


To run an arbitrary command in the image:

```
docker run -it solr date
```

here "solr" is the name of the image, and "date" is the command.
This does not invoke any solr functionality.


To run the Solr server:

```
docker run -it solr
```

Here "solr" is the name of the image, and there is no specific command,
so the image defaults to run the "solr" command with "-f" to run it in the foreground.


To run the Solr server with extra arguments:

```
docker run -it solr -h myhostname
```

This is is the same as the previous one, but an additional argument is passed.
The image will run the "solr" command with "-f -h myhostname"

To run solr as an arbitrary command:

```
docker run -it solr solr zk --help
```

here the first "solr" is the image name, and the second "solr"
is the "solr" command. The image runs the command exactly as specified;
no "-f" is implicitly added. The container will print help text, and exit.

If you find this visually confusing, it might be helpful to use more specific image tags,
and specific command paths. For example:

```
docker run -it solr:6 bin/solr -f -h myhostname
```

Finally, the docker-solr image offers several commands that do some work before
then invoking the Solr server, like "solr-precreate" and "solr-demo".
See the README.md for usage.
These are implemented by the `docker-entrypoint.sh` script, and must be passed
as the first argument to the image. For example:

```
docker run -it solr:6 solr-demo
```

It's important to understand an implementation detail here. The Dockerfile uses
`solr-foreground` as the `CMD`, and the `docker-entrypoint.sh` implements
that by by running "solr -f". So these two are equivalent:

```
docker run -it solr:6
docker run -it solr:6 solr-foreground
```

whereas:

```
docker run -it solr:6 solr -f
```

is slightly different: the "solr" there is a generic command, not treated in any
special way by `docker-entrypoint.sh`. In particular, this means that the
`docker-entrypoint-initdb.d` mechanism is not applied.
So, if you want to use `docker-entrypoint-initdb.d`, then you must use one
of the other two invocations.
You also need to keep that in mind when you want to invoke solr from the bash
command. For example, this does NOT run `docker-entrypoint-initdb.d` scripts:

```
docker run -it -v $PWD/set-heap.sh:/docker-entrypoint-initdb.d/set-heap.sh \
    solr:6 bash -c "echo hello; solr -f"
```

but this does:

```
docker run -it $PWD/set-heap.sh:/docker-entrypoint-initdb.d/set-heap.sh \
    solr:6 bash -c "echo hello; /opt/docker-solr/scripts/docker-entrypoint.sh solr-foreground"
```
