Apache Solr MapReduce

*Experimental* - This contrib is currently subject to change in ways that may 
break back compatibility.

The Solr MapReduce contrib provides an a mapreduce job that allows you to build
Solr indexes and optionally merge them into a live Solr cluster.

Example:

# Build an index with map-reduce and deploy it to SolrCloud

source $solr_distrib/example/scripts/map-reduce/set-map-reduce-classpath.sh

$hadoop_distrib/bin/hadoop --config $hadoop_conf_dir jar \
$solr_distrib/dist/solr-map-reduce-*.jar -D 'mapred.child.java.opts=-Xmx500m' \
-libjars "$HADOOP_LIBJAR" --morphline-file readAvroContainer.conf \
--zk-host 127.0.0.1:9983 --output-dir hdfs://127.0.0.1:8020/outdir \
--collection $collection --log4j log4j.properties --go-live \
--verbose "hdfs://127.0.0.1:8020/indir"