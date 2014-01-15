/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.hadoop;


import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.impl.action.HelpArgumentAction;
import net.sourceforge.argparse4j.impl.choice.RangeArgumentChoice;
import net.sourceforge.argparse4j.impl.type.FileArgumentType;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.ArgumentGroup;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.FeatureControl;
import net.sourceforge.argparse4j.inf.Namespace;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.hadoop.dedup.RetainMostRecentUpdateConflictResolver;
import org.apache.solr.hadoop.morphline.MorphlineMapRunner;
import org.apache.solr.hadoop.morphline.MorphlineMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kitesdk.morphline.base.Fields;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;


/**
 * Public API for a MapReduce batch job driver that creates a set of Solr index shards from a set of
 * input files and writes the indexes into HDFS, in a flexible, scalable and fault-tolerant manner.
 * Also supports merging the output shards into a set of live customer facing Solr servers,
 * typically a SolrCloud.
 */
public class MapReduceIndexerTool extends Configured implements Tool {
  
  Job job; // visible for testing only
  
  public static final String RESULTS_DIR = "results";

  static final String MAIN_MEMORY_RANDOMIZATION_THRESHOLD = 
      MapReduceIndexerTool.class.getName() + ".mainMemoryRandomizationThreshold";
  
  private static final String FULL_INPUT_LIST = "full-input-list.txt";
  
  private static final Logger LOG = LoggerFactory.getLogger(MapReduceIndexerTool.class);

  
  /**
   * See http://argparse4j.sourceforge.net and for details see http://argparse4j.sourceforge.net/usage.html
   */
  static final class MyArgumentParser {
    
    private static final String SHOW_NON_SOLR_CLOUD = "--show-non-solr-cloud";

    private boolean showNonSolrCloud = false;

    /**
     * Parses the given command line arguments.
     * 
     * @return exitCode null indicates the caller shall proceed with processing,
     *         non-null indicates the caller shall exit the program with the
     *         given exit status code.
     */
    public Integer parseArgs(String[] args, Configuration conf, Options opts) {
      assert args != null;
      assert conf != null;
      assert opts != null;

      if (args.length == 0) {
        args = new String[] { "--help" };
      }
      
      showNonSolrCloud = Arrays.asList(args).contains(SHOW_NON_SOLR_CLOUD); // intercept it first
      
      ArgumentParser parser = ArgumentParsers
        .newArgumentParser("hadoop [GenericOptions]... jar solr-map-reduce-*.jar ", false)
        .defaultHelp(true)
        .description(
          "MapReduce batch job driver that takes a morphline and creates a set of Solr index shards from a set of input files " +
          "and writes the indexes into HDFS, in a flexible, scalable and fault-tolerant manner. " +
          "It also supports merging the output shards into a set of live customer facing Solr servers, " +
          "typically a SolrCloud. The program proceeds in several consecutive MapReduce based phases, as follows:" +
          "\n\n" +
          "1) Randomization phase: This (parallel) phase randomizes the list of input files in order to spread " +
          "indexing load more evenly among the mappers of the subsequent phase." +  
          "\n\n" +
          "2) Mapper phase: This (parallel) phase takes the input files, extracts the relevant content, transforms it " +
          "and hands SolrInputDocuments to a set of reducers. " +
          "The ETL functionality is flexible and " +
          "customizable using chains of arbitrary morphline commands that pipe records from one transformation command to another. " + 
          "Commands to parse and transform a set of standard data formats such as Avro, CSV, Text, HTML, XML, " +
          "PDF, Word, Excel, etc. are provided out of the box, and additional custom commands and parsers for additional " +
          "file or data formats can be added as morphline plugins. " +
          "This is done by implementing a simple Java interface that consumes a record (e.g. a file in the form of an InputStream " +
          "plus some headers plus contextual metadata) and generates as output zero or more records. " + 
          "Any kind of data format can be indexed and any Solr documents for any kind of Solr schema can be generated, " +
          "and any custom ETL logic can be registered and executed.\n" +
          "Record fields, including MIME types, can also explicitly be passed by force from the CLI to the morphline, for example: " +
          "hadoop ... -D " + MorphlineMapRunner.MORPHLINE_FIELD_PREFIX + Fields.ATTACHMENT_MIME_TYPE + "=text/csv" +
          "\n\n" +
          "3) Reducer phase: This (parallel) phase loads the mapper's SolrInputDocuments into one EmbeddedSolrServer per reducer. " +
          "Each such reducer and Solr server can be seen as a (micro) shard. The Solr servers store their " +
          "data in HDFS." + 
          "\n\n" +
          "4) Mapper-only merge phase: This (parallel) phase merges the set of reducer shards into the number of solr " +
          "shards expected by the user, using a mapper-only job. This phase is omitted if the number " +
          "of shards is already equal to the number of shards expected by the user. " +
          "\n\n" +
          "5) Go-live phase: This optional (parallel) phase merges the output shards of the previous phase into a set of " +
          "live customer facing Solr servers, typically a SolrCloud. " +
          "If this phase is omitted you can explicitly point each Solr server to one of the HDFS output shard directories." +
          "\n\n" +
          "Fault Tolerance: Mapper and reducer task attempts are retried on failure per the standard MapReduce semantics. " +
          "On program startup all data in the --output-dir is deleted if that output directory already exists. " +
          "If the whole job fails you can retry simply by rerunning the program again using the same arguments." 
          );

      parser.addArgument("--help", "-help", "-h")
        .help("Show this help message and exit")
        .action(new HelpArgumentAction() {
          @Override
          public void run(ArgumentParser parser, Argument arg, Map<String, Object> attrs, String flag, Object value) throws ArgumentParserException {
            try {
              parser.printHelp(new PrintWriter(new OutputStreamWriter(System.out, "UTF-8")));
            } catch (UnsupportedEncodingException e) {
              throw new RuntimeException("Won't Happen for UTF-8");
            }  
            System.out.println();
            System.out.print(ToolRunnerHelpFormatter.getGenericCommandUsage());
            //ToolRunner.printGenericCommandUsage(System.out);
            System.out.println(
              "Examples: \n\n" + 

              "# (Re)index an Avro based Twitter tweet file:\n" +
              "sudo -u hdfs hadoop \\\n" + 
              "  --config /etc/hadoop/conf.cloudera.mapreduce1 \\\n" +
              "  jar target/solr-map-reduce-*.jar \\\n" +
              "  -D 'mapred.child.java.opts=-Xmx500m' \\\n" + 
//            "  -D 'mapreduce.child.java.opts=-Xmx500m' \\\n" + 
              "  --log4j src/test/resources/log4j.properties \\\n" + 
              "  --morphline-file ../search-core/src/test/resources/test-morphlines/tutorialReadAvroContainer.conf \\\n" + 
              "  --solr-home-dir src/test/resources/solr/minimr \\\n" +
              "  --output-dir hdfs://c2202.mycompany.com/user/$USER/test \\\n" + 
              "  --shards 1 \\\n" + 
              "  hdfs:///user/$USER/test-documents/sample-statuses-20120906-141433.avro\n" +
              "\n" +
              "# (Re)index all files that match all of the following conditions:\n" +
              "# 1) File is contained in dir tree hdfs:///user/$USER/solrloadtest/twitter/tweets\n" +
              "# 2) file name matches the glob pattern 'sample-statuses*.gz'\n" +
              "# 3) file was last modified less than 100000 minutes ago\n" +
              "# 4) file size is between 1 MB and 1 GB\n" +
              "# Also include extra library jar file containing JSON tweet Java parser:\n" +
              "hadoop jar target/solr-map-reduce-*.jar " + "com.cloudera.cdk.morphline.hadoop.find.HdfsFindTool" + " \\\n" + 
              "  -find hdfs:///user/$USER/solrloadtest/twitter/tweets \\\n" + 
              "  -type f \\\n" + 
              "  -name 'sample-statuses*.gz' \\\n" + 
              "  -mmin -1000000 \\\n" + 
              "  -size -100000000c \\\n" + 
              "  -size +1000000c \\\n" + 
              "| sudo -u hdfs hadoop \\\n" + 
              "  --config /etc/hadoop/conf.cloudera.mapreduce1 \\\n" + 
              "  jar target/solr-map-reduce-*.jar \\\n" +
              "  -D 'mapred.child.java.opts=-Xmx500m' \\\n" + 
//            "  -D 'mapreduce.child.java.opts=-Xmx500m' \\\n" + 
              "  --log4j src/test/resources/log4j.properties \\\n" + 
              "  --morphline-file ../search-core/src/test/resources/test-morphlines/tutorialReadJsonTestTweets.conf \\\n" + 
              "  --solr-home-dir src/test/resources/solr/minimr \\\n" + 
              "  --output-dir hdfs://c2202.mycompany.com/user/$USER/test \\\n" + 
              "  --shards 100 \\\n" + 
              "  --input-list -\n" +
              "\n" +
              "# Go live by merging resulting index shards into a live Solr cluster\n" +
              "# (explicitly specify Solr URLs - for a SolrCloud cluster see next example):\n" +
              "sudo -u hdfs hadoop \\\n" + 
              "  --config /etc/hadoop/conf.cloudera.mapreduce1 \\\n" +
              "  jar target/solr-map-reduce-*.jar \\\n" +
              "  -D 'mapred.child.java.opts=-Xmx500m' \\\n" + 
//            "  -D 'mapreduce.child.java.opts=-Xmx500m' \\\n" + 
              "  --log4j src/test/resources/log4j.properties \\\n" + 
              "  --morphline-file ../search-core/src/test/resources/test-morphlines/tutorialReadAvroContainer.conf \\\n" + 
              "  --solr-home-dir src/test/resources/solr/minimr \\\n" + 
              "  --output-dir hdfs://c2202.mycompany.com/user/$USER/test \\\n" + 
              "  --shard-url http://solr001.mycompany.com:8983/solr/collection1 \\\n" + 
              "  --shard-url http://solr002.mycompany.com:8983/solr/collection1 \\\n" + 
              "  --go-live \\\n" + 
              "  hdfs:///user/foo/indir\n" +  
              "\n" +
              "# Go live by merging resulting index shards into a live SolrCloud cluster\n" +
              "# (discover shards and Solr URLs through ZooKeeper):\n" +
              "sudo -u hdfs hadoop \\\n" + 
              "  --config /etc/hadoop/conf.cloudera.mapreduce1 \\\n" +
              "  jar target/solr-map-reduce-*.jar \\\n" +
              "  -D 'mapred.child.java.opts=-Xmx500m' \\\n" + 
//            "  -D 'mapreduce.child.java.opts=-Xmx500m' \\\n" + 
              "  --log4j src/test/resources/log4j.properties \\\n" + 
              "  --morphline-file ../search-core/src/test/resources/test-morphlines/tutorialReadAvroContainer.conf \\\n" + 
              "  --output-dir hdfs://c2202.mycompany.com/user/$USER/test \\\n" + 
              "  --zk-host zk01.mycompany.com:2181/solr \\\n" + 
              "  --collection collection1 \\\n" + 
              "  --go-live \\\n" + 
              "  hdfs:///user/foo/indir\n"
            );
            throw new FoundHelpArgument(); // Trick to prevent processing of any remaining arguments
          }
        });
      
      ArgumentGroup requiredGroup = parser.addArgumentGroup("Required arguments");
      
      Argument outputDirArg = requiredGroup.addArgument("--output-dir")
        .metavar("HDFS_URI")
        .type(new PathArgumentType(conf) {
          @Override
          public Path convert(ArgumentParser parser, Argument arg, String value) throws ArgumentParserException {
            Path path = super.convert(parser, arg, value);
            if ("hdfs".equals(path.toUri().getScheme()) && path.toUri().getAuthority() == null) {
              // TODO: consider defaulting to hadoop's fs.default.name here or in SolrRecordWriter.createEmbeddedSolrServer()
              throw new ArgumentParserException("Missing authority in path URI: " + path, parser); 
            }
            return path;
          }
        }.verifyHasScheme().verifyIsAbsolute().verifyCanWriteParent())
        .required(true)
        .help("HDFS directory to write Solr indexes to. Inside there one output directory per shard will be generated. " +
              "Example: hdfs://c2202.mycompany.com/user/$USER/test");
      
      Argument inputListArg = parser.addArgument("--input-list")
        .action(Arguments.append())
        .metavar("URI")
  //      .type(new PathArgumentType(fs).verifyExists().verifyCanRead())
        .type(Path.class)
        .help("Local URI or HDFS URI of a UTF-8 encoded file containing a list of HDFS URIs to index, " +
              "one URI per line in the file. If '-' is specified, URIs are read from the standard input. " + 
              "Multiple --input-list arguments can be specified.");
        
      Argument morphlineFileArg = requiredGroup.addArgument("--morphline-file")
        .metavar("FILE")
        .type(new FileArgumentType().verifyExists().verifyIsFile().verifyCanRead())
        .required(true)
        .help("Relative or absolute path to a local config file that contains one or more morphlines. " +
              "The file must be UTF-8 encoded. Example: /path/to/morphline.conf");
          
      Argument morphlineIdArg = parser.addArgument("--morphline-id")
        .metavar("STRING")
        .type(String.class)
        .help("The identifier of the morphline that shall be executed within the morphline config file " +
              "specified by --morphline-file. If the --morphline-id option is ommitted the first (i.e. " +
              "top-most) morphline within the config file is used. Example: morphline1");
            
      Argument solrHomeDirArg = nonSolrCloud(parser.addArgument("--solr-home-dir")
        .metavar("DIR")
        .type(new FileArgumentType() {
          @Override
          public File convert(ArgumentParser parser, Argument arg, String value) throws ArgumentParserException {
            File solrHomeDir = super.convert(parser, arg, value);
            File solrConfigFile = new File(new File(solrHomeDir, "conf"), "solrconfig.xml");
            new FileArgumentType().verifyExists().verifyIsFile().verifyCanRead().convert(
                parser, arg, solrConfigFile.getPath());
            return solrHomeDir;
          }
        }.verifyIsDirectory().verifyCanRead())
        .required(false)
        .help("Relative or absolute path to a local dir containing Solr conf/ dir and in particular " +
              "conf/solrconfig.xml and optionally also lib/ dir. This directory will be uploaded to each MR task. " +
              "Example: src/test/resources/solr/minimr"));
        
      Argument updateConflictResolverArg = parser.addArgument("--update-conflict-resolver")
        .metavar("FQCN")
        .type(String.class)
        .setDefault(RetainMostRecentUpdateConflictResolver.class.getName())
        .help("Fully qualified class name of a Java class that implements the UpdateConflictResolver interface. " +
            "This enables deduplication and ordering of a series of document updates for the same unique document " +
            "key. For example, a MapReduce batch job might index multiple files in the same job where some of the " +
            "files contain old and new versions of the very same document, using the same unique document key.\n" +
            "Typically, implementations of this interface forbid collisions by throwing an exception, or ignore all but " +
            "the most recent document version, or, in the general case, order colliding updates ascending from least " +
            "recent to most recent (partial) update. The caller of this interface (i.e. the Hadoop Reducer) will then " +
            "apply the updates to Solr in the order returned by the orderUpdates() method.\n" +
            "The default RetainMostRecentUpdateConflictResolver implementation ignores all but the most recent document " +
            "version, based on a configurable numeric Solr field, which defaults to the file_last_modified timestamp");
      
      Argument mappersArg = parser.addArgument("--mappers")
        .metavar("INTEGER")
        .type(Integer.class)
        .choices(new RangeArgumentChoice(-1, Integer.MAX_VALUE)) // TODO: also support X% syntax where X is an integer
        .setDefault(-1)
        .help("Tuning knob that indicates the maximum number of MR mapper tasks to use. -1 indicates use all map slots " +
            "available on the cluster.");
  
      Argument reducersArg = parser.addArgument("--reducers")
        .metavar("INTEGER")
        .type(Integer.class)
        .choices(new RangeArgumentChoice(-1, Integer.MAX_VALUE)) // TODO: also support X% syntax where X is an integer
        .setDefault(-1)
        .help("Tuning knob that indicates the number of reducers to index into. " +
            "-1 indicates use all reduce slots available on the cluster. " +
            "0 indicates use one reducer per output shard, which disables the mtree merge MR algorithm. " +
            "The mtree merge MR algorithm improves scalability by spreading load " +
            "(in particular CPU load) among a number of parallel reducers that can be much larger than the number " +
            "of solr shards expected by the user. It can be seen as an extension of concurrent lucene merges " +
            "and tiered lucene merges to the clustered case. The subsequent mapper-only phase " +
            "merges the output of said large number of reducers to the number of shards expected by the user, " +
            "again by utilizing more available parallelism on the cluster.");

      Argument fanoutArg = parser.addArgument("--fanout")
        .metavar("INTEGER")
        .type(Integer.class)
        .choices(new RangeArgumentChoice(2, Integer.MAX_VALUE))
        .setDefault(Integer.MAX_VALUE)
        .help(FeatureControl.SUPPRESS);
  
      Argument maxSegmentsArg = parser.addArgument("--max-segments")
        .metavar("INTEGER")  
        .type(Integer.class)
        .choices(new RangeArgumentChoice(1, Integer.MAX_VALUE))
        .setDefault(1)
        .help("Tuning knob that indicates the maximum number of segments to be contained on output in the index of " +
            "each reducer shard. After a reducer has built its output index it applies a merge policy to merge segments " +
            "until there are <= maxSegments lucene segments left in this index. " + 
            "Merging segments involves reading and rewriting all data in all these segment files, " + 
            "potentially multiple times, which is very I/O intensive and time consuming. " + 
            "However, an index with fewer segments can later be merged faster, " +
            "and it can later be queried faster once deployed to a live Solr serving shard. " + 
            "Set maxSegments to 1 to optimize the index for low query latency. " + 
            "In a nutshell, a small maxSegments value trades indexing latency for subsequently improved query latency. " + 
            "This can be a reasonable trade-off for batch indexing systems.");
      
      Argument fairSchedulerPoolArg = parser.addArgument("--fair-scheduler-pool")
        .metavar("STRING")
        .help("Optional tuning knob that indicates the name of the fair scheduler pool to submit jobs to. " +
              "The Fair Scheduler is a pluggable MapReduce scheduler that provides a way to share large clusters. " +
              "Fair scheduling is a method of assigning resources to jobs such that all jobs get, on average, an " +
              "equal share of resources over time. When there is a single job running, that job uses the entire " +
              "cluster. When other jobs are submitted, tasks slots that free up are assigned to the new jobs, so " +
              "that each job gets roughly the same amount of CPU time. Unlike the default Hadoop scheduler, which " +
              "forms a queue of jobs, this lets short jobs finish in reasonable time while not starving long jobs. " +
              "It is also an easy way to share a cluster between multiple of users. Fair sharing can also work with " +
              "job priorities - the priorities are used as weights to determine the fraction of total compute time " +
              "that each job gets.");
  
      Argument dryRunArg = parser.addArgument("--dry-run")
        .action(Arguments.storeTrue())
        .help("Run in local mode and print documents to stdout instead of loading them into Solr. This executes " +
              "the morphline in the client process (without submitting a job to MR) for quicker turnaround during " +
              "early trial & debug sessions.");
    
      Argument log4jConfigFileArg = parser.addArgument("--log4j")
        .metavar("FILE")
        .type(new FileArgumentType().verifyExists().verifyIsFile().verifyCanRead())
        .help("Relative or absolute path to a log4j.properties config file on the local file system. This file " +
            "will be uploaded to each MR task. Example: /path/to/log4j.properties");
    
      Argument verboseArg = parser.addArgument("--verbose", "-v")
        .action(Arguments.storeTrue())
        .help("Turn on verbose output.");
  
      parser.addArgument(SHOW_NON_SOLR_CLOUD)
        .action(Arguments.storeTrue())
        .help("Also show options for Non-SolrCloud mode as part of --help.");
      
      ArgumentGroup clusterInfoGroup = parser
          .addArgumentGroup("Cluster arguments")
          .description(
              "Arguments that provide information about your Solr cluster. "
            + nonSolrCloud("If you are building shards for a SolrCloud cluster, pass the --zk-host argument. "
            + "If you are building shards for "
            + "a Non-SolrCloud cluster, pass the --shard-url argument one or more times. To build indexes for "
            + "a replicated Non-SolrCloud cluster with --shard-url, pass replica urls consecutively and also pass --shards. "
            + "Using --go-live requires either --zk-host or --shard-url."));

      Argument zkHostArg = clusterInfoGroup.addArgument("--zk-host")
        .metavar("STRING")
        .type(String.class)
        .help("The address of a ZooKeeper ensemble being used by a SolrCloud cluster. "
            + "This ZooKeeper ensemble will be examined to determine the number of output "
            + "shards to create as well as the Solr URLs to merge the output shards into when using the --go-live option. "
            + "Requires that you also pass the --collection to merge the shards into.\n"
            + "\n"
            + "The --zk-host option implements the same partitioning semantics as the standard SolrCloud " 
            + "Near-Real-Time (NRT) API. This enables to mix batch updates from MapReduce ingestion with "
            + "updates from standard Solr NRT ingestion on the same SolrCloud cluster, "
            + "using identical unique document keys.\n"
            + "\n"
            + "Format is: a list of comma separated host:port pairs, each corresponding to a zk "
            + "server. Example: '127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183' If "
            + "the optional chroot suffix is used the example would look "
            + "like: '127.0.0.1:2181/solr,127.0.0.1:2182/solr,127.0.0.1:2183/solr' "
            + "where the client would be rooted at '/solr' and all paths "
            + "would be relative to this root - i.e. getting/setting/etc... "
            + "'/foo/bar' would result in operations being run on "
            + "'/solr/foo/bar' (from the server perspective).\n"
            + nonSolrCloud("\n"
            + "If --solr-home-dir is not specified, the Solr home directory for the collection "
            + "will be downloaded from this ZooKeeper ensemble."));

      Argument shardUrlsArg = nonSolrCloud(clusterInfoGroup.addArgument("--shard-url")
        .metavar("URL")
        .type(String.class)
        .action(Arguments.append())
        .help("Solr URL to merge resulting shard into if using --go-live. " +
              "Example: http://solr001.mycompany.com:8983/solr/collection1. " + 
              "Multiple --shard-url arguments can be specified, one for each desired shard. " +
              "If you are merging shards into a SolrCloud cluster, use --zk-host instead."));
      
      Argument shardsArg = nonSolrCloud(clusterInfoGroup.addArgument("--shards")
        .metavar("INTEGER")
        .type(Integer.class)
        .choices(new RangeArgumentChoice(1, Integer.MAX_VALUE))
        .help("Number of output shards to generate."));
      
      ArgumentGroup goLiveGroup = parser.addArgumentGroup("Go live arguments")
        .description("Arguments for merging the shards that are built into a live Solr cluster. " +
                     "Also see the Cluster arguments.");

      Argument goLiveArg = goLiveGroup.addArgument("--go-live")
        .action(Arguments.storeTrue())
        .help("Allows you to optionally merge the final index shards into a live Solr cluster after they are built. " +
              "You can pass the ZooKeeper address with --zk-host and the relevant cluster information will be auto detected. " +
              nonSolrCloud("If you are not using a SolrCloud cluster, --shard-url arguments can be used to specify each SolrCore to merge " +
              "each shard into."));

      Argument collectionArg = goLiveGroup.addArgument("--collection")
        .metavar("STRING")
        .help("The SolrCloud collection to merge shards into when using --go-live and --zk-host. Example: collection1");
      
      Argument goLiveThreadsArg = goLiveGroup.addArgument("--go-live-threads")
        .metavar("INTEGER")
        .type(Integer.class)
        .choices(new RangeArgumentChoice(1, Integer.MAX_VALUE))
        .setDefault(1000)
        .help("Tuning knob that indicates the maximum number of live merges to run in parallel at one time.");
      
      // trailing positional arguments
      Argument inputFilesArg = parser.addArgument("input-files")
        .metavar("HDFS_URI")
        .type(new PathArgumentType(conf).verifyHasScheme().verifyExists().verifyCanRead())
        .nargs("*")
        .setDefault()
        .help("HDFS URI of file or directory tree to index.");
          
      Namespace ns;
      try {
        ns = parser.parseArgs(args);
      } catch (FoundHelpArgument e) {
        return 0;
      } catch (ArgumentParserException e) {
        parser.handleError(e);
        return 1;
      }
      
      opts.log4jConfigFile = (File) ns.get(log4jConfigFileArg.getDest());
      if (opts.log4jConfigFile != null) {
        PropertyConfigurator.configure(opts.log4jConfigFile.getPath());        
      }
      LOG.debug("Parsed command line args: {}", ns);
      
      opts.inputLists = ns.getList(inputListArg.getDest());
      if (opts.inputLists == null) {
        opts.inputLists = Collections.EMPTY_LIST;
      }
      opts.inputFiles = ns.getList(inputFilesArg.getDest());
      opts.outputDir = (Path) ns.get(outputDirArg.getDest());
      opts.mappers = ns.getInt(mappersArg.getDest());
      opts.reducers = ns.getInt(reducersArg.getDest());
      opts.updateConflictResolver = ns.getString(updateConflictResolverArg.getDest());
      opts.fanout = ns.getInt(fanoutArg.getDest());
      opts.maxSegments = ns.getInt(maxSegmentsArg.getDest());
      opts.morphlineFile = (File) ns.get(morphlineFileArg.getDest());
      opts.morphlineId = ns.getString(morphlineIdArg.getDest());
      opts.solrHomeDir = (File) ns.get(solrHomeDirArg.getDest());
      opts.fairSchedulerPool = ns.getString(fairSchedulerPoolArg.getDest());
      opts.isDryRun = ns.getBoolean(dryRunArg.getDest());
      opts.isVerbose = ns.getBoolean(verboseArg.getDest());
      opts.zkHost = ns.getString(zkHostArg.getDest());
      opts.shards = ns.getInt(shardsArg.getDest());
      opts.shardUrls = buildShardUrls(ns.getList(shardUrlsArg.getDest()), opts.shards);
      opts.goLive = ns.getBoolean(goLiveArg.getDest());
      opts.goLiveThreads = ns.getInt(goLiveThreadsArg.getDest());
      opts.collection = ns.getString(collectionArg.getDest());

      try {
        verifyGoLiveArgs(opts, parser);
      } catch (ArgumentParserException e) {
        parser.handleError(e);
        return 1;
      }

      if (opts.inputLists.isEmpty() && opts.inputFiles.isEmpty()) {
        LOG.info("No input files specified - nothing to process");
        return 0; // nothing to process
      }
      return null;     
    }

    // make it a "hidden" option, i.e. the option is functional and enabled but not shown in --help output
    private Argument nonSolrCloud(Argument arg) {
        return showNonSolrCloud ? arg : arg.help(FeatureControl.SUPPRESS); 
    }

    private String nonSolrCloud(String msg) {
        return showNonSolrCloud ? msg : "";
    }

    /** Marker trick to prevent processing of any remaining arguments once --help option has been parsed */
    private static final class FoundHelpArgument extends RuntimeException {      
    }
  }
  // END OF INNER CLASS  

  static List<List<String>> buildShardUrls(List<Object> urls, Integer numShards) {
    if (urls == null) return null;
    List<List<String>> shardUrls = new ArrayList<List<String>>(urls.size());
    List<String> list = null;
    
    int sz;
    if (numShards == null) {
      numShards = urls.size();
    }
    sz = (int) Math.ceil(urls.size() / (float)numShards);
    for (int i = 0; i < urls.size(); i++) {
      if (i % sz == 0) {
        list = new ArrayList<String>();
        shardUrls.add(list);
      }
      list.add((String) urls.get(i));
    }

    return shardUrls;
  }
  
  static final class Options {    
    boolean goLive;
    String collection;
    String zkHost;
    Integer goLiveThreads;
    List<List<String>> shardUrls;
    List<Path> inputLists;
    List<Path> inputFiles;
    Path outputDir;
    int mappers;
    int reducers;
    String updateConflictResolver;
    int fanout;
    Integer shards;
    int maxSegments;
    File morphlineFile;
    String morphlineId;
    File solrHomeDir;
    String fairSchedulerPool;
    boolean isDryRun;
    File log4jConfigFile;
    boolean isVerbose;
  }
  // END OF INNER CLASS  

  
  /** API for command line clients */
  public static void main(String[] args) throws Exception  {
    int res = ToolRunner.run(new Configuration(), new MapReduceIndexerTool(), args);
    System.exit(res);
  }

  public MapReduceIndexerTool() {}

  @Override
  public int run(String[] args) throws Exception {
    Options opts = new Options();
    Integer exitCode = new MyArgumentParser().parseArgs(args, getConf(), opts);
    if (exitCode != null) {
      return exitCode;
    }
    return run(opts);
  }
  
  /** API for Java clients; visible for testing; may become a public API eventually */
  int run(Options options) throws Exception {

    if ("local".equals(getConf().get("mapred.job.tracker"))) {
      throw new IllegalStateException(
        "Running with LocalJobRunner (i.e. all of Hadoop inside a single JVM) is not supported " +
        "because LocalJobRunner does not (yet) implement the Hadoop Distributed Cache feature, " +
        "which is required for passing files via --files and --libjars");
    }

    long programStartTime = System.currentTimeMillis();
    if (options.fairSchedulerPool != null) {
      getConf().set("mapred.fairscheduler.pool", options.fairSchedulerPool);
    }
    getConf().setInt(SolrOutputFormat.SOLR_RECORD_WRITER_MAX_SEGMENTS, options.maxSegments);
    
    // switch off a false warning about allegedly not implementing Tool
    // also see http://hadoop.6.n7.nabble.com/GenericOptionsParser-warning-td8103.html
    // also see https://issues.apache.org/jira/browse/HADOOP-8183
    getConf().setBoolean("mapred.used.genericoptionsparser", true);
    
    if (options.log4jConfigFile != null) {
      Utils.setLogConfigFile(options.log4jConfigFile, getConf());
      addDistributedCacheFile(options.log4jConfigFile, getConf());
    }

    job = Job.getInstance(getConf());
    job.setJarByClass(getClass());

    if (options.morphlineFile == null) {
      throw new ArgumentParserException("Argument --morphline-file is required", null);
    }
    verifyGoLiveArgs(options, null);
    verifyZKStructure(options, null);
    
    int mappers = new JobClient(job.getConfiguration()).getClusterStatus().getMaxMapTasks(); // MR1
    //int mappers = job.getCluster().getClusterStatus().getMapSlotCapacity(); // Yarn only
    LOG.info("Cluster reports {} mapper slots", mappers);
    
    if (options.mappers == -1) { 
      mappers = 8 * mappers; // better accomodate stragglers
    } else {
      mappers = options.mappers;
    }
    if (mappers <= 0) {
      throw new IllegalStateException("Illegal number of mappers: " + mappers);
    }
    options.mappers = mappers;
    
    FileSystem fs = options.outputDir.getFileSystem(job.getConfiguration());
    if (fs.exists(options.outputDir) && !delete(options.outputDir, true, fs)) {
      return -1;
    }
    Path outputResultsDir = new Path(options.outputDir, RESULTS_DIR);
    Path outputReduceDir = new Path(options.outputDir, "reducers");
    Path outputStep1Dir = new Path(options.outputDir, "tmp1");    
    Path outputStep2Dir = new Path(options.outputDir, "tmp2");    
    Path outputTreeMergeStep = new Path(options.outputDir, "mtree-merge-output");
    Path fullInputList = new Path(outputStep1Dir, FULL_INPUT_LIST);
    
    LOG.debug("Creating list of input files for mappers: {}", fullInputList);
    long numFiles = addInputFiles(options.inputFiles, options.inputLists, fullInputList, job.getConfiguration());
    if (numFiles == 0) {
      LOG.info("No input files found - nothing to process");
      return 0;
    }
    int numLinesPerSplit = (int) ceilDivide(numFiles, mappers);
    if (numLinesPerSplit < 0) { // numeric overflow from downcasting long to int?
      numLinesPerSplit = Integer.MAX_VALUE;
    }
    numLinesPerSplit = Math.max(1, numLinesPerSplit);

    int realMappers = Math.min(mappers, (int) ceilDivide(numFiles, numLinesPerSplit));
    calculateNumReducers(options, realMappers);
    int reducers = options.reducers;
    LOG.info("Using these parameters: " +
        "numFiles: {}, mappers: {}, realMappers: {}, reducers: {}, shards: {}, fanout: {}, maxSegments: {}",
        new Object[] {numFiles, mappers, realMappers, reducers, options.shards, options.fanout, options.maxSegments});
        
    
    LOG.info("Randomizing list of {} input files to spread indexing load more evenly among mappers", numFiles);
    long startTime = System.currentTimeMillis();      
    if (numFiles < job.getConfiguration().getInt(MAIN_MEMORY_RANDOMIZATION_THRESHOLD, 100001)) {
      // If there are few input files reduce latency by directly running main memory randomization 
      // instead of launching a high latency MapReduce job
      randomizeFewInputFiles(fs, outputStep2Dir, fullInputList);
    } else {
      // Randomize using a MapReduce job. Use sequential algorithm below a certain threshold because there's no
      // benefit in using many parallel mapper tasks just to randomize the order of a few lines each
      int numLinesPerRandomizerSplit = Math.max(10 * 1000 * 1000, numLinesPerSplit);
      Job randomizerJob = randomizeManyInputFiles(getConf(), fullInputList, outputStep2Dir, numLinesPerRandomizerSplit);
      if (!waitForCompletion(randomizerJob, options.isVerbose)) {
        return -1; // job failed
      }
    }
    float secs = (System.currentTimeMillis() - startTime) / 1000.0f;
    LOG.info("Done. Randomizing list of {} input files took {} secs", numFiles, secs);
    
    
    job.setInputFormatClass(NLineInputFormat.class);
    NLineInputFormat.addInputPath(job, outputStep2Dir);
    NLineInputFormat.setNumLinesPerSplit(job, numLinesPerSplit);    
    FileOutputFormat.setOutputPath(job, outputReduceDir);
    
    String mapperClass = job.getConfiguration().get(JobContext.MAP_CLASS_ATTR);
    if (mapperClass == null) { // enable customization
      Class clazz = MorphlineMapper.class;
      mapperClass = clazz.getName();
      job.setMapperClass(clazz);
    }
    job.setJobName(getClass().getName() + "/" + Utils.getShortClassName(mapperClass));
    
    if (job.getConfiguration().get(JobContext.REDUCE_CLASS_ATTR) == null) { // enable customization
      job.setReducerClass(SolrReducer.class);
    }
    if (options.updateConflictResolver == null) {
      throw new IllegalArgumentException("updateConflictResolver must not be null");
    }
    job.getConfiguration().set(SolrReducer.UPDATE_CONFLICT_RESOLVER, options.updateConflictResolver);
    
    if (options.zkHost != null) {
      assert options.collection != null;
      /*
       * MapReduce partitioner that partitions the Mapper output such that each
       * SolrInputDocument gets sent to the SolrCloud shard that it would have
       * been sent to if the document were ingested via the standard SolrCloud
       * Near Real Time (NRT) API.
       * 
       * In other words, this class implements the same partitioning semantics
       * as the standard SolrCloud NRT API. This enables to mix batch updates
       * from MapReduce ingestion with updates from standard NRT ingestion on
       * the same SolrCloud cluster, using identical unique document keys.
       */
      if (job.getConfiguration().get(JobContext.PARTITIONER_CLASS_ATTR) == null) { // enable customization
        job.setPartitionerClass(SolrCloudPartitioner.class);
      }
      job.getConfiguration().set(SolrCloudPartitioner.ZKHOST, options.zkHost);
      job.getConfiguration().set(SolrCloudPartitioner.COLLECTION, options.collection);
    }
    job.getConfiguration().setInt(SolrCloudPartitioner.SHARDS, options.shards);

    job.setOutputFormatClass(SolrOutputFormat.class);
    if (options.solrHomeDir != null) {
      SolrOutputFormat.setupSolrHomeCache(options.solrHomeDir, job);
    } else {
      assert options.zkHost != null;
      // use the config that this collection uses for the SolrHomeCache.
      ZooKeeperInspector zki = new ZooKeeperInspector();
      SolrZkClient zkClient = zki.getZkClient(options.zkHost);
      try {
        String configName = zki.readConfigName(zkClient, options.collection);
        File tmpSolrHomeDir = zki.downloadConfigDir(zkClient, configName);
        SolrOutputFormat.setupSolrHomeCache(tmpSolrHomeDir, job);
        options.solrHomeDir = tmpSolrHomeDir;
      } finally {
        zkClient.close();
      }
    }
    
    MorphlineMapRunner runner = setupMorphline(options);
    if (options.isDryRun && runner != null) {
      LOG.info("Indexing {} files in dryrun mode", numFiles);
      startTime = System.currentTimeMillis();
      dryRun(runner, fs, fullInputList);
      secs = (System.currentTimeMillis() - startTime) / 1000.0f;
      LOG.info("Done. Indexing {} files in dryrun mode took {} secs", numFiles, secs);
      goodbye(null, programStartTime);
      return 0;
    }          
    job.getConfiguration().set(MorphlineMapRunner.MORPHLINE_FILE_PARAM, options.morphlineFile.getName());

    job.setNumReduceTasks(reducers);  
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(SolrInputDocumentWritable.class);
    LOG.info("Indexing {} files using {} real mappers into {} reducers", new Object[] {numFiles, realMappers, reducers});
    startTime = System.currentTimeMillis();
    if (!waitForCompletion(job, options.isVerbose)) {
      return -1; // job failed
    }

    secs = (System.currentTimeMillis() - startTime) / 1000.0f;
    LOG.info("Done. Indexing {} files using {} real mappers into {} reducers took {} secs", new Object[] {numFiles, realMappers, reducers, secs});

    int mtreeMergeIterations = 0;
    if (reducers > options.shards) {
      mtreeMergeIterations = (int) Math.round(log(options.fanout, reducers / options.shards));
    }
    LOG.debug("MTree merge iterations to do: {}", mtreeMergeIterations);
    int mtreeMergeIteration = 1;
    while (reducers > options.shards) { // run a mtree merge iteration
      job = Job.getInstance(getConf());
      job.setJarByClass(getClass());
      job.setJobName(getClass().getName() + "/" + Utils.getShortClassName(TreeMergeMapper.class));
      job.setMapperClass(TreeMergeMapper.class);
      job.setOutputFormatClass(TreeMergeOutputFormat.class);
      job.setNumReduceTasks(0);  
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(NullWritable.class);    
      job.setInputFormatClass(NLineInputFormat.class);
      
      Path inputStepDir = new Path(options.outputDir, "mtree-merge-input-iteration" + mtreeMergeIteration);
      fullInputList = new Path(inputStepDir, FULL_INPUT_LIST);    
      LOG.debug("MTree merge iteration {}/{}: Creating input list file for mappers {}", new Object[] {mtreeMergeIteration, mtreeMergeIterations, fullInputList});
      numFiles = createTreeMergeInputDirList(outputReduceDir, fs, fullInputList);    
      if (numFiles != reducers) {
        throw new IllegalStateException("Not same reducers: " + reducers + ", numFiles: " + numFiles);
      }
      NLineInputFormat.addInputPath(job, fullInputList);
      NLineInputFormat.setNumLinesPerSplit(job, options.fanout);    
      FileOutputFormat.setOutputPath(job, outputTreeMergeStep);
      
      LOG.info("MTree merge iteration {}/{}: Merging {} shards into {} shards using fanout {}", new Object[] { 
          mtreeMergeIteration, mtreeMergeIterations, reducers, (reducers / options.fanout), options.fanout});
      startTime = System.currentTimeMillis();
      if (!waitForCompletion(job, options.isVerbose)) {
        return -1; // job failed
      }
      if (!renameTreeMergeShardDirs(outputTreeMergeStep, job, fs)) {
        return -1;
      }
      secs = (System.currentTimeMillis() - startTime) / 1000.0f;
      LOG.info("MTree merge iteration {}/{}: Done. Merging {} shards into {} shards using fanout {} took {} secs",
          new Object[] {mtreeMergeIteration, mtreeMergeIterations, reducers, (reducers / options.fanout), options.fanout, secs});
      
      if (!delete(outputReduceDir, true, fs)) {
        return -1;
      }
      if (!rename(outputTreeMergeStep, outputReduceDir, fs)) {
        return -1;
      }
      assert reducers % options.fanout == 0;
      reducers = reducers / options.fanout;
      mtreeMergeIteration++;
    }
    assert reducers == options.shards;
    
    // normalize output shard dir prefix, i.e.
    // rename part-r-00000 to part-00000 (stems from zero tree merge iterations)
    // rename part-m-00000 to part-00000 (stems from > 0 tree merge iterations)
    for (FileStatus stats : fs.listStatus(outputReduceDir)) {
      String dirPrefix = SolrOutputFormat.getOutputName(job);
      Path srcPath = stats.getPath();
      if (stats.isDirectory() && srcPath.getName().startsWith(dirPrefix)) {
        String dstName = dirPrefix + srcPath.getName().substring(dirPrefix.length() + "-m".length());
        Path dstPath = new Path(srcPath.getParent(), dstName);
        if (!rename(srcPath, dstPath, fs)) {
          return -1;
        }        
      }
    };    
    
    // publish results dir    
    if (!rename(outputReduceDir, outputResultsDir, fs)) {
      return -1;
    }

    if (options.goLive && !new GoLive().goLive(options, listSortedOutputShardDirs(outputResultsDir, fs))) {
      return -1;
    }
    
    goodbye(job, programStartTime);    
    return 0;
  }

  private void calculateNumReducers(Options options, int realMappers) throws IOException {
    if (options.shards <= 0) {
      throw new IllegalStateException("Illegal number of shards: " + options.shards);
    }
    if (options.fanout <= 1) {
      throw new IllegalStateException("Illegal fanout: " + options.fanout);
    }
    if (realMappers <= 0) {
      throw new IllegalStateException("Illegal realMappers: " + realMappers);
    }
    

    int reducers = new JobClient(job.getConfiguration()).getClusterStatus().getMaxReduceTasks(); // MR1
    //reducers = job.getCluster().getClusterStatus().getReduceSlotCapacity(); // Yarn only      
    LOG.info("Cluster reports {} reduce slots", reducers);

    if (options.reducers == 0) {
      reducers = options.shards;
    } else if (options.reducers == -1) {
      reducers = Math.min(reducers, realMappers); // no need to use many reducers when using few mappers
    } else {
      reducers = options.reducers;
    }
    reducers = Math.max(reducers, options.shards);
    
    if (reducers != options.shards) {
      // Ensure fanout isn't misconfigured. fanout can't meaningfully be larger than what would be 
      // required to merge all leaf shards in one single tree merge iteration into root shards
      options.fanout = Math.min(options.fanout, (int) ceilDivide(reducers, options.shards));
      
      // Ensure invariant reducers == options.shards * (fanout ^ N) where N is an integer >= 1.
      // N is the number of mtree merge iterations.
      // This helps to evenly spread docs among root shards and simplifies the impl of the mtree merge algorithm.
      int s = options.shards;
      while (s < reducers) { 
        s = s * options.fanout;
      }
      reducers = s;
      assert reducers % options.fanout == 0;
    }
    options.reducers = reducers;
  }
  
  private long addInputFiles(List<Path> inputFiles, List<Path> inputLists, Path fullInputList, Configuration conf) 
      throws IOException {
    
    long numFiles = 0;
    FileSystem fs = fullInputList.getFileSystem(conf);
    FSDataOutputStream out = fs.create(fullInputList);
    try {
      Writer writer = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"));
      
      for (Path inputFile : inputFiles) {
        FileSystem inputFileFs = inputFile.getFileSystem(conf);
        if (inputFileFs.exists(inputFile)) {
          PathFilter pathFilter = new PathFilter() {      
            @Override
            public boolean accept(Path path) {
              return !path.getName().startsWith("."); // ignore "hidden" files and dirs
            }
          };
          numFiles += addInputFilesRecursively(inputFile, writer, inputFileFs, pathFilter);
        }
      }

      for (Path inputList : inputLists) {
        InputStream in;
        if (inputList.toString().equals("-")) {
          in = System.in;
        } else if (inputList.isAbsoluteAndSchemeAuthorityNull()) {
          in = new BufferedInputStream(new FileInputStream(inputList.toString()));
        } else {
          in = inputList.getFileSystem(conf).open(inputList);
        }
        try {
          BufferedReader reader = new BufferedReader(new InputStreamReader(in, "UTF-8"));
          String line;
          while ((line = reader.readLine()) != null) {
            writer.write(line + "\n");
            numFiles++;
          }
          reader.close();
        } finally {
          in.close();
        }
      }
      
      writer.close();
    } finally {
      out.close();
    }    
    return numFiles;
  }
  
  /**
   * Add the specified file to the input set, if path is a directory then
   * add the files contained therein.
   */
  private long addInputFilesRecursively(Path path, Writer writer, FileSystem fs, PathFilter pathFilter) throws IOException {
    long numFiles = 0;
    for (FileStatus stat : fs.listStatus(path, pathFilter)) {
      LOG.debug("Adding path {}", stat.getPath());
      if (stat.isDirectory()) {
        numFiles += addInputFilesRecursively(stat.getPath(), writer, fs, pathFilter);
      } else {
        writer.write(stat.getPath().toString() + "\n");
        numFiles++;
      }
    }
    return numFiles;
  }
  
  private void randomizeFewInputFiles(FileSystem fs, Path outputStep2Dir, Path fullInputList) throws IOException {    
    List<String> lines = new ArrayList();
    BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(fullInputList), "UTF-8"));
    try {
      String line;
      while ((line = reader.readLine()) != null) {
        lines.add(line);
      }
    } finally {
      reader.close();
    }
    
    Collections.shuffle(lines, new Random(421439783L)); // constant seed for reproducability
    
    FSDataOutputStream out = fs.create(new Path(outputStep2Dir, FULL_INPUT_LIST));
    Writer writer = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"));
    try {
      for (String line : lines) {
        writer.write(line + "\n");
      } 
    } finally {
      writer.close();
    }
  }

  /**
   * To uniformly spread load across all mappers we randomize fullInputList
   * with a separate small Mapper & Reducer preprocessing step. This way
   * each input line ends up on a random position in the output file list.
   * Each mapper indexes a disjoint consecutive set of files such that each
   * set has roughly the same size, at least from a probabilistic
   * perspective.
   * 
   * For example an input file with the following input list of URLs:
   * 
   * A
   * B
   * C
   * D
   * 
   * might be randomized into the following output list of URLs:
   * 
   * C
   * A
   * D
   * B
   * 
   * The implementation sorts the list of lines by randomly generated numbers.
   */
  private Job randomizeManyInputFiles(Configuration baseConfig, Path fullInputList, Path outputStep2Dir, int numLinesPerSplit) 
      throws IOException {
    
    Job job2 = Job.getInstance(baseConfig);
    job2.setJarByClass(getClass());
    job2.setJobName(getClass().getName() + "/" + Utils.getShortClassName(LineRandomizerMapper.class));
    job2.setInputFormatClass(NLineInputFormat.class);
    NLineInputFormat.addInputPath(job2, fullInputList);
    NLineInputFormat.setNumLinesPerSplit(job2, numLinesPerSplit);          
    job2.setMapperClass(LineRandomizerMapper.class);
    job2.setReducerClass(LineRandomizerReducer.class);
    job2.setOutputFormatClass(TextOutputFormat.class);
    FileOutputFormat.setOutputPath(job2, outputStep2Dir);
    job2.setNumReduceTasks(1);
    job2.setOutputKeyClass(LongWritable.class);
    job2.setOutputValueClass(Text.class);
    return job2;
  }

  // do the same as if the user had typed 'hadoop ... --files <file>' 
  private void addDistributedCacheFile(File file, Configuration conf) throws IOException {
    String HADOOP_TMP_FILES = "tmpfiles"; // see Hadoop's GenericOptionsParser
    String tmpFiles = conf.get(HADOOP_TMP_FILES, "");
    if (tmpFiles.length() > 0) { // already present?
      tmpFiles = tmpFiles + ","; 
    }
    GenericOptionsParser parser = new GenericOptionsParser(
        new Configuration(conf), 
        new String[] { "--files", file.getCanonicalPath() });
    String additionalTmpFiles = parser.getConfiguration().get(HADOOP_TMP_FILES);
    assert additionalTmpFiles != null;
    assert additionalTmpFiles.length() > 0;
    tmpFiles += additionalTmpFiles;
    conf.set(HADOOP_TMP_FILES, tmpFiles);
  }
  
  private MorphlineMapRunner setupMorphline(Options options) throws IOException, URISyntaxException {
    if (options.morphlineId != null) {
      job.getConfiguration().set(MorphlineMapRunner.MORPHLINE_ID_PARAM, options.morphlineId);
    }
    addDistributedCacheFile(options.morphlineFile, job.getConfiguration());    
    if (!options.isDryRun) {
      return null;
    }
    
    /*
     * Ensure scripting support for Java via morphline "java" command works even in dryRun mode,
     * i.e. when executed in the client side driver JVM. To do so, collect all classpath URLs from
     * the class loaders chain that org.apache.hadoop.util.RunJar (hadoop jar xyz-job.jar) and
     * org.apache.hadoop.util.GenericOptionsParser (--libjars) have installed, then tell
     * FastJavaScriptEngine.parse() where to find classes that JavaBuilder scripts might depend on.
     * This ensures that scripts that reference external java classes compile without exceptions
     * like this:
     * 
     * ... caused by compilation failed: mfm:///MyJavaClass1.java:2: package
     * com.cloudera.cdk.morphline.api does not exist
     */
    LOG.trace("dryRun: java.class.path: {}", System.getProperty("java.class.path"));
    String fullClassPath = "";
    ClassLoader loader = Thread.currentThread().getContextClassLoader(); // see org.apache.hadoop.util.RunJar
    while (loader != null) { // walk class loaders, collect all classpath URLs
      if (loader instanceof URLClassLoader) { 
        URL[] classPathPartURLs = ((URLClassLoader) loader).getURLs(); // see org.apache.hadoop.util.RunJar
        LOG.trace("dryRun: classPathPartURLs: {}", Arrays.asList(classPathPartURLs));
        StringBuilder classPathParts = new StringBuilder();
        for (URL url : classPathPartURLs) {
          File file = new File(url.toURI());
          if (classPathPartURLs.length > 0) {
            classPathParts.append(File.pathSeparator);
          }
          classPathParts.append(file.getPath());
        }
        LOG.trace("dryRun: classPathParts: {}", classPathParts);
        String separator = File.pathSeparator;
        if (fullClassPath.length() == 0 || classPathParts.length() == 0) {
          separator = "";
        }
        fullClassPath = classPathParts + separator + fullClassPath;
      }
      loader = loader.getParent();
    }
    
    // tell FastJavaScriptEngine.parse() where to find the classes that the script might depend on
    if (fullClassPath.length() > 0) {
      assert System.getProperty("java.class.path") != null;
      fullClassPath = System.getProperty("java.class.path") + File.pathSeparator + fullClassPath;
      LOG.trace("dryRun: fullClassPath: {}", fullClassPath);
      System.setProperty("java.class.path", fullClassPath); // see FastJavaScriptEngine.parse()
    }
    
    job.getConfiguration().set(MorphlineMapRunner.MORPHLINE_FILE_PARAM, options.morphlineFile.getPath());
    return new MorphlineMapRunner(
        job.getConfiguration(), new DryRunDocumentLoader(), options.solrHomeDir.getPath());
  }
  
  /*
   * Executes the morphline in the current process (without submitting a job to MR) for quicker
   * turnaround during trial & debug sessions
   */
  private void dryRun(MorphlineMapRunner runner, FileSystem fs, Path fullInputList) throws IOException {    
    BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(fullInputList), "UTF-8"));
    try {
      String line;
      while ((line = reader.readLine()) != null) {
        runner.map(line, job.getConfiguration(), null);
      }
      runner.cleanup();
    } finally {
      reader.close();
    }
  }
  
  private int createTreeMergeInputDirList(Path outputReduceDir, FileSystem fs, Path fullInputList)
      throws FileNotFoundException, IOException {
    
    FileStatus[] dirs = listSortedOutputShardDirs(outputReduceDir, fs);
    int numFiles = 0;
    FSDataOutputStream out = fs.create(fullInputList);
    try {
      Writer writer = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"));
      for (FileStatus stat : dirs) {
        LOG.debug("Adding path {}", stat.getPath());
        Path dir = new Path(stat.getPath(), "data/index");
        if (!fs.isDirectory(dir)) {
          throw new IllegalStateException("Not a directory: " + dir);
        }
        writer.write(dir.toString() + "\n");
        numFiles++;
      }
      writer.close();
    } finally {
      out.close();
    }
    return numFiles;
  }

  private FileStatus[] listSortedOutputShardDirs(Path outputReduceDir, FileSystem fs) throws FileNotFoundException,
      IOException {
    
    final String dirPrefix = SolrOutputFormat.getOutputName(job);
    FileStatus[] dirs = fs.listStatus(outputReduceDir, new PathFilter() {      
      @Override
      public boolean accept(Path path) {
        return path.getName().startsWith(dirPrefix);
      }
    });
    for (FileStatus dir : dirs) {
      if (!dir.isDirectory()) {
        throw new IllegalStateException("Not a directory: " + dir.getPath());
      }
    }
    
    // use alphanumeric sort (rather than lexicographical sort) to properly handle more than 99999 shards
    Arrays.sort(dirs, new Comparator<FileStatus>() {
      @Override
      public int compare(FileStatus f1, FileStatus f2) {
        return new AlphaNumericComparator().compare(f1.getPath().getName(), f2.getPath().getName());
      }
    });

    return dirs;
  }

  /*
   * You can run MapReduceIndexerTool in Solrcloud mode, and once the MR job completes, you can use
   * the standard solrj Solrcloud API to send doc updates and deletes to SolrCloud, and those updates
   * and deletes will go to the right Solr shards, and it will work just fine.
   * 
   * The MapReduce framework doesn't guarantee that input split N goes to the map task with the
   * taskId = N. The job tracker and Yarn schedule and assign tasks, considering data locality
   * aspects, but without regard of the input split# withing the overall list of input splits. In
   * other words, split# != taskId can be true.
   * 
   * To deal with this issue, our mapper tasks write a little auxiliary metadata file (per task)
   * that tells the job driver which taskId processed which split#. Once the mapper-only job is
   * completed, the job driver renames the output dirs such that the dir name contains the true solr
   * shard id, based on these auxiliary files.
   * 
   * This way each doc gets assigned to the right Solr shard even with #reducers > #solrshards
   * 
   * Example for a merge with two shards:
   * 
   * part-m-00000 and part-m-00001 goes to outputShardNum = 0 and will end up in merged part-m-00000
   * part-m-00002 and part-m-00003 goes to outputShardNum = 1 and will end up in merged part-m-00001
   * part-m-00004 and part-m-00005 goes to outputShardNum = 2 and will end up in merged part-m-00002
   * ... and so on
   * 
   * Also see run() method above where it uses NLineInputFormat.setNumLinesPerSplit(job,
   * options.fanout)
   * 
   * Also see TreeMergeOutputFormat.TreeMergeRecordWriter.writeShardNumberFile()
   */
  private boolean renameTreeMergeShardDirs(Path outputTreeMergeStep, Job job, FileSystem fs) throws IOException {
    final String dirPrefix = SolrOutputFormat.getOutputName(job);
    FileStatus[] dirs = fs.listStatus(outputTreeMergeStep, new PathFilter() {      
      @Override
      public boolean accept(Path path) {
        return path.getName().startsWith(dirPrefix);
      }
    });
    
    for (FileStatus dir : dirs) {
      if (!dir.isDirectory()) {
        throw new IllegalStateException("Not a directory: " + dir.getPath());
      }
    }

    // Example: rename part-m-00004 to _part-m-00004
    for (FileStatus dir : dirs) {
      Path path = dir.getPath();
      Path renamedPath = new Path(path.getParent(), "_" + path.getName());
      if (!rename(path, renamedPath, fs)) {
        return false;
      }
    }
    
    // Example: rename _part-m-00004 to part-m-00002
    for (FileStatus dir : dirs) {
      Path path = dir.getPath();
      Path renamedPath = new Path(path.getParent(), "_" + path.getName());
      
      // read auxiliary metadata file (per task) that tells which taskId 
      // processed which split# aka solrShard
      Path solrShardNumberFile = new Path(renamedPath, TreeMergeMapper.SOLR_SHARD_NUMBER);
      InputStream in = fs.open(solrShardNumberFile);
      byte[] bytes = ByteStreams.toByteArray(in);
      in.close();
      Preconditions.checkArgument(bytes.length > 0);
      int solrShard = Integer.parseInt(new String(bytes, Charsets.UTF_8));
      if (!delete(solrShardNumberFile, false, fs)) {
        return false;
      }
      
      // same as FileOutputFormat.NUMBER_FORMAT
      NumberFormat numberFormat = NumberFormat.getInstance(Locale.ENGLISH);
      numberFormat.setMinimumIntegerDigits(5);
      numberFormat.setGroupingUsed(false);
      Path finalPath = new Path(renamedPath.getParent(), dirPrefix + "-m-" + numberFormat.format(solrShard));
      
      LOG.info("MTree merge renaming solr shard: " + solrShard + " from dir: " + dir.getPath() + " to dir: " + finalPath);
      if (!rename(renamedPath, finalPath, fs)) {
        return false;
      }
    }
    return true;
  }

  private static void verifyGoLiveArgs(Options opts, ArgumentParser parser) throws ArgumentParserException {
    if (opts.zkHost == null && opts.solrHomeDir == null) {
      throw new ArgumentParserException("At least one of --zk-host or --solr-home-dir is required", parser);
    }
    if (opts.goLive && opts.zkHost == null && opts.shardUrls == null) {
      throw new ArgumentParserException("--go-live requires that you also pass --shard-url or --zk-host", parser);
    }
    
    if (opts.zkHost != null && opts.collection == null) {
      throw new ArgumentParserException("--zk-host requires that you also pass --collection", parser);
    }
    
    if (opts.zkHost != null) {
      return;
      // verify structure of ZK directory later, to avoid checking run-time errors during parsing.
    } else if (opts.shardUrls != null) {
      if (opts.shardUrls.size() == 0) {
        throw new ArgumentParserException("--shard-url requires at least one URL", parser);
      }
    } else if (opts.shards != null) {
      if (opts.shards <= 0) {
        throw new ArgumentParserException("--shards must be a positive number: " + opts.shards, parser);
      }
    } else {
      throw new ArgumentParserException("You must specify one of the following (mutually exclusive) arguments: "
          + "--zk-host or --shard-url or --shards", parser);
    }

    if (opts.shardUrls != null) {
      opts.shards = opts.shardUrls.size();
    }
    
    assert opts.shards != null;
    assert opts.shards > 0;
  }

  private static void verifyZKStructure(Options opts, ArgumentParser parser) throws ArgumentParserException {
    if (opts.zkHost != null) {
      assert opts.collection != null;
      ZooKeeperInspector zki = new ZooKeeperInspector();
      try {
        opts.shardUrls = zki.extractShardUrls(opts.zkHost, opts.collection);
      } catch (Exception e) {
        LOG.debug("Cannot extract SolrCloud shard URLs from ZooKeeper", e);
        throw new ArgumentParserException(e, parser);
      }
      assert opts.shardUrls != null;
      if (opts.shardUrls.size() == 0) {
        throw new ArgumentParserException("--zk-host requires ZooKeeper " + opts.zkHost
          + " to contain at least one SolrCore for collection: " + opts.collection, parser);
      }
      opts.shards = opts.shardUrls.size();
      LOG.debug("Using SolrCloud shard URLs: {}", opts.shardUrls);
    }
  }

  private boolean waitForCompletion(Job job, boolean isVerbose) 
      throws IOException, InterruptedException, ClassNotFoundException {
    
    LOG.debug("Running job: " + getJobInfo(job));
    boolean success = job.waitForCompletion(isVerbose);
    if (!success) {
      LOG.error("Job failed! " + getJobInfo(job));
    }
    return success;
  }

  private void goodbye(Job job, long startTime) {
    float secs = (System.currentTimeMillis() - startTime) / 1000.0f;
    if (job != null) {
      LOG.info("Succeeded with job: " + getJobInfo(job));
    }
    LOG.info("Success. Done. Program took {} secs. Goodbye.", secs);
  }

  private String getJobInfo(Job job) {
    return "jobName: " + job.getJobName() + ", jobId: " + job.getJobID();
  }
  
  private boolean rename(Path src, Path dst, FileSystem fs) throws IOException {
    boolean success = fs.rename(src, dst);
    if (!success) {
      LOG.error("Cannot rename " + src + " to " + dst);
    }
    return success;
  }
  
  private boolean delete(Path path, boolean recursive, FileSystem fs) throws IOException {
    boolean success = fs.delete(path, recursive);
    if (!success) {
      LOG.error("Cannot delete " + path);
    }
    return success;
  }

  // same as IntMath.divide(p, q, RoundingMode.CEILING)
  private long ceilDivide(long p, long q) {
    long result = p / q;
    if (p % q != 0) {
      result++;
    }
    return result;
  }
  
  /**
   * Returns <tt>log<sub>base</sub>value</tt>.
   */
  private double log(double base, double value) {
    return Math.log(value) / Math.log(base);
  }

}
