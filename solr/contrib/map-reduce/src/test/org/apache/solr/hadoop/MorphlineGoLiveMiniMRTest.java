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

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.lang.reflect.Array;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.util.JarFinder;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.cloud.AbstractZkTestCase;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.hadoop.hack.MiniMRClientCluster;
import org.apache.solr.hadoop.hack.MiniMRClientClusterFactory;
import org.apache.solr.morphlines.solr.AbstractSolrMorphlineTestBase;
import org.apache.solr.util.BadHdfsThreadsFilter;
import org.apache.solr.util.BadMrClusterThreadsFilter;
import org.apache.solr.util.TimeOut;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.carrotsearch.randomizedtesting.annotations.Nightly;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakAction;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakAction.Action;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakZombies;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakZombies.Consequence;

@ThreadLeakAction({Action.WARN})
@ThreadLeakLingering(linger = 0)
@ThreadLeakZombies(Consequence.CONTINUE)
@ThreadLeakFilters(defaultFilters = true, filters = {
    BadHdfsThreadsFilter.class, BadMrClusterThreadsFilter.class // hdfs currently leaks thread(s)
})
@SuppressSSL // SSL does not work with this test for currently unknown reasons
@Slow
@Nightly
public class MorphlineGoLiveMiniMRTest extends AbstractFullDistribZkTestBase {
  
  private static final int RECORD_COUNT = 2104;
  private static final String RESOURCES_DIR = getFile("morphlines-core.marker").getParent();  
  private static final String DOCUMENTS_DIR = RESOURCES_DIR + "/test-documents";
  private static final File MINIMR_INSTANCE_DIR = new File(RESOURCES_DIR + "/solr/minimr");
  private static final File MINIMR_CONF_DIR = new File(RESOURCES_DIR + "/solr/minimr");
  
  private static String SEARCH_ARCHIVES_JAR;
  
  private static MiniDFSCluster dfsCluster = null;
  private static MiniMRClientCluster mrCluster = null;
  private static String tempDir;
 
  private final String inputAvroFile1;
  private final String inputAvroFile2;
  private final String inputAvroFile3;

  private static File solrHomeDirectory;

  @Override
  public String getSolrHome() {
    return solrHomeDirectory.getPath();
  }
  
  public MorphlineGoLiveMiniMRTest() {
    this.inputAvroFile1 = "sample-statuses-20120521-100919.avro";
    this.inputAvroFile2 = "sample-statuses-20120906-141433.avro";
    this.inputAvroFile3 = "sample-statuses-20120906-141433-medium.avro";

    sliceCount = TEST_NIGHTLY ? 5 : 3;
    fixShardCount(TEST_NIGHTLY ? 5 : 3);
  }
  
  @BeforeClass
  public static void setupClass() throws Exception {
    System.setProperty("solr.hdfs.blockcache.global", Boolean.toString(LuceneTestCase.random().nextBoolean()));
    System.setProperty("solr.hdfs.blockcache.enabled", Boolean.toString(LuceneTestCase.random().nextBoolean()));
    System.setProperty("solr.hdfs.blockcache.blocksperbank", "2048");
    
    solrHomeDirectory = createTempDir().toFile();

    assumeFalse("HDFS tests were disabled by -Dtests.disableHdfs",
        Boolean.parseBoolean(System.getProperty("tests.disableHdfs", "false")));
    
    assumeFalse("FIXME: This test does not work with Windows because of native library requirements", Constants.WINDOWS);
    
    AbstractZkTestCase.SOLRHOME = solrHomeDirectory;
    FileUtils.copyDirectory(MINIMR_INSTANCE_DIR, AbstractZkTestCase.SOLRHOME);
    tempDir = createTempDir().toFile().getAbsolutePath();

    new File(tempDir).mkdirs();

    FileUtils.copyFile(new File(RESOURCES_DIR + "/custom-mimetypes.xml"), new File(tempDir + "/custom-mimetypes.xml"));
    
    AbstractSolrMorphlineTestBase.setupMorphline(tempDir, "test-morphlines/solrCellDocumentTypes", true);
    
    
    System.setProperty("hadoop.log.dir", new File(tempDir, "logs").getAbsolutePath());
    
    int dataNodes = 2;
    
    JobConf conf = new JobConf();
    conf.set("dfs.block.access.token.enable", "false");
    conf.set("dfs.permissions", "true");
    conf.set("hadoop.security.authentication", "simple");
    conf.set("mapreduce.jobhistory.minicluster.fixed.ports", "false");
    conf.set("mapreduce.jobhistory.admin.address", "0.0.0.0:0");
    
    conf.set(YarnConfiguration.NM_LOCAL_DIRS, tempDir + File.separator +  "nm-local-dirs");
    conf.set(YarnConfiguration.DEFAULT_NM_LOG_DIRS, tempDir + File.separator +  "nm-logs");

    
    new File(tempDir + File.separator +  "nm-local-dirs").mkdirs();
    
    System.setProperty("test.build.dir", tempDir + File.separator + "hdfs" + File.separator + "test-build-dir");
    System.setProperty("test.build.data", tempDir + File.separator + "hdfs" + File.separator + "build");
    System.setProperty("test.cache.data", tempDir + File.separator + "hdfs" + File.separator + "cache");

    // Initialize AFTER test.build.dir is set, JarFinder uses it.
    SEARCH_ARCHIVES_JAR = JarFinder.getJar(MapReduceIndexerTool.class);
    
    dfsCluster = new MiniDFSCluster(conf, dataNodes, true, null);
    FileSystem fileSystem = dfsCluster.getFileSystem();
    fileSystem.mkdirs(new Path("/tmp"));
    fileSystem.mkdirs(new Path("/user"));
    fileSystem.mkdirs(new Path("/hadoop/mapred/system"));
    fileSystem.setPermission(new Path("/tmp"),
        FsPermission.valueOf("-rwxrwxrwx"));
    fileSystem.setPermission(new Path("/user"),
        FsPermission.valueOf("-rwxrwxrwx"));
    fileSystem.setPermission(new Path("/hadoop/mapred/system"),
        FsPermission.valueOf("-rwx------"));
    
    mrCluster = MiniMRClientClusterFactory.create(MorphlineGoLiveMiniMRTest.class, 1, conf, new File(tempDir, "mrCluster")); 
        
        //new MiniMRCluster(0, 0, taskTrackers, nnURI, numDirs, racks,
        //hosts, null, conf);

    ProxyUsers.refreshSuperUserGroupsConfiguration(conf);
  }
  
  @Override
  public void distribSetUp() throws Exception {
    super.distribSetUp();
    System.setProperty("host", "127.0.0.1");
    System.setProperty("numShards", Integer.toString(sliceCount));
    URI uri = dfsCluster.getFileSystem().getUri();
    System.setProperty("solr.hdfs.home",  uri.toString() + "/" + this.getClass().getName());
    uploadConfFiles();
  }
  
  @Override
  public void distribTearDown() throws Exception {
    super.distribTearDown();
    System.clearProperty("host");
    System.clearProperty("numShards");
    System.clearProperty("solr.hdfs.home");
  }
  
  @AfterClass
  public static void teardownClass() throws Exception {
    System.clearProperty("solr.hdfs.blockcache.global");
    System.clearProperty("solr.hdfs.blockcache.blocksperbank");
    System.clearProperty("solr.hdfs.blockcache.enabled");
    System.clearProperty("hadoop.log.dir");
    System.clearProperty("test.build.dir");
    System.clearProperty("test.build.data");
    System.clearProperty("test.cache.data");
    
    if (mrCluster != null) {
      mrCluster.stop();
      mrCluster = null;
    }
    if (dfsCluster != null) {
      dfsCluster.shutdown();
      dfsCluster = null;
    }
    FileSystem.closeAll();
  }
  
  private JobConf getJobConf() throws IOException {
    JobConf jobConf = new JobConf(mrCluster.getConfig());
    return jobConf;
  }
  
  @Test
  public void testBuildShardUrls() throws Exception {
    // 2x3
    Integer numShards = 2;
    List<Object> urls = new ArrayList<>();
    urls.add("shard1");
    urls.add("shard2");
    urls.add("shard3");
    urls.add("shard4");
    urls.add("shard5");
    urls.add("shard6");
    List<List<String>> shardUrls = MapReduceIndexerTool.buildShardUrls(urls , numShards);
    
    assertEquals(shardUrls.toString(), 2, shardUrls.size());
    
    for (List<String> u : shardUrls) {
      assertEquals(3, u.size());
    }
    
    // 1x6
    numShards = 1;
    shardUrls = MapReduceIndexerTool.buildShardUrls(urls , numShards);
    
    assertEquals(shardUrls.toString(), 1, shardUrls.size());
    
    for (List<String> u : shardUrls) {
      assertEquals(6, u.size());
    }
    
    // 6x1
    numShards = 6;
    shardUrls = MapReduceIndexerTool.buildShardUrls(urls , numShards);
    
    assertEquals(shardUrls.toString(), 6, shardUrls.size());
    
    for (List<String> u : shardUrls) {
      assertEquals(1, u.size());
    }
    
    // 3x2
    numShards = 3;
    shardUrls = MapReduceIndexerTool.buildShardUrls(urls , numShards);
    
    assertEquals(shardUrls.toString(), 3, shardUrls.size());
    
    for (List<String> u : shardUrls) {
      assertEquals(2, u.size());
    }
    
    // null shards, 6x1
    numShards = null;
    shardUrls = MapReduceIndexerTool.buildShardUrls(urls , numShards);
    
    assertEquals(shardUrls.toString(), 6, shardUrls.size());
    
    for (List<String> u : shardUrls) {
      assertEquals(1, u.size());
    }
    
    // null shards 3x1
    numShards = null;
    
    urls = new ArrayList<>();
    urls.add("shard1");
    urls.add("shard2");
    urls.add("shard3");
    
    shardUrls = MapReduceIndexerTool.buildShardUrls(urls , numShards);

    assertEquals(shardUrls.toString(), 3, shardUrls.size());
    
    for (List<String> u : shardUrls) {
      assertEquals(1, u.size());
    }
    
    // 2x(2,3) off balance
    numShards = 2;
    urls = new ArrayList<>();
    urls.add("shard1");
    urls.add("shard2");
    urls.add("shard3");
    urls.add("shard4");
    urls.add("shard5");
    shardUrls = MapReduceIndexerTool.buildShardUrls(urls , numShards);

    assertEquals(shardUrls.toString(), 2, shardUrls.size());
    
    Set<Integer> counts = new HashSet<>();
    counts.add(shardUrls.get(0).size());
    counts.add(shardUrls.get(1).size());
    
    assertTrue(counts.contains(2));
    assertTrue(counts.contains(3));
  }
  
  private String[] prependInitialArgs(String[] args) {
    String[] head = new String[] {
        "--morphline-file=" + tempDir + "/test-morphlines/solrCellDocumentTypes.conf",
        "--morphline-id=morphline1",
    };
    return concat(head, args); 
  }

  @Nightly
  @Test
  public void test() throws Exception {
    
    waitForRecoveriesToFinish(false);
    
    FileSystem fs = dfsCluster.getFileSystem();
    Path inDir = fs.makeQualified(new Path(
        "/user/testing/testMapperReducer/input"));
    fs.delete(inDir, true);
    String DATADIR = "/user/testing/testMapperReducer/data";
    Path dataDir = fs.makeQualified(new Path(DATADIR));
    fs.delete(dataDir, true);
    Path outDir = fs.makeQualified(new Path(
        "/user/testing/testMapperReducer/output"));
    fs.delete(outDir, true);
    
    assertTrue(fs.mkdirs(inDir));
    Path INPATH = upAvroFile(fs, inDir, DATADIR, dataDir, inputAvroFile1);
    
    JobConf jobConf = getJobConf();
    jobConf.set("jobclient.output.filter", "ALL");
    // enable mapred.job.tracker = local to run in debugger and set breakpoints
    // jobConf.set("mapred.job.tracker", "local");
    jobConf.setMaxMapAttempts(1);
    jobConf.setMaxReduceAttempts(1);
    jobConf.setJar(SEARCH_ARCHIVES_JAR);

    MapReduceIndexerTool tool;
    int res;
    QueryResponse results;
    String[] args = new String[]{};
    List<String> argList = new ArrayList<>();

    try (HttpSolrClient server = new HttpSolrClient(cloudJettys.get(0).url)) {

      args = new String[]{
          "--solr-home-dir=" + MINIMR_CONF_DIR.getAbsolutePath(),
          "--output-dir=" + outDir.toString(),
          "--log4j=" + getFile("log4j.properties").getAbsolutePath(),
          "--mappers=3",
          random().nextBoolean() ? "--input-list=" + INPATH.toString() : dataDir.toString(),
          "--go-live-threads", Integer.toString(random().nextInt(15) + 1),
          "--verbose",
          "--go-live"
      };
      args = prependInitialArgs(args);
      getShardUrlArgs(argList);
      args = concat(args, argList.toArray(new String[0]));

      if (true) {
        tool = new MapReduceIndexerTool();
        res = ToolRunner.run(jobConf, tool, args);
        assertEquals(0, res);
        assertTrue(tool.job.isComplete());
        assertTrue(tool.job.isSuccessful());
        results = server.query(new SolrQuery("*:*"));
        assertEquals(20, results.getResults().getNumFound());
      }

      fs.delete(inDir, true);
      fs.delete(outDir, true);
      fs.delete(dataDir, true);
      assertTrue(fs.mkdirs(inDir));
      INPATH = upAvroFile(fs, inDir, DATADIR, dataDir, inputAvroFile2);

      args = new String[]{
          "--solr-home-dir=" + MINIMR_CONF_DIR.getAbsolutePath(),
          "--output-dir=" + outDir.toString(),
          "--mappers=3",
          "--verbose",
          "--go-live",
          random().nextBoolean() ? "--input-list=" + INPATH.toString() : dataDir.toString(),
          "--go-live-threads", Integer.toString(random().nextInt(15) + 1)
      };
      args = prependInitialArgs(args);

      getShardUrlArgs(argList);
      args = concat(args, argList.toArray(new String[0]));

      if (true) {
        tool = new MapReduceIndexerTool();
        res = ToolRunner.run(jobConf, tool, args);
        assertEquals(0, res);
        assertTrue(tool.job.isComplete());
        assertTrue(tool.job.isSuccessful());
        results = server.query(new SolrQuery("*:*"));

        assertEquals(22, results.getResults().getNumFound());
      }

      // try using zookeeper
      String collection = "collection1";
      if (random().nextBoolean()) {
        // sometimes, use an alias
        createAlias("updatealias", "collection1");
        collection = "updatealias";
      }

      fs.delete(inDir, true);
      fs.delete(outDir, true);
      fs.delete(dataDir, true);
      INPATH = upAvroFile(fs, inDir, DATADIR, dataDir, inputAvroFile3);

      cloudClient.deleteByQuery("*:*");
      cloudClient.commit();
      assertEquals(0, cloudClient.query(new SolrQuery("*:*")).getResults().getNumFound());

      args = new String[]{
          "--output-dir=" + outDir.toString(),
          "--mappers=3",
          "--reducers=6",
          "--fanout=2",
          "--verbose",
          "--go-live",
          random().nextBoolean() ? "--input-list=" + INPATH.toString() : dataDir.toString(),
          "--zk-host", zkServer.getZkAddress(),
          "--collection", collection
      };
      args = prependInitialArgs(args);

      if (true) {
        tool = new MapReduceIndexerTool();
        res = ToolRunner.run(jobConf, tool, args);
        assertEquals(0, res);
        assertTrue(tool.job.isComplete());
        assertTrue(tool.job.isSuccessful());

        SolrDocumentList resultDocs = executeSolrQuery(cloudClient, "*:*");
        assertEquals(RECORD_COUNT, resultDocs.getNumFound());
        assertEquals(RECORD_COUNT, resultDocs.size());

        // perform updates
        for (int i = 0; i < RECORD_COUNT; i++) {
          SolrDocument doc = resultDocs.get(i);
          SolrInputDocument update = new SolrInputDocument();
          for (Map.Entry<String, Object> entry : doc.entrySet()) {
            update.setField(entry.getKey(), entry.getValue());
          }
          update.setField("user_screen_name", "Nadja" + i);
          update.removeField("_version_");
          cloudClient.add(update);
        }
        cloudClient.commit();

        // verify updates
        SolrDocumentList resultDocs2 = executeSolrQuery(cloudClient, "*:*");
        assertEquals(RECORD_COUNT, resultDocs2.getNumFound());
        assertEquals(RECORD_COUNT, resultDocs2.size());
        for (int i = 0; i < RECORD_COUNT; i++) {
          SolrDocument doc = resultDocs.get(i);
          SolrDocument doc2 = resultDocs2.get(i);
          assertEquals(doc.getFirstValue("id"), doc2.getFirstValue("id"));
          assertEquals("Nadja" + i, doc2.getFirstValue("user_screen_name"));
          assertEquals(doc.getFirstValue("text"), doc2.getFirstValue("text"));

          // perform delete
          cloudClient.deleteById((String) doc.getFirstValue("id"));
        }
        cloudClient.commit();

        // verify deletes
        assertEquals(0, executeSolrQuery(cloudClient, "*:*").size());
      }

      cloudClient.deleteByQuery("*:*");
      cloudClient.commit();
      assertEquals(0, cloudClient.query(new SolrQuery("*:*")).getResults().getNumFound());
    }
    
    // try using zookeeper with replication
    String replicatedCollection = "replicated_collection";
    if (TEST_NIGHTLY) {
      createCollection(replicatedCollection, 3, 3, 3);
    } else {
      createCollection(replicatedCollection, 2, 3, 2);
    }
    waitForRecoveriesToFinish(false);
    cloudClient.setDefaultCollection(replicatedCollection);
    fs.delete(inDir, true);   
    fs.delete(outDir, true);  
    fs.delete(dataDir, true);  
    assertTrue(fs.mkdirs(dataDir));
    INPATH = upAvroFile(fs, inDir, DATADIR, dataDir, inputAvroFile3);
    
    args = new String[] {
        "--solr-home-dir=" + MINIMR_CONF_DIR.getAbsolutePath(),
        "--output-dir=" + outDir.toString(),
        "--mappers=3",
        "--reducers=12",
        "--fanout=2",
        "--verbose",
        "--go-live",
        "--zk-host", zkServer.getZkAddress(), 
        "--collection", replicatedCollection, dataDir.toString()
    };
    args = prependInitialArgs(args);
    
    if (true) {
      tool = new MapReduceIndexerTool();
      res = ToolRunner.run(jobConf, tool, args);
      assertEquals(0, res);
      assertTrue(tool.job.isComplete());
      assertTrue(tool.job.isSuccessful());
      
      SolrDocumentList resultDocs = executeSolrQuery(cloudClient, "*:*");   
      assertEquals(RECORD_COUNT, resultDocs.getNumFound());
      assertEquals(RECORD_COUNT, resultDocs.size());
      
      checkConsistency(replicatedCollection);
      
      // perform updates
      for (int i = 0; i < RECORD_COUNT; i++) {
          SolrDocument doc = resultDocs.get(i);          
          SolrInputDocument update = new SolrInputDocument();
          for (Map.Entry<String, Object> entry : doc.entrySet()) {
              update.setField(entry.getKey(), entry.getValue());
          }
          update.setField("user_screen_name", "@Nadja" + i);
          update.removeField("_version_");
          cloudClient.add(update);
      }
      cloudClient.commit();
      
      // verify updates
      SolrDocumentList resultDocs2 = executeSolrQuery(cloudClient, "*:*");   
      assertEquals(RECORD_COUNT, resultDocs2.getNumFound());
      assertEquals(RECORD_COUNT, resultDocs2.size());
      for (int i = 0; i < RECORD_COUNT; i++) {
          SolrDocument doc = resultDocs.get(i);
          SolrDocument doc2 = resultDocs2.get(i);
          assertEquals(doc.getFieldValues("id"), doc2.getFieldValues("id"));
          assertEquals(1, doc.getFieldValues("id").size());
          assertEquals(Arrays.asList("@Nadja" + i), doc2.getFieldValues("user_screen_name"));
          assertEquals(doc.getFieldValues("text"), doc2.getFieldValues("text"));
          
          // perform delete
          cloudClient.deleteById((String)doc.getFirstValue("id"));
      }
      cloudClient.commit();
      
      // verify deletes
      assertEquals(0, executeSolrQuery(cloudClient, "*:*").size());
    }
    
    // try using solr_url with replication
    cloudClient.deleteByQuery("*:*");
    cloudClient.commit();
    assertEquals(0, executeSolrQuery(cloudClient, "*:*").getNumFound());
    assertEquals(0, executeSolrQuery(cloudClient, "*:*").size());
    fs.delete(inDir, true);    
    fs.delete(dataDir, true);
    assertTrue(fs.mkdirs(dataDir));
    INPATH = upAvroFile(fs, inDir, DATADIR, dataDir, inputAvroFile3);
    
    args = new String[] {
        "--solr-home-dir=" + MINIMR_CONF_DIR.getAbsolutePath(),
        "--output-dir=" + outDir.toString(),
        "--shards", "2",
        "--mappers=3",
        "--verbose",
        "--go-live", 
        "--go-live-threads", Integer.toString(random().nextInt(15) + 1),  dataDir.toString()
    };
    args = prependInitialArgs(args);

    argList = new ArrayList<>();
    getShardUrlArgs(argList, replicatedCollection);
    args = concat(args, argList.toArray(new String[0]));
    
    if (true) {
      tool = new MapReduceIndexerTool();
      res = ToolRunner.run(jobConf, tool, args);
      assertEquals(0, res);
      assertTrue(tool.job.isComplete());
      assertTrue(tool.job.isSuccessful());
      
      checkConsistency(replicatedCollection);
      
      assertEquals(RECORD_COUNT, executeSolrQuery(cloudClient, "*:*").size());
    }
    
    // delete collection
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionAction.DELETE.toString());
    params.set(CoreAdminParams.DELETE_INSTANCE_DIR, true);
    params.set(CoreAdminParams.DELETE_DATA_DIR, true);
    params.set(CoreAdminParams.DELETE_INDEX, true);
    params.set("name", replicatedCollection);
    QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    cloudClient.request(request);

    final TimeOut timeout = new TimeOut(10, TimeUnit.SECONDS);
    while (cloudClient.getZkStateReader().getClusterState().hasCollection(replicatedCollection)) {
      if (timeout.hasTimedOut()) {
         throw new AssertionError("Timeout waiting to see removed collection leave clusterstate");
      }
      
      Thread.sleep(200);
      cloudClient.getZkStateReader().updateClusterState();
    }
    
    if (TEST_NIGHTLY) {
      createCollection(replicatedCollection, 3, 3, 3);
    } else {
      createCollection(replicatedCollection, 2, 3, 2);
    }
    
    waitForRecoveriesToFinish(replicatedCollection, false);
    printLayout();
    assertEquals(0, executeSolrQuery(cloudClient, "*:*").getNumFound());
    
    
    args = new String[] {
        "--solr-home-dir=" + MINIMR_CONF_DIR.getAbsolutePath(),
        "--output-dir=" + outDir.toString(),
        "--shards", "2",
        "--mappers=3",
        "--verbose",
        "--go-live", 
        "--go-live-threads", Integer.toString(random().nextInt(15) + 1),  dataDir.toString()
    };
    args = prependInitialArgs(args);

    argList = new ArrayList<>();
    getShardUrlArgs(argList, replicatedCollection);
    args = concat(args, argList.toArray(new String[0]));
    
    tool = new MapReduceIndexerTool();
    res = ToolRunner.run(jobConf, tool, args);
    assertEquals(0, res);
    assertTrue(tool.job.isComplete());
    assertTrue(tool.job.isSuccessful());
    
    checkConsistency(replicatedCollection);
    
    assertEquals(RECORD_COUNT, executeSolrQuery(cloudClient, "*:*").size());
  }

  private void getShardUrlArgs(List<String> args) {
    for (int i = 0; i < getShardCount(); i++) {
      args.add("--shard-url");
      args.add(cloudJettys.get(i).url);
    }
  }
  
  private SolrDocumentList executeSolrQuery(SolrClient collection, String queryString) throws SolrServerException, IOException {
    SolrQuery query = new SolrQuery(queryString).setRows(2 * RECORD_COUNT).addSort("id", ORDER.asc);
    QueryResponse response = collection.query(query);
    return response.getResults();
  }

  private void checkConsistency(String replicatedCollection)
      throws Exception {
    Collection<Slice> slices = cloudClient.getZkStateReader().getClusterState()
        .getSlices(replicatedCollection);
    for (Slice slice : slices) {
      Collection<Replica> replicas = slice.getReplicas();
      long found = -1;
      for (Replica replica : replicas) {
        try (HttpSolrClient client = new HttpSolrClient(new ZkCoreNodeProps(replica).getCoreUrl())) {
          SolrQuery query = new SolrQuery("*:*");
          query.set("distrib", false);
          QueryResponse replicaResults = client.query(query);
          long count = replicaResults.getResults().getNumFound();
          if (found != -1) {
            assertEquals(slice.getName() + " is inconsistent "
                + new ZkCoreNodeProps(replica).getCoreUrl(), found, count);
          }
          found = count;
        }
      }
    }
  }
  
  private void getShardUrlArgs(List<String> args, String replicatedCollection) {
    Collection<Slice> slices = cloudClient.getZkStateReader().getClusterState().getSlices(replicatedCollection);
    for (Slice slice : slices) {
      Collection<Replica> replicas = slice.getReplicas();
      for (Replica replica : replicas) {
        args.add("--shard-url");
        args.add(new ZkCoreNodeProps(replica).getCoreUrl());
      }
    }
  }
  
  private Path upAvroFile(FileSystem fs, Path inDir, String DATADIR,
      Path dataDir, String localFile) throws IOException, UnsupportedEncodingException {
    Path INPATH = new Path(inDir, "input.txt");
    OutputStream os = fs.create(INPATH);
    Writer wr = new OutputStreamWriter(os, StandardCharsets.UTF_8);
    wr.write(DATADIR + File.separator + localFile);
    wr.close();
    
    assertTrue(fs.mkdirs(dataDir));
    fs.copyFromLocalFile(new Path(DOCUMENTS_DIR, localFile), dataDir);
    return INPATH;
  }
  
  @Override
  public JettySolrRunner createJetty(File solrHome, String dataDir,
      String shardList, String solrConfigOverride, String schemaOverride)
      throws Exception {

    Properties props = new Properties();
    if (solrConfigOverride != null)
      props.setProperty("solrconfig", solrConfigOverride);
    if (schemaOverride != null)
      props.setProperty("schema", schemaOverride);
    if (shardList != null) 
      props.setProperty("shards", shardList);

    String collection = System.getProperty("collection");
    if (collection == null)
      collection = "collection1";
    props.setProperty("collection", collection);

    JettySolrRunner jetty = new JettySolrRunner(solrHome.getAbsolutePath(), props, buildJettyConfig(context));
    jetty.start();
    
    return jetty;
  }
  
  private static void putConfig(SolrZkClient zkClient, File solrhome, String name) throws Exception {
    putConfig(zkClient, solrhome, name, name);
  }
  
  private static void putConfig(SolrZkClient zkClient, File solrhome, String srcName, String destName)
      throws Exception {
    
    File file = new File(solrhome, "conf" + File.separator + srcName);
    if (!file.exists()) {
      // LOG.info("skipping " + file.getAbsolutePath() +
      // " because it doesn't exist");
      return;
    }
    
    String destPath = "/configs/conf1/" + destName;
    // LOG.info("put " + file.getAbsolutePath() + " to " + destPath);
    zkClient.makePath(destPath, file, false, true);
  }
  
  private void uploadConfFiles() throws Exception {
    // upload our own config files
    SolrZkClient zkClient = new SolrZkClient(zkServer.getZkAddress(), 10000);
    putConfig(zkClient, new File(RESOURCES_DIR + "/solr/solrcloud"),
        "solrconfig.xml");
    putConfig(zkClient, MINIMR_CONF_DIR, "schema.xml");
    putConfig(zkClient, MINIMR_CONF_DIR, "elevate.xml");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_en.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_ar.txt");
    
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_bg.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_ca.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_cz.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_da.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_el.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_es.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_eu.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_de.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_fa.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_fi.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_fr.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_ga.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_gl.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_hi.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_hu.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_hy.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_id.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_it.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_ja.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_lv.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_nl.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_no.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_pt.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_ro.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_ru.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_sv.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_th.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_tr.txt");
    
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/contractions_ca.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/contractions_fr.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/contractions_ga.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/contractions_it.txt");
    
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stemdict_nl.txt");
    
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/hyphenations_ga.txt");
    
    putConfig(zkClient, MINIMR_CONF_DIR, "stopwords.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "protwords.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "currency.xml");
    putConfig(zkClient, MINIMR_CONF_DIR, "open-exchange-rates.json");
    putConfig(zkClient, MINIMR_CONF_DIR, "mapping-ISOLatin1Accent.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "old_synonyms.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "synonyms.txt");
    zkClient.close();
  }
  
  protected static <T> T[] concat(T[]... arrays) {
    if (arrays.length <= 0) {
      throw new IllegalArgumentException();
    }
    Class clazz = null;
    int length = 0;
    for (T[] array : arrays) {
      clazz = array.getClass();
      length += array.length;
    }
    T[] result = (T[]) Array.newInstance(clazz.getComponentType(), length);
    int pos = 0;
    for (T[] array : arrays) {
      System.arraycopy(array, 0, result, pos, array.length);
      pos += array.length;
    }
    return result;
  }
  
  private NamedList<Object> createAlias(String alias, String collections) throws SolrServerException, IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("collections", collections);
    params.set("name", alias);
    params.set("action", CollectionAction.CREATEALIAS.toString());
    QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    return cloudClient.request(request);
  }

  
}
