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
package org.apache.solr.cloud;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.CoreStatus;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.V2Response;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.RetryUtil;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Arrays.asList;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_DEF;
import static org.apache.solr.common.cloud.ZkStateReader.NRT_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.NUM_SHARDS_PROP;
import static org.apache.solr.common.params.CollectionAdminParams.COLLECTION;
import static org.apache.solr.common.params.CollectionAdminParams.DEFAULTS;

public class CollectionsAPISolrJTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static SolrTestUtil.HorridGC horridGc;

  @BeforeClass
  public static void beforeCollectionsAPISolrJTest() throws Exception {
    System.setProperty("solr.zkclienttimeout", "15000");
    System.setProperty("zkClientTimeout", "15000");


    String timeout = "640000";
    System.setProperty("solr.http2solrclient.default.idletimeout", timeout);
    System.setProperty("distribUpdateSoTimeout", timeout);
    System.setProperty("socketTimeout", timeout);
    System.setProperty("connTimeout", timeout);
    System.setProperty("solr.test.socketTimeout.default", timeout);
    System.setProperty("solr.connect_timeout.default", timeout);
    System.setProperty("solr.so_commit_timeout.default", timeout);
    System.setProperty("solr.httpclient.defaultConnectTimeout", timeout);
    System.setProperty("solr.httpclient.defaultSoTimeout", timeout);
    System.setProperty("solr.default.collection_op_timeout", timeout);
    System.setProperty("solr.enableMetrics", "false");
    System.setProperty("solr.createCollectionTimeout", timeout);

    System.setProperty("solr.suppressDefaultConfigBootstrap", "false");
    configureCluster( TEST_NIGHTLY ? 4 : 2).formatZk(true)
            .addConfig("conf", SolrTestUtil.configset("cloud-minimal"))
            .addConfig("conf2", SolrTestUtil.configset("cloud-dynamic"))
            .configure();

//    horridGc = SolrTestUtil.horridGC();
//
//    getTestExecutor().submit(() -> {
//      try {
//        Thread.sleep(SolrTestCase.random().nextInt(10 + 10000));
//      } catch (InterruptedException e) {
//        e.printStackTrace();
//      }
//      horridGc.stopHorridGC();
//    });
  }

  @AfterClass
  public static void afterCollectionsAPISolrJTest() throws Exception {
    if (horridGc != null) {
      horridGc.stopHorridGC();
      horridGc.waitForThreads(15000);
    }
  }


  @After
  public void afterTest() throws Exception {

  }

  /**
   * When a config name is not specified during collection creation, the _default should
   * be used.
   */
  @Test
  public void testCreateWithDefaultConfigSet() throws Exception {
    String collectionName = "solrj_default_configset";
    CollectionAdminResponse response = CollectionAdminRequest.createCollection(collectionName, 2, 2)
            .setMaxShardsPerNode(4).process(cluster.getSolrClient());

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    Map<String, NamedList<Integer>> coresStatus = response.getCollectionCoresStatus();

    assertEquals(4, coresStatus.size());
    for (String coreName : coresStatus.keySet()) {
      NamedList<Integer> status = coresStatus.get(coreName);
      assertEquals(0, (int)status.get("status"));
      assertTrue(status.get("QTime") > 0);
    }
    // Use of _default configset should generate a warning for data-driven functionality in production use
    assertTrue(response.getWarning() != null && response.getWarning().contains("NOT RECOMMENDED for production use"));

    response = CollectionAdminRequest.deleteCollection(collectionName).process(cluster.getSolrClient());
    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    Map<String,NamedList<Integer>> nodesStatus = response.getCollectionNodesStatus();
//    assertEquals(TEST_NIGHTLY ? 4 : 2, nodesStatus.size());
  }

  @Test
  @LuceneTestCase.Nightly
  public void testCreateCollWithDefaultClusterPropertiesNewFormat() throws Exception {
    String COLL_NAME = "CollWithDefaultClusterProperties";
      V2Response rsp = new V2Request.Builder("/cluster")
          .withMethod(SolrRequest.METHOD.POST)
          .withPayload("{set-obj-property:{defaults : {collection:{numShards : 2 , nrtReplicas : 2}}}}")
          .build()
          .process(cluster.getSolrClient());

      // MRM TODO: cluster property watcher?
      for (int i = 0; i < 15; i++) {
        Map m = cluster.getSolrClient().getZkStateReader().getClusterProperty(COLLECTION_DEF, null);
        if (m != null) break;
        Thread.sleep(50);
      }
      Object clusterProperty = cluster.getSolrClient().getZkStateReader().getClusterProperty(ImmutableList.of(DEFAULTS, COLLECTION, NUM_SHARDS_PROP), null);
      assertEquals("2", String.valueOf(clusterProperty));
      clusterProperty = cluster.getSolrClient().getZkStateReader().getClusterProperty(ImmutableList.of(DEFAULTS, COLLECTION, NRT_REPLICAS), null);
      assertEquals("2", String.valueOf(clusterProperty));
      CollectionAdminResponse response = CollectionAdminRequest
          .createCollection(COLL_NAME, "conf", 2, 2, null, null)
          .process(cluster.getSolrClient());
      assertEquals(0, response.getStatus());
      assertTrue(response.isSuccess());

      DocCollection coll = cluster.getSolrClient().getClusterStateProvider().getClusterState().getCollection(COLL_NAME);
      Map<String, Slice> slices = coll.getSlicesMap();
      assertEquals(2, slices.size());
      for (Slice slice : slices.values()) {
        assertEquals(2, slice.getReplicas().size());
      }
      CollectionAdminRequest.deleteCollection(COLL_NAME).process(cluster.getSolrClient());

      // unset only a single value
      rsp = new V2Request.Builder("/cluster")
          .withMethod(SolrRequest.METHOD.POST)
          .withPayload("{\n" +
              "  \"set-obj-property\": {\n" +
              "    \"defaults\" : {\n" +
              "      \"collection\": {\n" +
              "        \"nrtReplicas\": null\n" +
              "      }\n" +
              "    }\n" +
              "  }\n" +
              "}")
          .build()
          .process(cluster.getSolrClient());
      // we use a timeout so that the change made in ZK is reflected in the watched copy inside ZkStateReader
      TimeOut timeOut = new TimeOut(5, TimeUnit.SECONDS, 100, new TimeSource.NanoTimeSource());
      while (!timeOut.hasTimedOut())  {
        clusterProperty = cluster.getSolrClient().getZkStateReader().getClusterProperty(ImmutableList.of(DEFAULTS, COLLECTION, NRT_REPLICAS), null);
        if (clusterProperty == null)  break;
      }
      assertNull(clusterProperty);

      rsp = new V2Request.Builder("/cluster")
          .withMethod(SolrRequest.METHOD.POST)
          .withPayload("{set-obj-property:{defaults: {collection:null}}}")
          .build()
          .process(cluster.getSolrClient());
      // assert that it is really gone in both old and new paths
      timeOut = new TimeOut(5, TimeUnit.SECONDS, 100, new TimeSource.NanoTimeSource());
      while (!timeOut.hasTimedOut()) {
        clusterProperty = cluster.getSolrClient().getZkStateReader().getClusterProperty(ImmutableList.of(DEFAULTS, COLLECTION, NUM_SHARDS_PROP), null);
        if (clusterProperty == null)  break;
      }
      assertNull(clusterProperty);
      clusterProperty = cluster.getSolrClient().getZkStateReader().getClusterProperty(ImmutableList.of(COLLECTION_DEF, NUM_SHARDS_PROP), null);
      assertNull(clusterProperty);


  }

  @Test
  @Ignore // MRM TODO: - testing large numbers
  public void testCreateAndDeleteCollection() throws Exception {
    String collectionName = "solrj_test";
    CollectionAdminResponse response = CollectionAdminRequest.createCollection(collectionName, "conf", 36, 36) // 24 * 24 = 576
            .process(cluster.getSolrClient());


    assertEquals(response.toString(), 0, response.getStatus());
    assertTrue(response.toString(), response.isSuccess());

//    Map<String, NamedList<Integer>> coresStatus = response.getCollectionCoresStatus();
//    assertEquals(4, coresStatus.size());
//    for (String coreName : coresStatus.keySet()) {
//      NamedList<Integer> status = coresStatus.get(coreName);
//      assertEquals(0, (int)status.get("status"));
//      assertTrue(status.get("QTime") > 0);
//    }

//    response = CollectionAdminRequest.deleteCollection(collectionName).process(cluster.getSolrClient());
//
//    assertEquals(0, response.getStatus());
//
//    assertFalse(zkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName));
    // MRM TODO: what happened to success?
//    assertTrue(response.toString(), response.isSuccess());
//    Map<String,NamedList<Integer>> nodesStatus = response.getCollectionNodesStatus();
//    assertEquals(TEST_NIGHTLY ? 4 : 2, nodesStatus.size());

    // Test Creating a new collection.
    collectionName = "solrj_test2";

//    response = CollectionAdminRequest.createCollection(collectionName, "conf", 2, 2)
//        .setMaxShardsPerNode(4).process(cluster.getSolrClient());
//    assertEquals(0, response.getStatus());
//    assertTrue(response.isSuccess());
//
//    cluster.waitForActiveCollection(collectionName, 2, 4);    response = CollectionAdminRequest.createCollection(collectionName, "conf", 2, 2)
//        .setMaxShardsPerNode(4).process(cluster.getSolrClient());
//    assertEquals(0, response.getStatus());
//    assertTrue(response.isSuccess());
//
//    cluster.waitForActiveCollection(collectionName, 2, 4);
  }

  @Test
  public void testCloudInfoInCoreStatus() throws IOException, SolrServerException {
    String collectionName = "corestatus_test";
    CollectionAdminResponse response = CollectionAdminRequest.createCollection(collectionName, "conf", 2, 2)
        .setMaxShardsPerNode(3)
        .process(cluster.getSolrClient());

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    
    String nodeName = (String) response._get("success[0]/key", null);
    String corename = (String) response._get(asList("success", nodeName, "core"), null);

    try (Http2SolrClient coreclient = SolrTestCaseJ4
        .getHttpSolrClient(cluster.getSolrClient().getZkStateReader().getBaseUrlForNodeName(nodeName))) {
      CoreAdminResponse status = CoreAdminRequest.getStatus(corename, coreclient);
      assertEquals(collectionName, status._get(asList("status", corename, "cloud", "collection"), null));
      assertNotNull(status._get(asList("status", corename, "cloud", "shard"), null));
      assertNotNull(status._get(asList("status", corename, "cloud", "replica"), null));
    }
  }

  @Test
  public void testCreateAndDeleteShard() throws Exception {
    // Create an implicit collection
    String collectionName = "solrj_implicit";
    // MRM TODO: 1, 1
    CollectionAdminResponse response
        = CollectionAdminRequest.createCollectionWithImplicitRouter(collectionName, "conf", "shardA,shardB", 3, 0, 0)
        .setMaxShardsPerNode(3)
        .process(cluster.getSolrClient());

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());

    Map<String, NamedList<Integer>> coresStatus = response.getCollectionCoresStatus();
   // MRM TODO:
    // assertEquals(6, coresStatus.size());

    // Add a shard to the implicit collection
    response = CollectionAdminRequest.createShard(collectionName, "shardC").process(cluster.getSolrClient());

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    
    coresStatus = response.getCollectionCoresStatus();
    // MRM TODO: TODO
    //assertEquals(3, coresStatus.size());
    int replicaTlog = 0;
    int replicaNrt = 0;
    int replicaPull = 0;
    for (String coreName : coresStatus.keySet()) {
      assertEquals(0, (int) coresStatus.get(coreName).get("status"));
      if (coreName.contains("shardC_replica_t")) replicaTlog++;
      else if (coreName.contains("shardC_replica_n")) replicaNrt++;
      else replicaPull++;
    }
//    assertEquals(1, replicaNrt);
//    assertEquals(1, replicaTlog);
//    assertEquals(1, replicaPull);

    response = CollectionAdminRequest.deleteShard(collectionName, "shardC").process(cluster.getSolrClient());

//    assertEquals(0, response.getStatus());
//    assertTrue(response.isSuccess());
//    Map<String, NamedList<Integer>> nodesStatus = response.getCollectionNodesStatus();
//    assertEquals(1, nodesStatus.size());
  }

  @Test
  @Ignore // MRM TODO:
  public void testCreateAndDeleteAlias() throws IOException, SolrServerException {

    final String collection = "aliasedCollection";
    CollectionAdminRequest.createCollection(collection, "conf", 1, 1).process(cluster.getSolrClient());

    CollectionAdminResponse response
        = CollectionAdminRequest.createAlias("solrj_alias", collection).process(cluster.getSolrClient());
    assertEquals(0, response.getStatus());

    response = CollectionAdminRequest.deleteAlias("solrj_alias").process(cluster.getSolrClient());
    assertEquals(0, response.getStatus());
  }

  @Test
  public void testSplitShard() throws Exception {

    final String collectionName = "solrj_test_splitshard";
    CollectionAdminRequest.createCollection(collectionName, "conf", 2, 1)
        .process(cluster.getSolrClient());

    CollectionAdminResponse response = CollectionAdminRequest.splitShard(collectionName)
        .setShardName("s1")
        .process(cluster.getSolrClient());

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    Map<String, NamedList<Integer>> coresStatus = response.getCollectionCoresStatus();
    int shard10 = 0;
    int shard11 = 0;
    for (String coreName : coresStatus.keySet()) {
      assertEquals(0, (int) coresStatus.get(coreName).get("status"));
      if (coreName.contains("_s1_0")) shard10++;
      else shard11++;
    }
    assertEquals(1, shard10);
    assertEquals(1, shard11);

//    waitForState("Expected all shards to be active and parent shard to be removed", collectionName, (n, c) -> {
//      if (c.getSlice("s1").getState() == Slice.State.ACTIVE)
//        return false;
//      for (Replica r : c.getReplicas()) {
//        if (r.isActive(n) == false)
//          return false;
//      }
//      return true;
//    });

    // MRM TODO: - just to remove from equation
    // Test splitting using split.key
//    response = CollectionAdminRequest.splitShard(collectionName)
//        .setSplitKey("b!")
//        .process(cluster.getSolrClient());
//
//    assertEquals(0, response.getStatus());
//    assertTrue(response.isSuccess());
  }

  @Test
  public void testCreateCollectionWithPropertyParam() throws Exception {

    String collectionName = "solrj_test_core_props";

    Path tmpDir = SolrTestUtil.createTempDir("testPropertyParamsForCreate");
    Path dataDir = tmpDir.resolve("dataDir-" + TestUtil.randomSimpleString(random(), 1, 5));
    Path ulogDir = tmpDir.resolve("ulogDir-" + TestUtil.randomSimpleString(random(), 1, 5));

    CollectionAdminResponse response = CollectionAdminRequest.createCollection(collectionName, "conf", 1, 1)
        .withProperty(CoreAdminParams.DATA_DIR, dataDir.toString())
        .withProperty(CoreAdminParams.ULOG_DIR, ulogDir.toString())
        .process(cluster.getSolrClient());

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());

    // MRM TODO: - there has always been a race where this can be missed if its handled too fast
//    Map<String, NamedList<Integer>> coresStatus = response.getCollectionCoresStatus();
//    assertEquals(1, coresStatus.size());

    DocCollection testCollection = getCollectionState(collectionName);

    Replica replica1 = testCollection.getReplicas().iterator().next();
    CoreStatus coreStatus = getCoreStatus(replica1);

    assertEquals(Paths.get(coreStatus.getDataDirectory()).toString(), dataDir.toString());

  }

  @Test
  public void testAddAndDeleteReplica() throws Exception {

    final String collectionName = "solrj_replicatests";
    CollectionAdminRequest.createCollection(collectionName, "conf", 1, 2)
        .process(cluster.getSolrClient());

    ArrayList<String> nodeList
        = new ArrayList<>(cluster.getSolrClient().getZkStateReader().getLiveNodes());
    Collections.shuffle(nodeList, random());
    final String node = nodeList.get(0);

    List<Replica> originalReplicas = cluster.getSolrClient().getZkStateReader().getClusterState().getCollection(collectionName).getReplicas();

    CollectionAdminResponse response = CollectionAdminRequest.addReplicaToShard(collectionName, "s1")
        .setNode(node)
        .process(cluster.getSolrClient());

    cluster.waitForActiveCollection(collectionName, 1, 3);
    // MRM TODO: - look at returned status not coming back
//    Replica newReplica = grabNewReplica(response, getCollectionState(collectionName));
//    assertEquals(0, response.getStatus());
//    assertTrue(response.isSuccess());
//    assertTrue(newReplica.getNodeName().equals(node));

    ArrayList rlist = new ArrayList(cluster.getSolrClient().getZkStateReader().getClusterState().getCollection(collectionName).getReplicas());
    rlist.removeAll(originalReplicas);
    Replica newReplica = (Replica) rlist.get(0);

    // Test DELETEREPLICA
    response = CollectionAdminRequest.deleteReplica(collectionName, "s1", newReplica.getName())
        .process(cluster.getSolrClient());
    assertEquals(0, response.getStatus());

    waitForState("Expected replica " + newReplica.getName() + " to vanish from cluster state", collectionName,
        (n, c) -> c.getSlice("s1").getReplica(newReplica.getName()) == null);

  }

  private Replica grabNewReplica(CollectionAdminResponse response, DocCollection docCollection) {
    String replicaName = response.getCollectionCoresStatus().keySet().iterator().next();
    Optional<Replica> optional = docCollection.getReplicas().stream()
        .filter(replica -> replicaName.equals(replica.getName()))
        .findAny();
    if (optional.isPresent()) {
      return optional.get();
    }
    throw new AssertionError("Can not find " + replicaName + " from " + docCollection);
  }

  @Test
  public void testClusterProp() throws InterruptedException, IOException, SolrServerException {

    // sanity check our expected default
    final ClusterProperties props = new ClusterProperties(zkClient());
    
    CollectionAdminResponse response = CollectionAdminRequest.setClusterProperty(ZkStateReader.MAX_CORES_PER_NODE, "42")
      .process(cluster.getSolrClient());
    assertEquals(0, response.getStatus());
    assertEquals("Cluster property was not set", props.getClusterProperty(ZkStateReader.MAX_CORES_PER_NODE, null), "42");

    // Unset ClusterProp that we set.
    CollectionAdminRequest.setClusterProperty(ZkStateReader.MAX_CORES_PER_NODE, null).process(cluster.getSolrClient());
    assertEquals("Cluster property was not unset", props.getClusterProperty(ZkStateReader.MAX_CORES_PER_NODE, null), null);

    response = CollectionAdminRequest.setClusterProperty(ZkStateReader.MAX_CORES_PER_NODE, "1")
        .process(cluster.getSolrClient());
    assertEquals(0, response.getStatus());
    assertEquals("Cluster property was not set", props.getClusterProperty(ZkStateReader.MAX_CORES_PER_NODE, null), "1");
  }

  @Test
  public void testCollectionProp() throws InterruptedException, IOException, SolrServerException {
    final String collectionName = "collectionPropTest";
    final String propName = "testProperty";

    CollectionAdminRequest.createCollection(collectionName, "conf", 2, 2)
            .setMaxShardsPerNode(4).process(cluster.getSolrClient());

    // Check for value change
    CollectionAdminRequest.setCollectionProperty(collectionName, propName, "false")
        .process(cluster.getSolrClient());
    checkCollectionProperty(collectionName, propName, "false", 3000);

    // Check for removing value
    // MRM TODO: our kind of ugly handling I think, flakey
//    CollectionAdminRequest.setCollectionProperty(collectionName, propName, null)
//        .process(cluster.getSolrClient());
//    checkCollectionProperty(collectionName, propName, null, 3000);
  }

  private void checkCollectionProperty(String collection, String propertyName, String propertyValue, long timeoutMs) throws InterruptedException {
    TimeOut timeout = new TimeOut(timeoutMs, TimeUnit.MILLISECONDS, TimeSource.NANO_TIME);
    while (!timeout.hasTimedOut()){
      Thread.sleep(10);
      if (Objects.equals(cluster.getSolrClient().getZkStateReader().getCollectionProperties(collection).get(propertyName), propertyValue)) {
        return;
      }
    }

    fail("Timed out waiting for cluster property value");
  }

  @Test
  @LuceneTestCase.Nightly
  public void testColStatus() throws Exception {
    final String collectionName = "collectionStatusTest";
    CollectionAdminRequest.createCollection(collectionName, "conf2", 2, 2)
        .setMaxShardsPerNode(100)
        .process(cluster.getSolrClient());

    SolrClient client = cluster.getSolrClient();
    byte[] binData = collectionName.getBytes("UTF-8");
    // index some docs
    for (int i = 0; i < 10; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", String.valueOf(i));
      doc.addField("number_i", i);
      doc.addField("number_l", i);
      doc.addField("number_f", i);
      doc.addField("number_d", i);
      doc.addField("number_ti", i);
      doc.addField("number_tl", i);
      doc.addField("number_tf", i);
      doc.addField("number_td", i);
      doc.addField("point", i + "," + i);
      doc.addField("pointD", i + "," + i);
      doc.addField("store", (i * 5) + "," + (i * 5));
      doc.addField("boolean_b", true);
      doc.addField("multi_int_with_docvals", i);
      doc.addField("string_s", String.valueOf(i));
      doc.addField("tv_mv_string", "this is a test " + i);
      doc.addField("timestamp_dt", new Date());
      doc.addField("timestamp_tdt", new Date());
      doc.addField("payload", binData);
      client.add(collectionName, doc);
    }
    client.commit(collectionName);

    CollectionAdminRequest.ColStatus req = CollectionAdminRequest.collectionStatus(collectionName);
    req.setWithFieldInfo(true);
    req.setWithCoreInfo(true);
    req.setWithSegments(true);
    req.setWithSizeInfo(true);
    CollectionAdminResponse rsp = req.process(cluster.getSolrClient());
    assertEquals(0, rsp.getStatus());
    List<Object> nonCompliant = (List<Object>)rsp.getResponse().findRecursive(collectionName, "schemaNonCompliant");
    assertEquals(nonCompliant.toString(), 1, nonCompliant.size());
    assertTrue(nonCompliant.toString(), nonCompliant.contains("(NONE)"));
    NamedList<Object> segInfos = (NamedList<Object>) rsp.getResponse().findRecursive(collectionName, "shards", "s1", "leader", "segInfos");
    assertNotNull(Utils.toJSONString(rsp), segInfos.findRecursive("info", "core", "startTime"));
    assertNotNull(Utils.toJSONString(rsp), segInfos.get("fieldInfoLegend"));
    assertNotNull(Utils.toJSONString(rsp), segInfos.findRecursive("segments", "_0", "fields", "id", "flags"));
    assertNotNull(Utils.toJSONString(rsp), segInfos.findRecursive("segments", "_0", "ramBytesUsed"));
    // test for replicas not active - SOLR-13882
    DocCollection coll = cluster.getSolrClient().getClusterStateProvider().getClusterState().getCollection(collectionName);
    Replica firstReplica = coll.getSlice("s1").getReplicas().iterator().next();
    String firstNode = firstReplica.getNodeName();

    JettySolrRunner jetty = cluster.getJettyForShard(collectionName, "s1");
    jetty.stop();
    rsp = req.process(cluster.getSolrClient());
    assertEquals(0, rsp.getStatus());
    Number down = (Number) rsp.getResponse().findRecursive(collectionName, "shards", "s1", "replicas", "down");
    assertTrue("should be some down replicas, but there were none in shard1:" + rsp, down.intValue() > 0);
    jetty.start();
  }

  @Test
  @LuceneTestCase.AwaitsFix(bugUrl = "Alias issue")
  public void testRenameCollection() throws Exception {
    doTestRenameCollection(true);
    CollectionAdminRequest.deleteAlias("col1").process(cluster.getSolrClient());
    CollectionAdminRequest.deleteAlias("col2").process(cluster.getSolrClient());
    CollectionAdminRequest.deleteAlias("foo").process(cluster.getSolrClient());
    CollectionAdminRequest.deleteAlias("simpleAlias").process(cluster.getSolrClient());
    CollectionAdminRequest.deleteAlias("catAlias").process(cluster.getSolrClient());
    CollectionAdminRequest.deleteAlias("compoundAlias").process(cluster.getSolrClient());
    cluster.getSolrClient().getZkStateReader().aliasesManager.update();
    doTestRenameCollection(false);
  }

  private void doTestRenameCollection(boolean followAliases) throws Exception {
    String collectionName1 = "testRename1_" + followAliases;
    String collectionName2 = "testRename2_" + followAliases;
    CollectionAdminRequest.createCollection(collectionName1, "conf", 1, 1).setAlias("col1").process(cluster.getSolrClient());
    CollectionAdminRequest.createCollection(collectionName2, "conf", 1, 1).setAlias("col2").process(cluster.getSolrClient());

    CollectionAdminRequest.createAlias("compoundAlias", "col1,col2").process(cluster.getSolrClient());
    CollectionAdminRequest.createAlias("simpleAlias", "col1").process(cluster.getSolrClient());
    CollectionAdminRequest.createCategoryRoutedAlias("catAlias", "field1", 100,
        CollectionAdminRequest.createCollection("_unused_", "conf", 1, 1)).process(cluster.getSolrClient());

    CollectionAdminRequest.Rename rename = CollectionAdminRequest.renameCollection("col1", "foo");
    rename.setFollowAliases(followAliases);
    ZkStateReader zkStateReader = cluster.getSolrClient().getZkStateReader();
    Aliases aliases;
    if (!followAliases) {
      try {
        rename.process(cluster.getSolrClient());
      } catch (Exception e) {
        assertTrue(e.toString(), e.toString().contains("source collection 'col1' not found"));
      }
    } else {
      rename.process(cluster.getSolrClient());
      zkStateReader.aliasesManager.update();

      aliases = zkStateReader.getAliases();
      assertEquals(aliases.getCollectionAliasListMap().toString(), collectionName1, aliases.resolveSimpleAlias("foo"));
      assertEquals(aliases.getCollectionAliasListMap().toString(), collectionName1, aliases.resolveSimpleAlias("simpleAlias"));
      List<String> compoundAliases = aliases.resolveAliases("compoundAlias");
      assertEquals(compoundAliases.toString(), 2, compoundAliases.size());
      assertTrue(compoundAliases.toString(), compoundAliases.contains(collectionName1));
      assertTrue(compoundAliases.toString(), compoundAliases.contains(collectionName2));
    }

    CollectionAdminRequest.renameCollection(collectionName1, collectionName2).process(cluster.getSolrClient());
    zkStateReader.aliasesManager.update();

    aliases = zkStateReader.getAliases();
    if (followAliases) {
      assertEquals(aliases.getCollectionAliasListMap().toString(), collectionName2, aliases.resolveSimpleAlias("foo"));
    }
    assertEquals(aliases.getCollectionAliasListMap().toString(), collectionName2, aliases.resolveSimpleAlias("simpleAlias"));
    assertEquals(aliases.getCollectionAliasListMap().toString(), collectionName2, aliases.resolveSimpleAlias(collectionName1));
    // we renamed col1 -> col2 so the compound alias contains only "col2,col2" which is reduced to col2
    List<String> compoundAliases = aliases.resolveAliases("compoundAlias");
    assertEquals(compoundAliases.toString(), 1, compoundAliases.size());
    assertTrue(compoundAliases.toString(), compoundAliases.contains(collectionName2));

    try {
      rename = CollectionAdminRequest.renameCollection("catAlias", "bar");
      rename.setFollowAliases(followAliases);
      rename.process(cluster.getSolrClient());
      fail("category-based alias renaming should fail");
    } catch (Exception e) {
      if (followAliases) {
        assertTrue(e.toString(), e.toString().contains("is a routed alias"));
      } else {
        assertTrue(e.toString(), e.toString().contains("source collection 'catAlias' not found"));
      }
    }

    try {
      rename = CollectionAdminRequest.renameCollection("col2", "foo");
      rename.setFollowAliases(followAliases);
      rename.process(cluster.getSolrClient());
      fail("should fail because 'foo' already exists");
    } catch (Exception e) {
      if (followAliases) {
        assertTrue(e.toString(), e.toString().contains("exists"));
      } else {
        assertTrue(e.toString(), e.toString().contains("source collection 'col2' not found"));
      }
    }
  }

  @Test
  @Ignore // MRM TODO: debug
  public void testDeleteAliasedCollection() throws Exception {
    CloudHttp2SolrClient solrClient = cluster.getSolrClient();
    String collectionName1 = "aliasedCollection1";
    String collectionName2 = "aliasedCollection2";
    CollectionAdminRequest.createCollection(collectionName1, "conf", 1, 1).process(solrClient);
    CollectionAdminRequest.createCollection(collectionName2, "conf", 1, 1).process(solrClient);

    SolrInputDocument doc = new SolrInputDocument("id", "1");
    solrClient.add(collectionName1, doc);
    doc = new SolrInputDocument("id", "2");
    solrClient.add(collectionName2, doc);
    solrClient.commit(collectionName1);
    solrClient.commit(collectionName2);

    assertDoc(solrClient, collectionName1, "1");
    assertDoc(solrClient, collectionName2, "2");

    CollectionAdminRequest.createAlias(collectionName1, collectionName2).process(solrClient);

    RetryUtil.retryUntil("didn't get the new aliases", 10, 1000, TimeUnit.MILLISECONDS, () -> {
      try {
        solrClient.getZkStateReader().aliasesManager.update();
        return solrClient.getZkStateReader().getAliases()
            .resolveSimpleAlias(collectionName1).equals(collectionName2);
      } catch (Exception e) {
        fail("exception caught refreshing aliases: " + e);
        return false;
      }
    });

    // both results should come from collection 2
    assertDoc(solrClient, collectionName1, "2"); // aliased
    assertDoc(solrClient, collectionName2, "2"); // direct

    // should be able to remove collection 1 when followAliases = false
    CollectionAdminRequest.Delete delete = CollectionAdminRequest.deleteCollection(collectionName1);
    delete.setFollowAliases(false);
    delete.process(solrClient);
    ClusterState state = solrClient.getClusterStateProvider().getClusterState();
    assertFalse(state.getCollectionsMap().toString(), state.hasCollection(collectionName1));
    // search should still work, returning results from collection 2
    assertDoc(solrClient, collectionName1, "2"); // aliased
    assertDoc(solrClient, collectionName2, "2"); // direct

    // without aliases this collection doesn't exist anymore
    delete = CollectionAdminRequest.deleteCollection(collectionName1);
    delete.setFollowAliases(false);
    try {
      delete.process(solrClient);
      fail("delete of nonexistent collection 1 should have failed when followAliases=false");
    } catch (Exception e) {
      assertTrue(e.toString(), e.toString().contains(collectionName1));
    }

    // with followAliases=true collection 2 (and the alias) should both be removed
    delete.setFollowAliases(true);
    delete.process(solrClient);

    state = solrClient.getClusterStateProvider().getClusterState();
    // the collection is gone
    assertFalse(state.getCollectionsMap().toString(), state.hasCollection(collectionName2));

    // and the alias is gone
    RetryUtil.retryUntil("didn't get the new aliases", 10, 1000, TimeUnit.MILLISECONDS, () -> {
      try {
        solrClient.getZkStateReader().aliasesManager.update();
        return !solrClient.getZkStateReader().getAliases().hasAlias(collectionName1);
      } catch (Exception e) {
        fail("exception caught refreshing aliases: " + e);
        return false;
      }
    });
  }

  private void assertDoc(CloudHttp2SolrClient solrClient, String collection, String id) throws Exception {
    QueryResponse rsp = solrClient.query(collection, params(CommonParams.Q, "*:*"));
    assertEquals(rsp.toString(), 1, rsp.getResults().getNumFound());
    SolrDocument sdoc = rsp.getResults().get(0);
    assertEquals(sdoc.toString(), id, sdoc.getFieldValue("id"));

  }

  @Test
  @Ignore // MRM TODO: - have to fix that race
  public void testOverseerStatus() throws IOException, SolrServerException {
    CollectionAdminResponse response = new CollectionAdminRequest.OverseerStatus().process(cluster.getSolrClient());
    assertEquals(0, response.getStatus());
    assertNotNull("overseer_operations shouldn't be null", response.getResponse().get("overseer_operations"));
  }

  @Test
  public void testList() throws IOException, SolrServerException {
    CollectionAdminResponse response = new CollectionAdminRequest.List().process(cluster.getSolrClient());
    assertEquals(0, response.getStatus());
    assertNotNull("collection list should not be null", response.getResponse().get("collections"));
  }

  @Test
  @Ignore // MRM TODO: whats status of preferred leader?
  public void testAddAndDeleteReplicaProp() throws InterruptedException, IOException, SolrServerException {

    final String collection = "replicaProperties";
    CollectionAdminRequest.createCollection(collection, "conf", 2, 2).setMaxShardsPerNode(3)
        .process(cluster.getSolrClient());

    final Replica replica = getCollectionState(collection).getLeader("shard1");
    CollectionAdminResponse response
        = CollectionAdminRequest.addReplicaProperty(collection, "s1", replica.getName(), "preferredleader", "true")
        .process(cluster.getSolrClient());
    assertEquals(0, response.getStatus());

    waitForState("Expecting property 'preferredleader' to appear on replica " + replica.getName(), collection,
        (n, c) -> "true".equals(c.getReplica(replica.getName()).getProperty("preferredleader")));

    response = CollectionAdminRequest.deleteReplicaProperty(collection, "s1", replica.getName(), "property.preferredleader")
        .process(cluster.getSolrClient());
    assertEquals(0, response.getStatus());

    waitForState("Expecting property 'preferredleader' to be removed from replica " + replica.getName(), collection,
        (n, c) -> c.getReplica(replica.getName()).getProperty("preferredleader") == null);

  }

  @Test
  @Ignore // MRM TODO: whats status of preferred leader?: Error from server at null: CMD did not return a response:balanceshardunique
  public void testBalanceShardUnique() throws IOException,
      SolrServerException, KeeperException, InterruptedException {

    final String collection = "balancedProperties";
    CollectionAdminRequest.createCollection(collection, "conf", 2, 2)
            .setMaxShardsPerNode(4).process(cluster.getSolrClient());

    CollectionAdminResponse response = CollectionAdminRequest.balanceReplicaProperty(collection, "preferredLeader")
        .process(cluster.getSolrClient());
    assertEquals(0, response.getStatus());

    waitForState("Expecting 'preferredleader' property to be balanced across all shards", collection, (n, c) -> {
      for (Slice slice : c) {
        int count = 0;
        for (Replica replica : slice) {
          if ("true".equals(replica.getProperty("preferredleader")))
            count += 1;
        }
        if (count != 1)
          return false;
      }
      return true;
    });

  }

  @Test
  public void testModifyCollectionAttribute() throws IOException, SolrServerException {
    final String collection = "testAddAndDeleteCollectionAttribute";
    CollectionAdminRequest.createCollection(collection, "conf", 1, 1)
        .process(cluster.getSolrClient());

    CollectionAdminRequest.modifyCollection(collection, null)
        .unsetAttribute("router")
        .process(cluster.getSolrClient());

    waitForState("Expecting attribute 'router' to be deleted", collection,
        (n, c) -> null == c.get("router"));

    LuceneTestCase.expectThrows(IllegalArgumentException.class,
        "An attempt to set unknown collection attribute should have failed",
        () -> CollectionAdminRequest.modifyCollection(collection, null)
            .setAttribute("non_existent_attr", 25)
            .process(cluster.getSolrClient())
    );

    LuceneTestCase.expectThrows(IllegalArgumentException.class,
        "An attempt to set null value should have failed",
        () -> CollectionAdminRequest.modifyCollection(collection, null)
            .setAttribute("non_existent_attr", null)
            .process(cluster.getSolrClient())
    );

    LuceneTestCase.expectThrows(IllegalArgumentException.class,
        "An attempt to unset unknown collection attribute should have failed",
        () -> CollectionAdminRequest.modifyCollection(collection, null)
            .unsetAttribute("non_existent_attr")
            .process(cluster.getSolrClient())
    );
  }
}
