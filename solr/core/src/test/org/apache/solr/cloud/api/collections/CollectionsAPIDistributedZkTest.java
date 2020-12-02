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
package org.apache.solr.cloud.api.collections;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.ObjectName;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.CoreStatus;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoBean.Category;
import org.apache.solr.util.TestInjection;
import org.apache.solr.util.TimeOut;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.CORE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICATION_FACTOR;

/**
 * Tests the Cloud Collections API.
 */
@Slow
public class CollectionsAPIDistributedZkTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected String getConfigSet() {
    return "cloud-minimal";
  }

  @Before
  public void setupCluster() throws Exception {
    // we don't want this test to have zk timeouts
    System.setProperty("zkClientTimeout", "60000");
    System.setProperty("createCollectionWaitTimeTillActive", "5");
    TestInjection.randomDelayInCoreCreation = "true:5";
    System.setProperty("validateAfterInactivity", "200");
    System.setProperty("solr.allowPaths", "*");

    configureCluster(4)
        .addConfig("conf", configset(getConfigSet()))
        .addConfig("conf2", configset(getConfigSet()))
        .withSolrXml(TEST_PATH().resolve("solr.xml"))
        .configure();
  }
  
  @After
  public void tearDownCluster() throws Exception {
    try {
      shutdownCluster();
    } finally {
      System.clearProperty("createCollectionWaitTimeTillActive");
      System.clearProperty("solr.allowPaths");
      super.tearDown();
    }
  }

  @Test
  public void testCreationAndDeletion() throws Exception {
    String collectionName = "created_and_deleted";

    CollectionAdminRequest.createCollection(collectionName, "conf", 1, 1).process(cluster.getSolrClient());
    assertTrue(CollectionAdminRequest.listCollections(cluster.getSolrClient())
                  .contains(collectionName));

    CollectionAdminRequest.deleteCollection(collectionName).process(cluster.getSolrClient());
    assertFalse(CollectionAdminRequest.listCollections(cluster.getSolrClient())
        .contains(collectionName));

    assertFalse(cluster.getZkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, true));
  }

  @Test
  public void deleteCollectionRemovesStaleZkCollectionsNode() throws Exception {
    String collectionName = "out_of_sync_collection";

    // manually create a collections zknode
    cluster.getZkClient().makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, true);

    CollectionAdminRequest.deleteCollection(collectionName)
        .process(cluster.getSolrClient());

    assertFalse(CollectionAdminRequest.listCollections(cluster.getSolrClient())
                  .contains(collectionName));
    
    assertFalse(cluster.getZkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, true));
  }

  @Test
  public void deletePartiallyCreatedCollection() throws Exception {
    final String collectionName = "halfdeletedcollection";

    assertEquals(0, CollectionAdminRequest.createCollection(collectionName, "conf", 2, 1)
        .setCreateNodeSet("")
        .process(cluster.getSolrClient()).getStatus());
    String dataDir = createTempDir().toFile().getAbsolutePath();
    // create a core that simulates something left over from a partially-deleted collection
    assertTrue(CollectionAdminRequest
        .addReplicaToShard(collectionName, "shard1")
        .setDataDir(dataDir)
        .process(cluster.getSolrClient()).isSuccess());

    CollectionAdminRequest.deleteCollection(collectionName)
        .process(cluster.getSolrClient());

    assertFalse(CollectionAdminRequest.listCollections(cluster.getSolrClient()).contains(collectionName));

    CollectionAdminRequest.createCollection(collectionName, "conf", 2, 1)
        .process(cluster.getSolrClient());

    assertTrue(CollectionAdminRequest.listCollections(cluster.getSolrClient()).contains(collectionName));
  }

  @Test
  public void deleteCollectionOnlyInZk() throws Exception {
    final String collectionName = "onlyinzk";

    // create the collections node, but nothing else
    cluster.getZkClient().makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, true);

    // delete via API - should remove collections node
    CollectionAdminRequest.deleteCollection(collectionName).process(cluster.getSolrClient());
    assertFalse(CollectionAdminRequest.listCollections(cluster.getSolrClient()).contains(collectionName));
    
    // now creating that collection should work
    CollectionAdminRequest.createCollection(collectionName, "conf", 2, 1)
        .process(cluster.getSolrClient());
    assertTrue(CollectionAdminRequest.listCollections(cluster.getSolrClient()).contains(collectionName));
  }

  @Test
  public void testBadActionNames() {
    // try a bad action
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", "BADACTION");
    String collectionName = "badactioncollection";
    params.set("name", collectionName);
    params.set("numShards", 2);
    final QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");

    expectThrows(Exception.class, () -> {
      cluster.getSolrClient().request(request);
    });
  }

  @Test
  public void testMissingRequiredParameters() {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionAction.CREATE.toString());
    params.set("numShards", 2);
    // missing required collection parameter
    @SuppressWarnings({"rawtypes"})
    final SolrRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");

    expectThrows(Exception.class, () -> {
      cluster.getSolrClient().request(request);
    });
  }

  @Test
  public void testTooManyReplicas() {
    @SuppressWarnings({"rawtypes"})
    CollectionAdminRequest req = CollectionAdminRequest.createCollection("collection", "conf", 2, 10);

    expectThrows(Exception.class, () -> {
      cluster.getSolrClient().request(req);
    });
  }

  @Test
  public void testMissingNumShards() {
    // No numShards should fail
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionAction.CREATE.toString());
    params.set("name", "acollection");
    params.set(REPLICATION_FACTOR, 10);
    params.set("collection.configName", "conf");

    @SuppressWarnings({"rawtypes"})
    final SolrRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");

    expectThrows(Exception.class, () -> {
      cluster.getSolrClient().request(request);
    });
  }

  @Test
  public void testZeroNumShards() {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionAction.CREATE.toString());
    params.set("name", "acollection");
    params.set(REPLICATION_FACTOR, 10);
    params.set("numShards", 0);
    params.set("collection.configName", "conf");

    @SuppressWarnings({"rawtypes"})
    final SolrRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    expectThrows(Exception.class, () -> {
      cluster.getSolrClient().request(request);
    });
  }

  @Test
  public void testCreateShouldFailOnExistingCore() throws Exception {
    assertEquals(0, CollectionAdminRequest.createCollection("halfcollectionblocker", "conf", 1, 1)
        .setCreateNodeSet("")
        .process(cluster.getSolrClient()).getStatus());
    assertTrue(CollectionAdminRequest.addReplicaToShard("halfcollectionblocker", "shard1")
        .setNode(cluster.getJettySolrRunner(0).getNodeName())
        .setCoreName("halfcollection_shard1_replica_n1")
        .process(cluster.getSolrClient()).isSuccess());

    assertEquals(0, CollectionAdminRequest.createCollection("halfcollectionblocker2", "conf",1, 1)
        .setCreateNodeSet("")
        .process(cluster.getSolrClient()).getStatus());
    assertTrue(CollectionAdminRequest.addReplicaToShard("halfcollectionblocker2", "shard1")
        .setNode(cluster.getJettySolrRunner(1).getNodeName())
        .setCoreName("halfcollection_shard1_replica_n1")
        .process(cluster.getSolrClient()).isSuccess());

    String nn1 = cluster.getJettySolrRunner(0).getNodeName();
    String nn2 = cluster.getJettySolrRunner(1).getNodeName();

    expectThrows(HttpSolrClient.RemoteSolrException.class, () -> {
      CollectionAdminResponse resp = CollectionAdminRequest.createCollection("halfcollection", "conf", 2, 1)
          .setCreateNodeSet(nn1 + "," + nn2)
          .process(cluster.getSolrClient());
    });
  }

  @Test
  public void testNoConfigSetExist() throws Exception {
    expectThrows(Exception.class, () -> {
      CollectionAdminRequest.createCollection("noconfig", "conf123", 1, 1)
          .process(cluster.getSolrClient());
    });

    TimeUnit.MILLISECONDS.sleep(1000);
    // in both cases, the collection should have default to the core name
    cluster.getSolrClient().getZkStateReader().forceUpdateCollection("noconfig");
    assertFalse(CollectionAdminRequest.listCollections(cluster.getSolrClient()).contains("noconfig"));
  }

  @Test
  public void testCoresAreDistributedAcrossNodes() throws Exception {
    CollectionAdminRequest.createCollection("nodes_used_collection", "conf", 2, 2)
        .process(cluster.getSolrClient());

    Set<String> liveNodes = cluster.getSolrClient().getZkStateReader().getClusterState().getLiveNodes();

    List<String> createNodeList = new ArrayList<>(liveNodes);

    DocCollection collection = getCollectionState("nodes_used_collection");
    for (Slice slice : collection.getSlices()) {
      for (Replica replica : slice.getReplicas()) {
        createNodeList.remove(replica.getNodeName());
      }
    }

    assertEquals(createNodeList.toString(), 0, createNodeList.size());
  }

  @Test
  public void testDeleteNonExistentCollection() throws Exception {

    expectThrows(SolrException.class, () -> {
      CollectionAdminRequest.deleteCollection("unknown_collection").process(cluster.getSolrClient());
    });

    // create another collection should still work
    CollectionAdminRequest.createCollection("acollectionafterbaddelete", "conf", 1, 2)
        .process(cluster.getSolrClient());
    waitForState("Collection creation after a bad delete failed", "acollectionafterbaddelete",
        (n, c) -> DocCollection.isFullyActive(n, c, 1, 2));
  }

  @Test
  public void testSpecificConfigsets() throws Exception {
    CollectionAdminRequest.createCollection("withconfigset2", "conf2", 1, 1).process(cluster.getSolrClient());
    byte[] data = zkClient().getData(ZkStateReader.COLLECTIONS_ZKNODE + "/" + "withconfigset2", null, null, true);
    assertNotNull(data);
    ZkNodeProps props = ZkNodeProps.load(data);
    String configName = props.getStr(ZkController.CONFIGNAME_PROP);
    assertEquals("conf2", configName);
  }

  @Test
  public void testMaxNodesPerShard() {
    int numLiveNodes = cluster.getJettySolrRunners().size();
    int numShards = (numLiveNodes/2) + 1;
    int replicationFactor = 2;

    expectThrows(SolrException.class, () -> {
      CollectionAdminRequest.createCollection("oversharded", "conf", numShards, replicationFactor)
          .process(cluster.getSolrClient());
    });
  }

  @Test
  public void testCreateNodeSet() throws Exception {
    JettySolrRunner jetty1 = cluster.getRandomJetty(random());
    JettySolrRunner jetty2 = cluster.getRandomJetty(random());

    List<String> baseUrls = ImmutableList.of(jetty1.getBaseUrl().toString(), jetty2.getBaseUrl().toString());

    CollectionAdminRequest.createCollection("nodeset_collection", "conf", 2, 1)
        .setCreateNodeSet(baseUrls.get(0) + "," + baseUrls.get(1))
        .process(cluster.getSolrClient());

    DocCollection collectionState = getCollectionState("nodeset_collection");
    for (Replica replica : collectionState.getReplicas()) {
      String replicaUrl = replica.getCoreUrl();
      boolean matchingJetty = false;
      for (String jettyUrl : baseUrls) {
        if (replicaUrl.startsWith(jettyUrl)) {
          matchingJetty = true;
        }
      }
      if (matchingJetty == false) {
        fail("Expected replica to be on " + baseUrls + " but was on " + replicaUrl);
      }
    }
  }

  @Test
  //28-June-2018 @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028")
  // See: https://issues.apache.org/jira/browse/SOLR-12028 Tests cannot remove files on Windows machines occasionally
  // commented out on: 24-Dec-2018   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 09-Aug-2018 SOLR-12028
  public void testCollectionsAPI() throws Exception {

    // create new collections rapid fire
    int cnt = random().nextInt(TEST_NIGHTLY ? 3 : 1) + 1;
    CollectionAdminRequest.Create[] createRequests = new CollectionAdminRequest.Create[cnt];
    
    class Coll {
      String name;
      int numShards;
      int replicationFactor;
    }
    
    List<Coll> colls = new ArrayList<>();

    for (int i = 0; i < cnt; i++) {

      int numShards = TestUtil.nextInt(random(), 0, cluster.getJettySolrRunners().size()) + 1;
      int replicationFactor = TestUtil.nextInt(random(), 0, 3) + 1;
      int maxShardsPerNode = (((numShards * replicationFactor) / cluster.getJettySolrRunners().size())) + 1;

      createRequests[i]
          = CollectionAdminRequest.createCollection("awhollynewcollection_" + i, "conf2", numShards, replicationFactor)
          .setMaxShardsPerNode(maxShardsPerNode);
      createRequests[i].processAsync(cluster.getSolrClient());
      
      Coll coll = new Coll();
      coll.name = "awhollynewcollection_" + i;
      coll.numShards = numShards;
      coll.replicationFactor = replicationFactor;
      colls.add(coll);
    }

    for (Coll coll : colls) {
      cluster.waitForActiveCollection(coll.name, coll.numShards, coll.numShards * coll.replicationFactor);
    }

    waitForStable(cnt, createRequests);

    for (int i = 0; i < cluster.getJettySolrRunners().size(); i++) {
      checkInstanceDirs(cluster.getJettySolrRunner(i));
    }
    
    String collectionName = createRequests[random().nextInt(createRequests.length)].getCollectionName();
    
    // TODO: we should not need this...beast test well when trying to fix
    Thread.sleep(1000);
    
    cluster.getSolrClient().getZkStateReader().forciblyRefreshAllClusterStateSlow();

    new UpdateRequest()
        .add("id", "6")
        .add("id", "7")
        .add("id", "8")
        .commit(cluster.getSolrClient(), collectionName);
    long numFound = 0;
    TimeOut timeOut = new TimeOut(10, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    while (!timeOut.hasTimedOut()) {

      numFound = cluster.getSolrClient().query(collectionName, new SolrQuery("*:*")).getResults().getNumFound();
      if (numFound == 3) {
        break;
      }

      Thread.sleep(500);
    }
    
    if (timeOut.hasTimedOut()) {
      fail("Timeout waiting to see 3 found, instead saw " + numFound + " for collection " + collectionName);
    }

    checkNoTwoShardsUseTheSameIndexDir();
  }

  private void waitForStable(int cnt, CollectionAdminRequest.Create[] createRequests) throws InterruptedException {
    for (int i = 0; i < cnt; i++) {
      String collectionName = "awhollynewcollection_" + i;
      final int j = i;
      waitForState("Expected to see collection " + collectionName, collectionName,
          (n, c) -> {
            CollectionAdminRequest.Create req = createRequests[j];
            return DocCollection.isFullyActive(n, c, req.getNumShards(), req.getReplicationFactor());
          });
      
      ZkStateReader zkStateReader = cluster.getSolrClient().getZkStateReader();
      // make sure we have leaders for each shard
      for (int z = 1; z < createRequests[j].getNumShards(); z++) {
        zkStateReader.getLeaderRetry(collectionName, "shard" + z, 10000);
      }      // make sure we again have leaders for each shard
    }
  }

  @Test
  public void testCollectionReload() throws Exception {
    final String collectionName = "reloaded_collection";
    CollectionAdminRequest.createCollection(collectionName, "conf", 2, 2).process(cluster.getSolrClient());

    // get core open times
    Map<String, Long> urlToTimeBefore = new HashMap<>();
    collectStartTimes(collectionName, urlToTimeBefore);
    assertTrue(urlToTimeBefore.size() > 0);

    CollectionAdminRequest.reloadCollection(collectionName).processAsync(cluster.getSolrClient());

    // reloads make take a short while
    boolean allTimesAreCorrect = waitForReloads(collectionName, urlToTimeBefore);
    assertTrue("some core start times did not change on reload", allTimesAreCorrect);
  }

  private void checkInstanceDirs(JettySolrRunner jetty) throws IOException {
    CoreContainer cores = jetty.getCoreContainer();
    Collection<SolrCore> theCores = cores.getCores();
    for (SolrCore core : theCores) {
      // look for core props file
      Path instancedir = core.getInstancePath();
      assertTrue("Could not find expected core.properties file", Files.exists(instancedir.resolve("core.properties")));

      Path expected = Paths.get(jetty.getSolrHome()).toAbsolutePath().resolve(core.getName());

      assertTrue("Expected: " + expected + "\nFrom core stats: " + instancedir, Files.isSameFile(expected, instancedir));
    }
  }

  private boolean waitForReloads(String collectionName, Map<String,Long> urlToTimeBefore) throws SolrServerException, IOException {
    TimeOut timeout = new TimeOut(45, TimeUnit.SECONDS, TimeSource.NANO_TIME);

    boolean allTimesAreCorrect = false;
    while (! timeout.hasTimedOut()) {
      Map<String,Long> urlToTimeAfter = new HashMap<>();
      collectStartTimes(collectionName, urlToTimeAfter);
      
      boolean retry = false;
      Set<Entry<String,Long>> entries = urlToTimeBefore.entrySet();
      for (Entry<String,Long> entry : entries) {
        Long beforeTime = entry.getValue();
        Long afterTime = urlToTimeAfter.get(entry.getKey());
        assertNotNull(afterTime);
        if (afterTime <= beforeTime) {
          retry = true;
          break;
        }

      }
      if (!retry) {
        allTimesAreCorrect = true;
        break;
      }
    }
    return allTimesAreCorrect;
  }

  private void collectStartTimes(String collectionName, Map<String,Long> urlToTime)
      throws SolrServerException, IOException {

    DocCollection collectionState = getCollectionState(collectionName);
    if (collectionState != null) {
      for (Slice shard : collectionState) {
        for (Replica replica : shard) {
          ZkCoreNodeProps coreProps = new ZkCoreNodeProps(replica);
          CoreStatus coreStatus;
          try (HttpSolrClient server = getHttpSolrClient(coreProps.getBaseUrl())) {
            coreStatus = CoreAdminRequest.getCoreStatus(coreProps.getCoreName(), false, server);
          }
          long before = coreStatus.getCoreStartTime().getTime();
          urlToTime.put(coreProps.getCoreUrl(), before);
        }
      }
    } else {
      throw new IllegalArgumentException("Could not find collection " + collectionName);
    }
  }
  
  private void checkNoTwoShardsUseTheSameIndexDir() {
    Map<String, Set<String>> indexDirToShardNamesMap = new HashMap<>();
    
    List<MBeanServer> servers = new LinkedList<>();
    servers.add(ManagementFactory.getPlatformMBeanServer());
    servers.addAll(MBeanServerFactory.findMBeanServer(null));
    for (final MBeanServer server : servers) {
      Set<ObjectName> mbeans = new HashSet<>(server.queryNames(null, null));
      for (final ObjectName mbean : mbeans) {
        try {
          Map<String, String> props = mbean.getKeyPropertyList();
          String category = props.get("category");
          String name = props.get("name");
          if ((category != null && category.equals(Category.CORE.toString())) &&
              (name != null && name.equals("indexDir"))) {
            String indexDir = server.getAttribute(mbean, "Value").toString();
            String key = props.get("dom2") + "." + props.get("dom3") + "." + props.get("dom4");
            if (!indexDirToShardNamesMap.containsKey(indexDir)) {
              indexDirToShardNamesMap.put(indexDir, new HashSet<>());
            }
            indexDirToShardNamesMap.get(indexDir).add(key);
          }
        } catch (Exception e) {
          // ignore, just continue - probably a "Value" attribute
          // not found
        }
      }
    }
    
    assertTrue(
        "Something is broken in the assert for no shards using the same indexDir - probably something was changed in the attributes published in the MBean of "
            + SolrCore.class.getSimpleName() + " : " + indexDirToShardNamesMap,
        indexDirToShardNamesMap.size() > 0);
    for (Entry<String,Set<String>> entry : indexDirToShardNamesMap.entrySet()) {
      if (entry.getValue().size() > 1) {
        fail("We have shards using the same indexDir. E.g. shards "
            + entry.getValue().toString() + " all use indexDir "
            + entry.getKey());
      }
    }
  }

  @Test
  public void addReplicaTest() throws Exception {
    String collectionName = "addReplicaColl";

    CollectionAdminRequest.createCollection(collectionName, "conf", 2, 2)
        .setMaxShardsPerNode(4)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(collectionName, 2, 4);

    ArrayList<String> nodeList
        = new ArrayList<>(cluster.getSolrClient().getZkStateReader().getClusterState().getLiveNodes());
    Collections.shuffle(nodeList, random());

    CollectionAdminResponse response = CollectionAdminRequest.addReplicaToShard(collectionName, "shard1")
        .setNode(nodeList.get(0))
        .process(cluster.getSolrClient());
    Replica newReplica = grabNewReplica(response, getCollectionState(collectionName));

    assertEquals("Replica should be created on the right node",
        cluster.getSolrClient().getZkStateReader().getBaseUrlForNodeName(nodeList.get(0)), newReplica.getBaseUrl());

    Path instancePath = createTempDir();
    response = CollectionAdminRequest.addReplicaToShard(collectionName, "shard1")
        .withProperty(CoreAdminParams.INSTANCE_DIR, instancePath.toString())
        .process(cluster.getSolrClient());
    newReplica = grabNewReplica(response, getCollectionState(collectionName));
    assertNotNull(newReplica);

    try (HttpSolrClient coreclient = getHttpSolrClient(newReplica.getBaseUrl())) {
      CoreAdminResponse status = CoreAdminRequest.getStatus(newReplica.getStr("core"), coreclient);
      NamedList<Object> coreStatus = status.getCoreStatus(newReplica.getStr("core"));
      String instanceDirStr = (String) coreStatus.get("instanceDir");
      assertEquals(instanceDirStr, instancePath.toString());
    }

    //Test to make sure we can't create another replica with an existing core_name of that collection
    String coreName = newReplica.getStr(CORE_NAME_PROP);
    SolrException e = expectThrows(SolrException.class, () -> {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("action", "addreplica");
      params.set("collection", collectionName);
      params.set("shard", "shard1");
      params.set("name", coreName);
      QueryRequest request = new QueryRequest(params);
      request.setPath("/admin/collections");
      cluster.getSolrClient().request(request);
    });

    assertTrue(e.getMessage().contains("Another replica with the same core name already exists for this collection"));

    // Check that specifying property.name works. DO NOT remove this when the "name" property is deprecated
    // for ADDREPLICA, this is "property.name". See SOLR-7132
    response = CollectionAdminRequest.addReplicaToShard(collectionName, "shard1")
        .withProperty(CoreAdminParams.NAME, "propertyDotName")
        .process(cluster.getSolrClient());

    newReplica = grabNewReplica(response, getCollectionState(collectionName));
    assertEquals("'core' should be 'propertyDotName' ", "propertyDotName", newReplica.getStr("core"));
  }

  private Replica grabNewReplica(CollectionAdminResponse response, DocCollection docCollection) {
    String replicaName = response.getCollectionCoresStatus().keySet().iterator().next();
    Optional<Replica> optional = docCollection.getReplicas().stream()
        .filter(replica -> replicaName.equals(replica.getCoreName()))
        .findAny();
    if (optional.isPresent()) {
      return optional.get();
    }
    throw new AssertionError("Can not find " + replicaName + " from " + docCollection);
  }
}
