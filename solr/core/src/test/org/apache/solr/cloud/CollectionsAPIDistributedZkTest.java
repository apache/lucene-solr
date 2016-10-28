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

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.ObjectName;
import java.io.IOException;
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
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest.Create;
import org.apache.solr.client.solrj.request.CoreStatus;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
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
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoMBean.Category;
import org.apache.solr.util.TestInjection;
import org.apache.solr.util.TimeOut;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.solr.common.cloud.ZkStateReader.CORE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICATION_FACTOR;

/**
 * Tests the Cloud Collections API.
 */
@Slow
public class CollectionsAPIDistributedZkTest extends SolrCloudTestCase {

  @BeforeClass
  public static void beforeCollectionsAPIDistributedZkTest() {
    TestInjection.randomDelayInCoreCreation = "true:20";
    System.setProperty("validateAfterInactivity", "200");
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(4)
        .addConfig("conf", configset("cloud-minimal"))
        .addConfig("conf2", configset("cloud-minimal-jmx"))
        .configure();
  }

  @Before
  public void clearCluster() throws Exception {
    cluster.deleteAllCollections();
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

    // create a core that simulates something left over from a partially-deleted collection
    Create createCmd = new Create();
    createCmd.setCoreName("halfdeletedcollection_shard1_replica1");
    createCmd.setCollection(collectionName);
    createCmd.setCollectionConfigName("conf");
    String dataDir = createTempDir().toFile().getAbsolutePath();
    createCmd.setDataDir(dataDir);
    createCmd.setNumShards(2);

    createCmd.process(cluster.getSolrClient());

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
  public void testBadActionNames() throws Exception {

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
    final SolrRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");

    expectThrows(Exception.class, () -> {
      cluster.getSolrClient().request(request);
    });
  }

  @Test
  public void testTooManyReplicas() {

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

    final SolrRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    expectThrows(Exception.class, () -> {
      cluster.getSolrClient().request(request);
    });

  }

  @Test
  public void testCreateShouldFailOnExistingCore() throws Exception {
    
    // first we make a core with the core name the collections api
    // will try and use - this will cause our mock fail
    Create createCmd = new Create();
    createCmd.setCoreName("halfcollection_shard1_replica1");
    createCmd.setCollection("halfcollectionblocker");
    String dataDir = createTempDir().toFile().getAbsolutePath();
    createCmd.setDataDir(dataDir);
    createCmd.setNumShards(1);
    createCmd.setCollectionConfigName("conf");

    try (SolrClient client = cluster.getJettySolrRunner(0).newClient()) {
      client.request(createCmd);
    }

    createCmd = new Create();
    createCmd.setCoreName("halfcollection_shard1_replica1");
    createCmd.setCollection("halfcollectionblocker2");
    dataDir = createTempDir().toFile().getAbsolutePath();
    createCmd.setDataDir(dataDir);
    createCmd.setNumShards(1);
    createCmd.setCollectionConfigName("conf");

    try (SolrClient client = cluster.getJettySolrRunner(1).newClient()) {
      client.request(createCmd);
    }

    String nn1 = cluster.getJettySolrRunner(0).getNodeName();
    String nn2 = cluster.getJettySolrRunner(1).getNodeName();

    CollectionAdminResponse resp = CollectionAdminRequest.createCollection("halfcollection", "conf", 2, 1)
        .setCreateNodeSet(nn1 + "," + nn2)
        .process(cluster.getSolrClient());
    
    SimpleOrderedMap success = (SimpleOrderedMap) resp.getResponse().get("success");
    SimpleOrderedMap failure = (SimpleOrderedMap) resp.getResponse().get("failure");

    assertNotNull(resp.toString(), success);
    assertNotNull(resp.toString(), failure);
    
    String val1 = success.getVal(0).toString();
    String val2 = failure.getVal(0).toString();
    assertTrue(val1.contains("SolrException") || val2.contains("SolrException"));
  }

  @Test
  public void testNoCollectionSpecified() throws Exception {

    // TODO - should we remove this behaviour?

    assertFalse(cluster.getSolrClient().getZkStateReader().getClusterState().hasCollection("corewithnocollection"));
    assertFalse(cluster.getSolrClient().getZkStateReader().getClusterState().hasCollection("corewithnocollection2"));
    
    // try and create a SolrCore with no collection name
    Create createCmd = new Create();
    createCmd.setCoreName("corewithnocollection");
    createCmd.setCollection("");
    String dataDir = createTempDir().toFile().getAbsolutePath();
    createCmd.setDataDir(dataDir);
    createCmd.setNumShards(1);
    createCmd.setCollectionConfigName("conf");

    cluster.getSolrClient().request(createCmd);
    
    // try and create a SolrCore with no collection name
    createCmd.setCollection(null);
    createCmd.setCoreName("corewithnocollection2");

    cluster.getSolrClient().request(createCmd);
    
    // in both cases, the collection should have default to the core name
    cluster.getSolrClient().getZkStateReader().forceUpdateCollection("corewithnocollection");
    cluster.getSolrClient().getZkStateReader().forceUpdateCollection("corewithnocollection2");
    assertTrue(cluster.getSolrClient().getZkStateReader().getClusterState().hasCollection("corewithnocollection"));
    assertTrue(cluster.getSolrClient().getZkStateReader().getClusterState().hasCollection("corewithnocollection2"));
  }

  @Test
  public void testNoConfigSetExist() throws Exception {

    final CloudSolrClient cloudClient = cluster.getSolrClient();

    assertFalse(cloudClient.getZkStateReader().getClusterState().hasCollection("corewithnocollection3"));

    // try and create a SolrCore with no collection name
    Create createCmd = new Create();
    createCmd.setCoreName("corewithnocollection3");
    createCmd.setCollection("");
    String dataDir = createTempDir().toFile().getAbsolutePath();
    createCmd.setDataDir(dataDir);
    createCmd.setNumShards(1);
    createCmd.setCollectionConfigName("conf123");

    expectThrows(Exception.class, () -> {
      cluster.getSolrClient().request(createCmd);
    });

    TimeUnit.MILLISECONDS.sleep(200);
    // in both cases, the collection should have default to the core name
    cloudClient.getZkStateReader().forceUpdateCollection("corewithnocollection3");

    Collection<Slice> slices = cloudClient.getZkStateReader().getClusterState().getActiveSlices("corewithnocollection3");
    int replicaCount = 0;
    if (slices != null) {
      for (Slice slice : slices) {
        replicaCount += slice.getReplicas().size();
      }
    }
    assertEquals("replicaCount", 0, replicaCount);

    // TODO - WTF? shouldn't this *not* contain the collection?
    assertTrue(CollectionAdminRequest.listCollections(cloudClient).contains("corewithnocollection3"));

  }

  @Test
  public void testCoresAreDistributedAcrossNodes() throws Exception {

    CollectionAdminRequest.createCollection("nodes_used_collection", "conf", 2, 2)
        .process(cluster.getSolrClient());

    Set<String> liveNodes = cluster.getSolrClient().getZkStateReader().getClusterState().getLiveNodes();

    List<String> createNodeList = new ArrayList<>();
    createNodeList.addAll(liveNodes);

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

    SolrException e = expectThrows(SolrException.class, () -> {
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
  public void testMaxNodesPerShard() throws Exception {

    // test maxShardsPerNode
    int numLiveNodes = cluster.getJettySolrRunners().size();
    int numShards = (numLiveNodes/2) + 1;
    int replicationFactor = 2;
    int maxShardsPerNode = 1;

    SolrException e = expectThrows(SolrException.class, () -> {
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
        if (replicaUrl.startsWith(jettyUrl))
          matchingJetty = true;
      }
      if (matchingJetty == false)
        fail("Expected replica to be on " + baseUrls + " but was on " + replicaUrl);
    }

  }

  @Test
  public void testCollectionsAPI() throws Exception {

    // create new collections rapid fire
    int cnt = random().nextInt(TEST_NIGHTLY ? 6 : 1) + 1;
    CollectionAdminRequest.Create[] createRequests = new CollectionAdminRequest.Create[cnt];

    for (int i = 0; i < cnt; i++) {

      int numShards = TestUtil.nextInt(random(), 0, cluster.getJettySolrRunners().size()) + 1;
      int replicationFactor = TestUtil.nextInt(random(), 0, 3) + 1;
      int maxShardsPerNode = (((numShards * replicationFactor) / cluster.getJettySolrRunners().size())) + 1;

      createRequests[i]
          = CollectionAdminRequest.createCollection("awhollynewcollection_" + i, "conf2", numShards, replicationFactor)
          .setMaxShardsPerNode(maxShardsPerNode);
      createRequests[i].processAsync(cluster.getSolrClient());
    }

    for (int i = 0; i < cnt; i++) {
      String collectionName = "awhollynewcollection_" + i;
      final int j = i;
      waitForState("Expected to see collection " + collectionName, collectionName,
          (n, c) -> {
            CollectionAdminRequest.Create req = createRequests[j];
            return DocCollection.isFullyActive(n, c, req.getNumShards(), req.getReplicationFactor());
          });
    }

    cluster.injectChaos(random());

    for (int i = 0; i < cluster.getJettySolrRunners().size(); i++) {
      checkInstanceDirs(cluster.getJettySolrRunner(i));
    }

    String collectionName = createRequests[random().nextInt(createRequests.length)].getCollectionName();

    new UpdateRequest()
        .add("id", "6")
        .add("id", "7")
        .add("id", "8")
        .commit(cluster.getSolrClient(), collectionName);
    assertEquals(3, cluster.getSolrClient().query(collectionName, new SolrQuery("*:*")).getResults().getNumFound());

    checkNoTwoShardsUseTheSameIndexDir();
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
      Path instancedir = (Path) core.getStatistics().get("instanceDir");
      assertTrue("Could not find expected core.properties file", Files.exists(instancedir.resolve("core.properties")));

      Path expected = Paths.get(jetty.getSolrHome()).toAbsolutePath().resolve(core.getName());

      assertTrue("Expected: " + expected + "\nFrom core stats: " + instancedir, Files.isSameFile(expected, instancedir));

    }
  }

  private boolean waitForReloads(String collectionName, Map<String,Long> urlToTimeBefore) throws SolrServerException, IOException {


    TimeOut timeout = new TimeOut(45, TimeUnit.SECONDS);

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
  
  private void checkNoTwoShardsUseTheSameIndexDir() throws Exception {
    Map<String, Set<String>> indexDirToShardNamesMap = new HashMap<>();
    
    List<MBeanServer> servers = new LinkedList<>();
    servers.add(ManagementFactory.getPlatformMBeanServer());
    servers.addAll(MBeanServerFactory.findMBeanServer(null));
    for (final MBeanServer server : servers) {
      Set<ObjectName> mbeans = new HashSet<>();
      mbeans.addAll(server.queryNames(null, null));
      for (final ObjectName mbean : mbeans) {
        Object value;
        Object indexDir;
        Object name;

        try {
          if (((value = server.getAttribute(mbean, "category")) != null && value
              .toString().equals(Category.CORE.toString()))
              && ((indexDir = server.getAttribute(mbean, "coreName")) != null)
              && ((indexDir = server.getAttribute(mbean, "indexDir")) != null)
              && ((name = server.getAttribute(mbean, "name")) != null)) {
            if (!indexDirToShardNamesMap.containsKey(indexDir.toString())) {
              indexDirToShardNamesMap.put(indexDir.toString(),
                  new HashSet<String>());
            }
            indexDirToShardNamesMap.get(indexDir.toString()).add(
                name.toString());
          }
        } catch (Exception e) {
          // ignore, just continue - probably a "category" or "source" attribute
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

    ArrayList<String> nodeList
        = new ArrayList<>(cluster.getSolrClient().getZkStateReader().getClusterState().getLiveNodes());
    Collections.shuffle(nodeList, random());

    String newReplicaName = Assign.assignNode(getCollectionState(collectionName));
    CollectionAdminRequest.addReplicaToShard(collectionName, "shard1")
        .setNode(nodeList.get(0))
        .process(cluster.getSolrClient());

    Replica newReplica = getCollectionState(collectionName).getReplica(newReplicaName);

    assertEquals("Replica should be created on the right node",
        cluster.getSolrClient().getZkStateReader().getBaseUrlForNodeName(nodeList.get(0)),
        newReplica.getStr(ZkStateReader.BASE_URL_PROP));

    newReplicaName = Assign.assignNode(getCollectionState(collectionName));
    Path instancePath = createTempDir();
    CollectionAdminRequest.addReplicaToShard(collectionName, "shard1")
        .withProperty(CoreAdminParams.INSTANCE_DIR, instancePath.toString())
        .process(cluster.getSolrClient());

    newReplica = getCollectionState(collectionName).getReplica(newReplicaName);
    assertNotNull(newReplica);

    try (HttpSolrClient coreclient = getHttpSolrClient(newReplica.getStr(ZkStateReader.BASE_URL_PROP))) {
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
    newReplicaName = Assign.assignNode(getCollectionState(collectionName));
    CollectionAdminRequest.addReplicaToShard(collectionName, "shard1")
        .withProperty(CoreAdminParams.NAME, "propertyDotName")
        .process(cluster.getSolrClient());

    newReplica = getCollectionState(collectionName).getReplica(newReplicaName);
    assertEquals("'core' should be 'propertyDotName' ", "propertyDotName", newReplica.getStr("core"));

  }

}
