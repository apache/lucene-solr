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

import org.apache.curator.shaded.com.google.common.collect.ImmutableList;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.CoreStatus;
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
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoBean.Category;
import org.apache.solr.util.TestInjection;
import org.apache.solr.util.TimeOut;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Tests the Cloud Collections API.
 */
public class CollectionsAPIDistClusterPerZkTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  protected static volatile String configSet = "cloud-minimal";
  protected static String getConfigSet() {
    return configSet;
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    useFactory(null);
    // we don't want this test to have zk timeouts
    System.setProperty("zkClientTimeout", "60000");
    if (TEST_NIGHTLY) {
      System.setProperty("createCollectionWaitTimeTillActive", "100");
      TestInjection.randomDelayInCoreCreation = "true:5";
    } else {
      System.setProperty("createCollectionWaitTimeTillActive", "100");
    }

    configureCluster(TEST_NIGHTLY ? 4 : 2)
        .addConfig("conf", configset(getConfigSet()))
        .addConfig("conf2", configset(getConfigSet()))
        .withSolrXml(TEST_PATH().resolve("solr.xml"))
        .configure();
  }
  
  @After
  public void tearDownCluster() throws Exception {
    cluster.deleteAllCollections();
  }

  @Test
  @Ignore
  public void deleteCollectionOnlyInZk() throws Exception {
    final String collectionName = "onlyinzk";

    // create the collections node
    CollectionAdminRequest.createCollection(collectionName, "conf", 2, 1)
            .process(cluster.getSolrClient());

    // delete via API - should remove collections node
    CollectionAdminRequest.deleteCollection(collectionName).process(cluster.getSolrClient());
    assertFalse(CollectionAdminRequest.listCollections(cluster.getSolrClient()).contains(collectionName));
    
    // now creating that collection should work
    CollectionAdminRequest.createCollection(collectionName, "conf", 2, 1)
        .process(cluster.getSolrClient());
    assertTrue(CollectionAdminRequest.listCollections(cluster.getSolrClient()).contains(collectionName));
  }

  @Test
  @Ignore // nocommit we can speed this up, TJP ~ WIP: fails
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

    expectThrows(BaseHttpSolrClient.RemoteSolrException.class, () -> {
      CollectionAdminResponse resp = CollectionAdminRequest.createCollection("halfcollection", "conf", 2, 1)
          .setCreateNodeSet(nn1 + "," + nn2)
          .process(cluster.getSolrClient());
    });
  }

  @Test
  @Nightly // needs 4 nodes
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
  public void testSpecificConfigsets() throws Exception {
    CollectionAdminRequest.createCollection("withconfigset2", "conf2", 1, 1).process(cluster.getSolrClient());
    byte[] data = zkClient().getData(ZkStateReader.COLLECTIONS_ZKNODE + "/" + "withconfigset2", null, null);
    assertNotNull(data);
    ZkNodeProps props = ZkNodeProps.load(data);
    String configName = props.getStr(ZkController.CONFIGNAME_PROP);
    assertEquals("conf2", configName);
  }

  @Test
  public void testCreateNodeSet() throws Exception {
    JettySolrRunner jetty1 = null;
    JettySolrRunner jetty2 = null;
    final List<JettySolrRunner> runners = cluster.getJettySolrRunners();
    if (runners.size() == 2) {
      jetty1 = runners.get(0);
      jetty2 = runners.get(1);
    } else if (runners.size() > 2) {
      jetty1 = cluster.getRandomJetty(random());
      jetty2 = cluster.getRandomJetty(random(), jetty1);
    } else {
      fail("This test requires at least 2 Jetty runners!");
    }

    List<String> baseUrls = ImmutableList.of(jetty1.getCoreContainer().getZkController().getNodeName(), jetty2.getCoreContainer().getZkController().getNodeName());

    CollectionAdminRequest.createCollection("nodeset_collection", "conf", 2, 1)
        .setCreateNodeSet(baseUrls.get(0) + "," + baseUrls.get(1))
        .process(cluster.getSolrClient());

    DocCollection collectionState = getCollectionState("nodeset_collection");
    for (Replica replica : collectionState.getReplicas()) {
      String node = replica.getNodeName();
      boolean matchingJetty = false;
      for (String jettyNode : baseUrls) {
        if (node.equals(jettyNode)) {
          matchingJetty = true;
        }
      }
      if (matchingJetty == false) {
        fail("Expected replica to be on " + baseUrls + " but was on " + node);
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
      int maxShardsPerNode = 100;

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

    for (CollectionAdminRequest.Create create : createRequests) {
      if (create == null) continue;
      try {
        cluster.waitForActiveCollection(create.getCollectionName(), create.getNumShards(), create.getNumShards() * create.getTotaleReplicaCount());
      } catch(Exception e) {
        throw new RuntimeException(create.getParams().toString(), e);
      }
    }

    for (int i = 0; i < cluster.getJettySolrRunners().size(); i++) {
      checkInstanceDirs(cluster.getJettySolrRunner(i));
    }
    
    String collectionName = createRequests[random().nextInt(createRequests.length)].getCollectionName();

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

      Thread.sleep(100);
    }
    
    if (timeOut.hasTimedOut()) {
      fail("Timeout waiting to see 3 found, instead saw " + numFound + " for collection " + collectionName);
    }

    // checkNoTwoShardsUseTheSameIndexDir();
  }

  @Test
  public void testCollectionReload() throws Exception {
    final String collectionName = "reloaded_collection";
    CollectionAdminRequest.createCollection(collectionName, "conf", 2, 2).setMaxShardsPerNode(10).process(cluster.getSolrClient());

    // get core open times
    Map<String, Long> urlToTimeBefore = new HashMap<>();
    collectStartTimes(collectionName, urlToTimeBefore);
    assertTrue(urlToTimeBefore.size() > 0);

    Thread.sleep(50);

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
      if (Files.exists(expected) && Files.exists(instancedir)) {
        assertTrue("Expected: " + expected + "\nFrom core stats: " + instancedir, Files.isSameFile(expected, instancedir));
      }
    }
  }

  private boolean waitForReloads(String collectionName, Map<String,Long> urlToTimeBefore) throws SolrServerException, IOException {
    TimeOut timeout = new TimeOut(5, TimeUnit.SECONDS, TimeSource.NANO_TIME);

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
          Replica coreProps = replica;
          CoreStatus coreStatus;
          try (Http2SolrClient server = SolrTestCaseJ4.getHttpSolrClient(coreProps.getBaseUrl())) {
            coreStatus = CoreAdminRequest.getCoreStatus(coreProps.getName(), false, server);
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
        .setMaxShardsPerNode(6)
        .process(cluster.getSolrClient());

    ArrayList<String> nodeList
        = new ArrayList<>(cluster.getSolrClient().getZkStateReader().getClusterState().getLiveNodes());
    Collections.shuffle(nodeList, random());

    CollectionAdminResponse response = CollectionAdminRequest.addReplicaToShard(collectionName, "s1")
        .setNode(nodeList.get(0))
        .process(cluster.getSolrClient());
    Replica newReplica = grabNewReplica(response, getCollectionState(collectionName));

    assertEquals("Replica should be created on the right node",
        cluster.getSolrClient().getZkStateReader().getBaseUrlForNodeName(nodeList.get(0)),
        newReplica.getStr(ZkStateReader.BASE_URL_PROP));

    Path instancePath = createTempDir();
    response = CollectionAdminRequest.addReplicaToShard(collectionName, "s1")
        .withProperty(CoreAdminParams.INSTANCE_DIR, instancePath.toString())
        .process(cluster.getSolrClient());
    newReplica = grabNewReplica(response, getCollectionState(collectionName));
    assertNotNull(newReplica);

    try (Http2SolrClient coreclient = SolrTestCaseJ4.getHttpSolrClient(newReplica.getStr(ZkStateReader.BASE_URL_PROP))) {
      CoreAdminResponse status = CoreAdminRequest.getStatus(newReplica.getName(), coreclient);
      NamedList<Object> coreStatus = status.getCoreStatus(newReplica.getName());
      String instanceDirStr = (String) coreStatus.get("instanceDir");
      assertEquals(instanceDirStr, instancePath.toString());
    }

    // Check that specifying property.name works. DO NOT remove this when the "name" property is deprecated
    // for ADDREPLICA, this is "property.name". See SOLR-7132
    response = CollectionAdminRequest.addReplicaToShard(collectionName, "s1")
        .withProperty(CoreAdminParams.NAME, "propertyDotName")
        .process(cluster.getSolrClient());

    newReplica = grabNewReplica(response, getCollectionState(collectionName));
    // nocommit do we really want to support this anymore?
    // assertEquals("'core' should be 'propertyDotName' " + newReplica.getName(), "propertyDotName", newReplica.getName());
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
}
