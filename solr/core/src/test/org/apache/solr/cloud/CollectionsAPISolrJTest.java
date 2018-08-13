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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.CoreStatus;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.V2Response;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.KeeperException;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_DEF;
import static org.apache.solr.common.cloud.ZkStateReader.NRT_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.NUM_SHARDS_PROP;

@LuceneTestCase.Slow
public class CollectionsAPISolrJTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(4)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  /**
   * When a config name is not specified during collection creation, the _default should
   * be used.
   */
  @Test
  public void testCreateWithDefaultConfigSet() throws Exception {
    String collectionName = "solrj_default_configset";
    CollectionAdminResponse response = CollectionAdminRequest.createCollection(collectionName, 2, 2)
        .process(cluster.getSolrClient());

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
    assertEquals(4, nodesStatus.size());

    waitForState("Expected " + collectionName + " to disappear from cluster state", collectionName, (n, c) -> c == null);
  }

  @Test
  public void testCreateCollWithDefaultClusterProperties() throws Exception {
    String COLL_NAME = "CollWithDefaultClusterProperties";
    try {
      V2Response rsp = new V2Request.Builder("/cluster")
          .withMethod(SolrRequest.METHOD.POST)
          .withPayload("{set-obj-property:{collectionDefaults:{numShards : 2 , nrtReplicas : 2}}}")
          .build()
          .process(cluster.getSolrClient());

      for (int i = 0; i < 10; i++) {
        Map m = cluster.getSolrClient().getZkStateReader().getClusterProperty(COLLECTION_DEF, null);
        if (m != null) break;
        Thread.sleep(10);
      }
      Object clusterProperty = cluster.getSolrClient().getZkStateReader().getClusterProperty(ImmutableList.of(COLLECTION_DEF, NUM_SHARDS_PROP), null);
      assertEquals("2", String.valueOf(clusterProperty));
      clusterProperty = cluster.getSolrClient().getZkStateReader().getClusterProperty(ImmutableList.of(COLLECTION_DEF, NRT_REPLICAS), null);
      assertEquals("2", String.valueOf(clusterProperty));
      CollectionAdminResponse response = CollectionAdminRequest
          .createCollection(COLL_NAME, "conf", null, null, null, null)
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
    } finally {
      V2Response rsp = new V2Request.Builder("/cluster")
          .withMethod(SolrRequest.METHOD.POST)
          .withPayload("{set-obj-property:{collectionDefaults: null}}")
          .build()
          .process(cluster.getSolrClient());

    }

  }

  @Test
  public void testCreateAndDeleteCollection() throws Exception {
    String collectionName = "solrj_test";
    CollectionAdminResponse response = CollectionAdminRequest.createCollection(collectionName, "conf", 2, 2)
        .setStateFormat(1)
        .process(cluster.getSolrClient());

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    Map<String, NamedList<Integer>> coresStatus = response.getCollectionCoresStatus();
    assertEquals(4, coresStatus.size());
    for (String coreName : coresStatus.keySet()) {
      NamedList<Integer> status = coresStatus.get(coreName);
      assertEquals(0, (int)status.get("status"));
      assertTrue(status.get("QTime") > 0);
    }

    response = CollectionAdminRequest.deleteCollection(collectionName).process(cluster.getSolrClient());

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    Map<String,NamedList<Integer>> nodesStatus = response.getCollectionNodesStatus();
    assertEquals(4, nodesStatus.size());

    waitForState("Expected " + collectionName + " to disappear from cluster state", collectionName, (n, c) -> c == null);

    // Test Creating a collection with new stateformat.
    collectionName = "solrj_newstateformat";

    response = CollectionAdminRequest.createCollection(collectionName, "conf", 2, 2)
        .setStateFormat(2)
        .process(cluster.getSolrClient());
    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());

    waitForState("Expected " + collectionName + " to appear in cluster state", collectionName, (n, c) -> c != null);

  }

  @Test
  public void testCloudInfoInCoreStatus() throws IOException, SolrServerException {
    String collectionName = "corestatus_test";
    CollectionAdminResponse response = CollectionAdminRequest.createCollection(collectionName, "conf", 2, 2)
        .setStateFormat(1)
        .process(cluster.getSolrClient());

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    String nodeName = ((NamedList) response.getResponse().get("success")).getName(0);
    String corename = (String) ((NamedList) ((NamedList) response.getResponse().get("success")).getVal(0)).get("core");

    try (HttpSolrClient coreclient = getHttpSolrClient(cluster.getSolrClient().getZkStateReader().getBaseUrlForNodeName(nodeName))) {
      CoreAdminResponse status = CoreAdminRequest.getStatus(corename, coreclient);
      Map m = status.getResponse().asMap(5);
      assertEquals(collectionName, Utils.getObjectByPath(m, true, Arrays.asList("status", corename, "cloud", "collection")));
      assertNotNull(Utils.getObjectByPath(m, true, Arrays.asList("status", corename, "cloud", "shard")));
      assertNotNull(Utils.getObjectByPath(m, true, Arrays.asList("status", corename, "cloud", "replica")));
    }
  }

  @Test
  public void testCreateAndDeleteShard() throws IOException, SolrServerException {
    // Create an implicit collection
    String collectionName = "solrj_implicit";
    CollectionAdminResponse response
        = CollectionAdminRequest.createCollectionWithImplicitRouter(collectionName, "conf", "shardA,shardB", 1, 1, 1)
        .setMaxShardsPerNode(3)
        .process(cluster.getSolrClient());

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    Map<String, NamedList<Integer>> coresStatus = response.getCollectionCoresStatus();
    assertEquals(6, coresStatus.size());

    // Add a shard to the implicit collection
    response = CollectionAdminRequest.createShard(collectionName, "shardC").process(cluster.getSolrClient());

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    coresStatus = response.getCollectionCoresStatus();
    assertEquals(3, coresStatus.size());
    int replicaTlog = 0;
    int replicaNrt = 0;
    int replicaPull = 0;
    for (String coreName : coresStatus.keySet()) {
      assertEquals(0, (int) coresStatus.get(coreName).get("status"));
      if (coreName.contains("shardC_replica_t")) replicaTlog++;
      else if (coreName.contains("shardC_replica_n")) replicaNrt++;
      else replicaPull++;
    }
    assertEquals(1, replicaNrt);
    assertEquals(1, replicaTlog);
    assertEquals(1, replicaPull);

    response = CollectionAdminRequest.deleteShard(collectionName, "shardC").process(cluster.getSolrClient());

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    Map<String, NamedList<Integer>> nodesStatus = response.getCollectionNodesStatus();
    assertEquals(1, nodesStatus.size());
  }

  @Test
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
        .setShardName("shard1")
        .process(cluster.getSolrClient());

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    Map<String, NamedList<Integer>> coresStatus = response.getCollectionCoresStatus();
    int shard10 = 0;
    int shard11 = 0;
    for (String coreName : coresStatus.keySet()) {
      assertEquals(0, (int) coresStatus.get(coreName).get("status"));
      if (coreName.contains("_shard1_0")) shard10++;
      else shard11++;
    }
    assertEquals(1, shard10);
    assertEquals(1, shard11);

    waitForState("Expected all shards to be active and parent shard to be removed", collectionName, (n, c) -> {
      if (c.getSlice("shard1").getState() == Slice.State.ACTIVE)
        return false;
      for (Replica r : c.getReplicas()) {
        if (r.isActive(n) == false)
          return false;
      }
      return true;
    });

    // Test splitting using split.key
    response = CollectionAdminRequest.splitShard(collectionName)
        .setSplitKey("b!")
        .process(cluster.getSolrClient());

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());

    waitForState("Expected 5 slices to be active", collectionName, (n, c) -> c.getActiveSlices().size() == 5);

  }

  @Test
  public void testCreateCollectionWithPropertyParam() throws Exception {

    String collectionName = "solrj_test_core_props";

    Path tmpDir = createTempDir("testPropertyParamsForCreate");
    Path dataDir = tmpDir.resolve("dataDir-" + TestUtil.randomSimpleString(random(), 1, 5));
    Path ulogDir = tmpDir.resolve("ulogDir-" + TestUtil.randomSimpleString(random(), 1, 5));

    CollectionAdminResponse response = CollectionAdminRequest.createCollection(collectionName, "conf", 1, 1)
        .withProperty(CoreAdminParams.DATA_DIR, dataDir.toString())
        .withProperty(CoreAdminParams.ULOG_DIR, ulogDir.toString())
        .process(cluster.getSolrClient());

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    Map<String, NamedList<Integer>> coresStatus = response.getCollectionCoresStatus();
    assertEquals(1, coresStatus.size());

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
        = new ArrayList<>(cluster.getSolrClient().getZkStateReader().getClusterState().getLiveNodes());
    Collections.shuffle(nodeList, random());
    final String node = nodeList.get(0);

    CollectionAdminResponse response = CollectionAdminRequest.addReplicaToShard(collectionName, "shard1")
        .setNode(node)
        .process(cluster.getSolrClient());
    Replica newReplica = grabNewReplica(response, getCollectionState(collectionName));
    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    assertTrue(newReplica.getNodeName().equals(node));

    // Test DELETEREPLICA
    response = CollectionAdminRequest.deleteReplica(collectionName, "shard1", newReplica.getName())
        .process(cluster.getSolrClient());
    assertEquals(0, response.getStatus());

    waitForState("Expected replica " + newReplica.getName() + " to vanish from cluster state", collectionName,
        (n, c) -> c.getSlice("shard1").getReplica(newReplica.getName()) == null);

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

  @Test
  public void testClusterProp() throws InterruptedException, IOException, SolrServerException {

    // sanity check our expected default
    final ClusterProperties props = new ClusterProperties(zkClient());
    assertEquals("Expecting prop to default to unset, test needs upated",
                 props.getClusterProperty(ZkStateReader.LEGACY_CLOUD, null), null);
    
    CollectionAdminResponse response = CollectionAdminRequest.setClusterProperty(ZkStateReader.LEGACY_CLOUD, "true")
      .process(cluster.getSolrClient());
    assertEquals(0, response.getStatus());
    assertEquals("Cluster property was not set", props.getClusterProperty(ZkStateReader.LEGACY_CLOUD, null), "true");

    // Unset ClusterProp that we set.
    CollectionAdminRequest.setClusterProperty(ZkStateReader.LEGACY_CLOUD, null).process(cluster.getSolrClient());
    assertEquals("Cluster property was not unset", props.getClusterProperty(ZkStateReader.LEGACY_CLOUD, null), null);

    response = CollectionAdminRequest.setClusterProperty(ZkStateReader.LEGACY_CLOUD, "false")
        .process(cluster.getSolrClient());
    assertEquals(0, response.getStatus());
    assertEquals("Cluster property was not set", props.getClusterProperty(ZkStateReader.LEGACY_CLOUD, null), "false");
  }

  @Test
  public void testCollectionProp() throws InterruptedException, IOException, SolrServerException {
    final String collectionName = "collectionPropTest";
    final String propName = "testProperty";

    CollectionAdminRequest.createCollection(collectionName, "conf", 2, 2)
        .process(cluster.getSolrClient());

    // Check for value change
    CollectionAdminRequest.setCollectionProperty(collectionName, propName, "false")
        .process(cluster.getSolrClient());
    checkCollectionProperty(collectionName, propName, "false", 3000);

    // Check for removing value
    CollectionAdminRequest.setCollectionProperty(collectionName, propName, null)
        .process(cluster.getSolrClient());
    checkCollectionProperty(collectionName, propName, null, 3000);
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
  public void testAddAndDeleteReplicaProp() throws InterruptedException, IOException, SolrServerException {

    final String collection = "replicaProperties";
    CollectionAdminRequest.createCollection(collection, "conf", 2, 2)
        .process(cluster.getSolrClient());

    final Replica replica = getCollectionState(collection).getLeader("shard1");
    CollectionAdminResponse response
        = CollectionAdminRequest.addReplicaProperty(collection, "shard1", replica.getName(), "preferredleader", "true")
        .process(cluster.getSolrClient());
    assertEquals(0, response.getStatus());

    waitForState("Expecting property 'preferredleader' to appear on replica " + replica.getName(), collection,
        (n, c) -> "true".equals(c.getReplica(replica.getName()).getProperty("preferredleader")));

    response = CollectionAdminRequest.deleteReplicaProperty(collection, "shard1", replica.getName(), "property.preferredleader")
        .process(cluster.getSolrClient());
    assertEquals(0, response.getStatus());

    waitForState("Expecting property 'preferredleader' to be removed from replica " + replica.getName(), collection,
        (n, c) -> c.getReplica(replica.getName()).getProperty("preferredleader") == null);

  }

  @Test
  public void testBalanceShardUnique() throws IOException,
      SolrServerException, KeeperException, InterruptedException {

    final String collection = "balancedProperties";
    CollectionAdminRequest.createCollection(collection, "conf", 2, 2)
        .process(cluster.getSolrClient());

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
        .setAttribute("replicationFactor", 25)
        .process(cluster.getSolrClient());

    waitForState("Expecting attribute 'replicationFactor' to be 25", collection,
        (n, c) -> 25 == c.getReplicationFactor());

    CollectionAdminRequest.modifyCollection(collection, null)
        .unsetAttribute("maxShardsPerNode")
        .process(cluster.getSolrClient());

    waitForState("Expecting attribute 'maxShardsPerNode' to be deleted", collection,
        (n, c) -> null == c.get("maxShardsPerNode"));

    expectThrows(IllegalArgumentException.class,
        "An attempt to set unknown collection attribute should have failed",
        () -> CollectionAdminRequest.modifyCollection(collection, null)
            .setAttribute("non_existent_attr", 25)
            .process(cluster.getSolrClient())
    );

    expectThrows(IllegalArgumentException.class,
        "An attempt to set null value should have failed",
        () -> CollectionAdminRequest.modifyCollection(collection, null)
            .setAttribute("non_existent_attr", null)
            .process(cluster.getSolrClient())
    );

    expectThrows(IllegalArgumentException.class,
        "An attempt to unset unknown collection attribute should have failed",
        () -> CollectionAdminRequest.modifyCollection(collection, null)
            .unsetAttribute("non_existent_attr")
            .process(cluster.getSolrClient())
    );
  }
}
