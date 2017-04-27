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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.CoreStatus;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.KeeperException;
import org.junit.BeforeClass;
import org.junit.Test;

@LuceneTestCase.Slow
public class CollectionsAPISolrJTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(4)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
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
    for (int i=0; i<4; i++) {
      NamedList<Integer> status = coresStatus.get(collectionName + "_shard" + (i/2+1) + "_replica" + (i%2+1));
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
        = CollectionAdminRequest.createCollectionWithImplicitRouter(collectionName, "conf", "shardA,shardB", 1)
        .process(cluster.getSolrClient());

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    Map<String, NamedList<Integer>> coresStatus = response.getCollectionCoresStatus();
    assertEquals(2, coresStatus.size());

    // Add a shard to the implicit collection
    response = CollectionAdminRequest.createShard(collectionName, "shardC").process(cluster.getSolrClient());

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    coresStatus = response.getCollectionCoresStatus();
    assertEquals(1, coresStatus.size());
    assertEquals(0, (int) coresStatus.get(collectionName + "_shardC_replica1").get("status"));

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
    assertEquals(0, (int) coresStatus.get(collectionName + "_shard1_0_replica1").get("status"));
    assertEquals(0, (int) coresStatus.get(collectionName + "_shard1_1_replica1").get("status"));

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

    Replica replica1 = testCollection.getReplica("core_node1");
    CoreStatus coreStatus = getCoreStatus(replica1);

    assertEquals(Paths.get(coreStatus.getDataDirectory()).toString(), dataDir.toString());

  }

  @Test
  public void testAddAndDeleteReplica() throws Exception {

    final String collectionName = "solrj_replicatests";
    CollectionAdminRequest.createCollection(collectionName, "conf", 1, 2)
        .process(cluster.getSolrClient());

    String newReplicaName = Assign.assignNode(getCollectionState(collectionName));
    ArrayList<String> nodeList
        = new ArrayList<>(cluster.getSolrClient().getZkStateReader().getClusterState().getLiveNodes());
    Collections.shuffle(nodeList, random());
    final String node = nodeList.get(0);

    CollectionAdminResponse response = CollectionAdminRequest.addReplicaToShard(collectionName, "shard1")
        .setNode(node)
        .process(cluster.getSolrClient());

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());

    waitForState("Expected to see replica " + newReplicaName + " on node " + node, collectionName, (n, c) -> {
      Replica r = c.getSlice("shard1").getReplica(newReplicaName);
      return r != null && r.getNodeName().equals(node);
    });

    // Test DELETEREPLICA
    response = CollectionAdminRequest.deleteReplica(collectionName, "shard1", newReplicaName)
        .process(cluster.getSolrClient());
    assertEquals(0, response.getStatus());

    waitForState("Expected replica " + newReplicaName + " to vanish from cluster state", collectionName,
        (n, c) -> c.getSlice("shard1").getReplica(newReplicaName) == null);

  }

  @Test
  public void testClusterProp() throws InterruptedException, IOException, SolrServerException {

    CollectionAdminResponse response = CollectionAdminRequest.setClusterProperty(ZkStateReader.LEGACY_CLOUD, "false")
        .process(cluster.getSolrClient());

    assertEquals(0, response.getStatus());

    ClusterProperties props = new ClusterProperties(zkClient());
    assertEquals("Cluster property was not set", props.getClusterProperty(ZkStateReader.LEGACY_CLOUD, "true"), "false");

    // Unset ClusterProp that we set.
    CollectionAdminRequest.setClusterProperty(ZkStateReader.LEGACY_CLOUD, null).process(cluster.getSolrClient());
    assertEquals("Cluster property was not unset", props.getClusterProperty(ZkStateReader.LEGACY_CLOUD, "true"), "true");

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
        (n, c) -> "true".equals(c.getReplica(replica.getName()).getStr("property.preferredleader")));

    response = CollectionAdminRequest.deleteReplicaProperty(collection, "shard1", replica.getName(), "property.preferredleader")
        .process(cluster.getSolrClient());
    assertEquals(0, response.getStatus());

    waitForState("Expecting property 'preferredleader' to be removed from replica " + replica.getName(), collection,
        (n, c) -> c.getReplica(replica.getName()).getStr("property.preferredleader") == null);

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
          if ("true".equals(replica.getStr("property.preferredleader")))
            count += 1;
        }
        if (count != 1)
          return false;
      }
      return true;
    });

  }
}
