package org.apache.solr.cloud;

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


import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.NamedList;
import org.apache.zookeeper.KeeperException;
import org.junit.Before;

import static org.apache.solr.cloud.OverseerCollectionProcessor.SLICE_UNIQUE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class TestCollectionAPI extends AbstractFullDistribZkTestBase {

  public static final String COLLECTION_NAME = "testcollection";
  public static final String COLLECTION_NAME1 = "testcollection1";

  public TestCollectionAPI() {
    schemaString = "schema15.xml";      // we need a string id
  }

  @Override
  @Before
  public void setUp() throws Exception {
    fixShardCount = true;
    sliceCount = 2;
    shardCount = 2;
    super.setUp();
  }

  @Override
  public void doTest() throws Exception {
    CloudSolrServer client = createCloudClient(null);
    try {
      createCollection(null, COLLECTION_NAME, 2, 2, 2, client, null, "conf1");
      createCollection(null, COLLECTION_NAME1, 1, 1, 1, client, null, "conf1");
    } finally {
      //remove collections
      client.shutdown();
    }

    waitForCollection(cloudClient.getZkStateReader(), COLLECTION_NAME, 2);
    waitForCollection(cloudClient.getZkStateReader(), COLLECTION_NAME1, 1);
    waitForRecoveriesToFinish(COLLECTION_NAME, false);
    waitForRecoveriesToFinish(COLLECTION_NAME1, false);

    listCollection();
    clusterStatusNoCollection();
    clusterStatusWithCollection();
    clusterStatusWithCollectionAndShard();
    clusterStatusWithRouteKey();
    clusterStatusAliasTest();
    clusterStatusRolesTest();
    replicaPropTest();
  }

  private void clusterStatusWithCollectionAndShard() throws IOException, SolrServerException {
    CloudSolrServer client = createCloudClient(null);
    try {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("action", CollectionParams.CollectionAction.CLUSTERSTATUS.toString());
      params.set("collection", COLLECTION_NAME);
      params.set("shard", SHARD1);
      SolrRequest request = new QueryRequest(params);
      request.setPath("/admin/collections");

      NamedList<Object> rsp = client.request(request);
      NamedList<Object> cluster = (NamedList<Object>) rsp.get("cluster");
      assertNotNull("Cluster state should not be null", cluster);
      NamedList<Object> collections = (NamedList<Object>) cluster.get("collections");
      assertNotNull("Collections should not be null in cluster state", collections);
      assertNotNull(collections.get(COLLECTION_NAME));
      assertEquals(1, collections.size());
      Map<String, Object> collection = (Map<String, Object>) collections.get(COLLECTION_NAME);
      Map<String, Object> shardStatus = (Map<String, Object>) collection.get("shards");
      assertEquals(1, shardStatus.size());
      Map<String, Object> selectedShardStatus = (Map<String, Object>) shardStatus.get(SHARD1);
      assertNotNull(selectedShardStatus);

    } finally {
      //remove collections
      client.shutdown();
    }
  }


  private void listCollection() throws IOException, SolrServerException {
    CloudSolrServer client = createCloudClient(null);
    try {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("action", CollectionParams.CollectionAction.LIST.toString());
      SolrRequest request = new QueryRequest(params);
      request.setPath("/admin/collections");

      NamedList<Object> rsp = client.request(request);
      List<String> collections = (List<String>) rsp.get("collections");
      assertTrue("control_collection was not found in list", collections.contains("control_collection"));
      assertTrue(DEFAULT_COLLECTION + " was not found in list", collections.contains(DEFAULT_COLLECTION));
      assertTrue(COLLECTION_NAME + " was not found in list", collections.contains(COLLECTION_NAME));
      assertTrue(COLLECTION_NAME1 + " was not found in list", collections.contains(COLLECTION_NAME1));
    } finally {
      //remove collections
      client.shutdown();
    }


  }

  private void clusterStatusNoCollection() throws Exception {
    CloudSolrServer client = createCloudClient(null);
    try {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("action", CollectionParams.CollectionAction.CLUSTERSTATUS.toString());
      SolrRequest request = new QueryRequest(params);
      request.setPath("/admin/collections");

      NamedList<Object> rsp = client.request(request);
      NamedList<Object> cluster = (NamedList<Object>) rsp.get("cluster");
      assertNotNull("Cluster state should not be null", cluster);
      NamedList<Object> collections = (NamedList<Object>) cluster.get("collections");
      assertNotNull("Collections should not be null in cluster state", collections);
      assertNotNull(collections.get(COLLECTION_NAME1));
      assertEquals(4, collections.size());

      List<String> liveNodes = (List<String>) cluster.get("live_nodes");
      assertNotNull("Live nodes should not be null", liveNodes);
      assertFalse(liveNodes.isEmpty());
    } finally {
      //remove collections
      client.shutdown();
    }

  }

  private void clusterStatusWithCollection() throws IOException, SolrServerException {
    CloudSolrServer client = createCloudClient(null);
    try {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("action", CollectionParams.CollectionAction.CLUSTERSTATUS.toString());
      params.set("collection", COLLECTION_NAME);
      SolrRequest request = new QueryRequest(params);
      request.setPath("/admin/collections");

      NamedList<Object> rsp = client.request(request);
      NamedList<Object> cluster = (NamedList<Object>) rsp.get("cluster");
      assertNotNull("Cluster state should not be null", cluster);
      NamedList<Object> collections = (NamedList<Object>) cluster.get("collections");
      assertNotNull("Collections should not be null in cluster state", collections);
      assertNotNull(collections.get(COLLECTION_NAME));
      assertEquals(1, collections.size());
    } finally {
      //remove collections
      client.shutdown();
    }
  }

  private void clusterStatusWithRouteKey() throws IOException, SolrServerException {
    CloudSolrServer client = createCloudClient(DEFAULT_COLLECTION);
    try {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "a!123"); // goes to shard2. see ShardRoutingTest for details
      client.add(doc);
      client.commit();

      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("action", CollectionParams.CollectionAction.CLUSTERSTATUS.toString());
      params.set("collection", DEFAULT_COLLECTION);
      params.set(ShardParams._ROUTE_, "a!");
      SolrRequest request = new QueryRequest(params);
      request.setPath("/admin/collections");

      NamedList<Object> rsp = client.request(request);
      NamedList<Object> cluster = (NamedList<Object>) rsp.get("cluster");
      assertNotNull("Cluster state should not be null", cluster);
      NamedList<Object> collections = (NamedList<Object>) cluster.get("collections");
      assertNotNull("Collections should not be null in cluster state", collections);
      assertNotNull(collections.get(DEFAULT_COLLECTION));
      assertEquals(1, collections.size());
      Map<String, Object> collection = (Map<String, Object>) collections.get(DEFAULT_COLLECTION);
      Map<String, Object> shardStatus = (Map<String, Object>) collection.get("shards");
      assertEquals(1, shardStatus.size());
      Map<String, Object> selectedShardStatus = (Map<String, Object>) shardStatus.get(SHARD2);
      assertNotNull(selectedShardStatus);
    } finally {
      //remove collections
      client.shutdown();
    }
  }

  private void clusterStatusAliasTest() throws Exception  {
    CloudSolrServer client = createCloudClient(null);
    try {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("action", CollectionParams.CollectionAction.CREATEALIAS.toString());
      params.set("name", "myalias");
      params.set("collections", DEFAULT_COLLECTION + "," + COLLECTION_NAME);
      SolrRequest request = new QueryRequest(params);
      request.setPath("/admin/collections");
      client.request(request);
      params = new ModifiableSolrParams();
      params.set("action", CollectionParams.CollectionAction.CLUSTERSTATUS.toString());
      params.set("collection", DEFAULT_COLLECTION);
      request = new QueryRequest(params);
      request.setPath("/admin/collections");

      NamedList<Object> rsp = client.request(request);


      NamedList<Object> cluster = (NamedList<Object>) rsp.get("cluster");
      assertNotNull("Cluster state should not be null", cluster);
      Map<String, String> aliases = (Map<String, String>) cluster.get("aliases");
      assertNotNull("Aliases should not be null", aliases);
      assertEquals("Alias: myalias not found in cluster status",
          DEFAULT_COLLECTION + "," + COLLECTION_NAME, aliases.get("myalias"));

      NamedList<Object> collections = (NamedList<Object>) cluster.get("collections");
      assertNotNull("Collections should not be null in cluster state", collections);
      assertNotNull(collections.get(DEFAULT_COLLECTION));
      Map<String, Object> collection = (Map<String, Object>) collections.get(DEFAULT_COLLECTION);
      List<String> collAlias = (List<String>) collection.get("aliases");
      assertEquals("Aliases not found", Lists.newArrayList("myalias"), collAlias);
    } finally {
      client.shutdown();
    }
  }

  private void clusterStatusRolesTest() throws Exception  {
    CloudSolrServer client = createCloudClient(null);
    try {
      client.connect();
      Replica replica = client.getZkStateReader().getLeaderRetry(DEFAULT_COLLECTION, SHARD1);

      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("action", CollectionParams.CollectionAction.ADDROLE.toString());
      params.set("node", replica.getNodeName());
      params.set("role", "overseer");
      SolrRequest request = new QueryRequest(params);
      request.setPath("/admin/collections");
      client.request(request);

      params = new ModifiableSolrParams();
      params.set("action", CollectionParams.CollectionAction.CLUSTERSTATUS.toString());
      params.set("collection", DEFAULT_COLLECTION);
      request = new QueryRequest(params);
      request.setPath("/admin/collections");

      NamedList<Object> rsp = client.request(request);
      NamedList<Object> cluster = (NamedList<Object>) rsp.get("cluster");
      assertNotNull("Cluster state should not be null", cluster);
      Map<String, Object> roles = (Map<String, Object>) cluster.get("roles");
      assertNotNull("Role information should not be null", roles);
      List<String> overseer = (List<String>) roles.get("overseer");
      assertNotNull(overseer);
      assertEquals(1, overseer.size());
      assertTrue(overseer.contains(replica.getNodeName()));
    } finally {
      client.shutdown();
    }
  }

  private void replicaPropTest() throws Exception {
    CloudSolrServer client = createCloudClient(null);
    try {
      client.connect();
      Map<String, Slice> slices = client.getZkStateReader().getClusterState().getCollection(COLLECTION_NAME).getSlicesMap();
      List<String> sliceList = new ArrayList<>(slices.keySet());
      String c1_s1 = sliceList.get(0);
      List<String> replicasList = new ArrayList<>(slices.get(c1_s1).getReplicasMap().keySet());
      String c1_s1_r1 = replicasList.get(0);
      String c1_s1_r2 = replicasList.get(1);

      String c1_s2 = sliceList.get(1);
      replicasList = new ArrayList<>(slices.get(c1_s2).getReplicasMap().keySet());
      String c1_s2_r1 = replicasList.get(0);
      String c1_s2_r2 = replicasList.get(1);


      slices = client.getZkStateReader().getClusterState().getCollection(COLLECTION_NAME1).getSlicesMap();
      sliceList = new ArrayList<>(slices.keySet());
      String c2_s1 = sliceList.get(0);
      replicasList = new ArrayList<>(slices.get(c2_s1).getReplicasMap().keySet());
      String c2_s1_r1 = replicasList.get(0);

      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("action", CollectionParams.CollectionAction.ADDREPLICAPROP.toString());

      // Insure we get error returns when omitting required parameters

      missingParamsError(client, params);
      params.set("collection", COLLECTION_NAME);
      missingParamsError(client, params);
      params.set("shard", c1_s1);
      missingParamsError(client, params);
      params.set("replica", c1_s1_r1);
      missingParamsError(client, params);
      params.set("property", "preferredLeader");
      missingParamsError(client, params);
      params.set("property.value", "true");

      SolrRequest request = new QueryRequest(params);
      request.setPath("/admin/collections");
      client.request(request);

      // The above should have set exactly one preferredleader...
      verifyPropertyVal(client, COLLECTION_NAME, c1_s1_r1, "preferredleader", "true");
      verifyPropertyNotPresent(client, COLLECTION_NAME, c1_s1_r2, "preferredLeader");
      verifyPropertyNotPresent(client, COLLECTION_NAME, c1_s2_r1, "preferredLeader");
      verifyPropertyNotPresent(client, COLLECTION_NAME, c1_s2_r2, "preferredLeader");

      doPropertyAction(client,
          "action", CollectionParams.CollectionAction.ADDREPLICAPROP.toString(),
          "collection", COLLECTION_NAME,
          "shard", c1_s1,
          "replica", c1_s1_r2,
          "property", "preferredLeader",
          "property.value", "true");
      // The preferred leader property for shard1 should have switched to the other replica.
      verifyPropertyVal(client, COLLECTION_NAME, c1_s1_r2, "preferredleader", "true");
      verifyPropertyNotPresent(client, COLLECTION_NAME, c1_s1_r1, "preferredLeader");
      verifyPropertyNotPresent(client, COLLECTION_NAME, c1_s2_r1, "preferredLeader");
      verifyPropertyNotPresent(client, COLLECTION_NAME, c1_s2_r2, "preferredLeader");

      doPropertyAction(client,
          "action", CollectionParams.CollectionAction.ADDREPLICAPROP.toString(),
          "collection", COLLECTION_NAME,
          "shard", c1_s2,
          "replica", c1_s2_r1,
          "property", "preferredLeader",
          "property.value", "true");

      // Now we should have a preferred leader in both shards...
      verifyPropertyVal(client, COLLECTION_NAME, c1_s1_r2, "preferredleader", "true");
      verifyPropertyNotPresent(client, COLLECTION_NAME, c1_s1_r1, "preferredleader");
      verifyPropertyVal(client, COLLECTION_NAME, c1_s2_r1, "preferredleader", "true");
      verifyPropertyNotPresent(client, COLLECTION_NAME, c1_s2_r2, "preferredLeader");

      doPropertyAction(client,
          "action", CollectionParams.CollectionAction.ADDREPLICAPROP.toString(),
          "collection", COLLECTION_NAME1,
          "shard", c2_s1,
          "replica", c2_s1_r1,
          "property", "preferredLeader",
          "property.value", "true");

      // Now we should have three preferred leaders.
      verifyPropertyVal(client, COLLECTION_NAME, c1_s1_r2, "preferredleader", "true");
      verifyPropertyVal(client, COLLECTION_NAME, c1_s2_r1, "preferredleader", "true");
      verifyPropertyVal(client, COLLECTION_NAME1, c2_s1_r1, "preferredleader", "true");

      doPropertyAction(client,
          "action", CollectionParams.CollectionAction.DELETEREPLICAPROP.toString(),
          "collection", COLLECTION_NAME1,
          "shard", c2_s1,
          "replica", c2_s1_r1,
          "property", "preferredLeader");

      // Now we should have two preferred leaders.
      // But first we have to wait for the overseer to finish the action
      verifyPropertyVal(client, COLLECTION_NAME, c1_s1_r2, "preferredleader", "true");
      verifyPropertyVal(client, COLLECTION_NAME, c1_s2_r1, "preferredleader", "true");
      verifyPropertyNotPresent(client, COLLECTION_NAME, c1_s1_r1, "preferredleader");
      verifyPropertyNotPresent(client, COLLECTION_NAME, c1_s2_r2, "preferredleader");
      verifyPropertyNotPresent(client, COLLECTION_NAME1, c2_s1_r1, "preferredleader");

      // Try adding an arbitrary property to one that has the leader property
      doPropertyAction(client,
          "action", CollectionParams.CollectionAction.ADDREPLICAPROP.toString(),
          "collection", COLLECTION_NAME,
          "shard", c1_s1,
          "replica", c1_s1_r1,
          "property", "testprop",
          "property.value", "true");

      verifyPropertyVal(client, COLLECTION_NAME, c1_s1_r2, "preferredleader", "true");
      verifyPropertyVal(client, COLLECTION_NAME, c1_s2_r1, "preferredleader", "true");
      verifyPropertyVal(client, COLLECTION_NAME, c1_s1_r1, "testprop", "true");
      verifyPropertyNotPresent(client, COLLECTION_NAME, c1_s1_r1, "preferredleader");
      verifyPropertyNotPresent(client, COLLECTION_NAME, c1_s2_r2, "preferredleader");
      verifyPropertyNotPresent(client, COLLECTION_NAME1, c2_s1_r1, "preferredleader");
      verifyPropertyNotPresent(client, COLLECTION_NAME1, c2_s1_r1, "preferredleader");

      doPropertyAction(client,
          "action", CollectionParams.CollectionAction.ADDREPLICAPROP.toString(),
          "collection", COLLECTION_NAME,
          "shard", c1_s1,
          "replica", c1_s1_r2,
          "property", "prop",
          "property.value", "silly");

      verifyPropertyVal(client, COLLECTION_NAME, c1_s1_r2, "preferredleader", "true");
      verifyPropertyVal(client, COLLECTION_NAME, c1_s2_r1, "preferredleader", "true");
      verifyPropertyVal(client, COLLECTION_NAME, c1_s1_r1, "testprop", "true");
      verifyPropertyVal(client, COLLECTION_NAME, c1_s1_r2, "prop", "silly");
      verifyPropertyNotPresent(client, COLLECTION_NAME, c1_s1_r1, "preferredleader");
      verifyPropertyNotPresent(client, COLLECTION_NAME, c1_s2_r2, "preferredleader");
      verifyPropertyNotPresent(client, COLLECTION_NAME1, c2_s1_r1, "preferredleader");
      verifyPropertyNotPresent(client, COLLECTION_NAME1, c2_s1_r1, "preferredleader");

      doPropertyAction(client,
          "action", CollectionParams.CollectionAction.ADDREPLICAPROP.toLower(),
          "collection", COLLECTION_NAME,
          "shard", c1_s1,
          "replica", c1_s1_r1,
          "property", "testprop",
          "property.value", "nonsense",
          SLICE_UNIQUE, "true");

      verifyPropertyVal(client, COLLECTION_NAME, c1_s1_r2, "preferredleader", "true");
      verifyPropertyVal(client, COLLECTION_NAME, c1_s2_r1, "preferredleader", "true");
      verifyPropertyVal(client, COLLECTION_NAME, c1_s1_r1, "testprop", "nonsense");
      verifyPropertyVal(client, COLLECTION_NAME, c1_s1_r2, "prop", "silly");
      verifyPropertyNotPresent(client, COLLECTION_NAME, c1_s1_r1, "preferredleader");
      verifyPropertyNotPresent(client, COLLECTION_NAME, c1_s2_r2, "preferredleader");
      verifyPropertyNotPresent(client, COLLECTION_NAME1, c2_s1_r1, "preferredleader");
      verifyPropertyNotPresent(client, COLLECTION_NAME1, c2_s1_r1, "preferredleader");


      doPropertyAction(client,
          "action", CollectionParams.CollectionAction.ADDREPLICAPROP.toLower(),
          "collection", COLLECTION_NAME,
          "shard", c1_s1,
          "replica", c1_s1_r1,
          "property", "testprop",
          "property.value", "true",
          SLICE_UNIQUE, "false");

      verifyPropertyVal(client, COLLECTION_NAME, c1_s1_r2, "preferredleader", "true");
      verifyPropertyVal(client, COLLECTION_NAME, c1_s2_r1, "preferredleader", "true");
      verifyPropertyVal(client, COLLECTION_NAME, c1_s1_r1, "testprop", "true");
      verifyPropertyVal(client, COLLECTION_NAME, c1_s1_r2, "prop", "silly");
      verifyPropertyNotPresent(client, COLLECTION_NAME, c1_s1_r1, "preferredleader");
      verifyPropertyNotPresent(client, COLLECTION_NAME, c1_s2_r2, "preferredleader");
      verifyPropertyNotPresent(client, COLLECTION_NAME1, c2_s1_r1, "preferredleader");
      verifyPropertyNotPresent(client, COLLECTION_NAME1, c2_s1_r1, "preferredleader");

      doPropertyAction(client,
          "action", CollectionParams.CollectionAction.DELETEREPLICAPROP.toLower(),
          "collection", COLLECTION_NAME,
          "shard", c1_s1,
          "replica", c1_s1_r1,
          "property", "testprop");

      verifyPropertyVal(client, COLLECTION_NAME, c1_s1_r2, "preferredleader", "true");
      verifyPropertyVal(client, COLLECTION_NAME, c1_s2_r1, "preferredleader", "true");
      verifyPropertyNotPresent(client, COLLECTION_NAME, c1_s1_r1, "testprop");
      verifyPropertyVal(client, COLLECTION_NAME, c1_s1_r2, "prop", "silly");
      verifyPropertyNotPresent(client, COLLECTION_NAME, c1_s1_r1, "preferredleader");
      verifyPropertyNotPresent(client, COLLECTION_NAME, c1_s2_r2, "preferredleader");
      verifyPropertyNotPresent(client, COLLECTION_NAME1, c2_s1_r1, "preferredleader");
      verifyPropertyNotPresent(client, COLLECTION_NAME1, c2_s1_r1, "preferredleader");

      try {
        doPropertyAction(client,
            "action", CollectionParams.CollectionAction.ADDREPLICAPROP.toString(),
            "collection", COLLECTION_NAME,
            "shard", c1_s1,
            "replica", c1_s1_r1,
            "property", "preferredLeader",
            "property.value", "true",
            SLICE_UNIQUE, "false");
        fail("Should have thrown an exception, setting sliceUnique=false is not allowed for 'preferredLeader'.");
      } catch (SolrException se) {
        assertTrue("Should have received a specific error message",
            se.getMessage().contains("with the sliceUnique parameter set to something other than 'true'"));
      }

      verifyPropertyVal(client, COLLECTION_NAME, c1_s1_r2, "preferredleader", "true");
      verifyPropertyVal(client, COLLECTION_NAME, c1_s2_r1, "preferredleader", "true");
      verifyPropertyNotPresent(client, COLLECTION_NAME, c1_s1_r1, "testprop");
      verifyPropertyVal(client, COLLECTION_NAME, c1_s1_r2, "prop", "silly");
      verifyPropertyNotPresent(client, COLLECTION_NAME, c1_s1_r1, "preferredleader");
      verifyPropertyNotPresent(client, COLLECTION_NAME, c1_s2_r2, "preferredleader");
      verifyPropertyNotPresent(client, COLLECTION_NAME1, c2_s1_r1, "preferredleader");
      verifyPropertyNotPresent(client, COLLECTION_NAME1, c2_s1_r1, "preferredleader");

    } finally {
      client.shutdown();
    }
  }

  private void doPropertyAction(CloudSolrServer client, String... paramsIn) throws IOException, SolrServerException {
    assertTrue("paramsIn must be an even multiple of 2, it is: " + paramsIn.length, (paramsIn.length % 2) == 0);
    ModifiableSolrParams params = new ModifiableSolrParams();
    for (int idx = 0; idx < paramsIn.length; idx += 2) {
      params.set(paramsIn[idx], paramsIn[idx + 1]);
    }
    QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    client.request(request);

  }

  private void verifyPropertyNotPresent(CloudSolrServer client, String collectionName, String replicaName,
                                        String property)
      throws KeeperException, InterruptedException {
    ClusterState clusterState = null;
    Replica replica = null;
    for (int idx = 0; idx < 300; ++idx) {
      client.getZkStateReader().updateClusterState(true);
      clusterState = client.getZkStateReader().getClusterState();
      replica = clusterState.getReplica(collectionName, replicaName);
      if (replica == null) {
        fail("Could not find collection/replica pair! " + collectionName + "/" + replicaName);
      }
      if (StringUtils.isBlank(replica.getStr(property))) return;
      Thread.sleep(100);
    }
    fail("Property " + property + " not set correctly for collection/replica pair: " +
        collectionName + "/" + replicaName + ". Replica props: " + replica.getProperties().toString() +
        ". Cluster state is " + clusterState.toString());

  }

  // The params are triplets,
  // collection
  // shard
  // replica
  private void verifyPropertyVal(CloudSolrServer client, String collectionName,
                                 String replicaName, String property, String val)
      throws InterruptedException, KeeperException {
    Replica replica = null;
    ClusterState clusterState = null;

    for (int idx = 0; idx < 300; ++idx) { // Keep trying while Overseer writes the ZK state for up to 30 seconds.
      client.getZkStateReader().updateClusterState(true);
      clusterState = client.getZkStateReader().getClusterState();
      replica = clusterState.getReplica(collectionName, replicaName);
      if (replica == null) {
        fail("Could not find collection/replica pair! " + collectionName + "/" + replicaName);
      }
      if (StringUtils.equals(val, replica.getStr(property))) return;
      Thread.sleep(100);
    }

    fail("Property '" + property + "' with value " + replica.getStr(property) +
        " not set correctly for collection/replica pair: " + collectionName + "/" + replicaName + " property map is " +
    replica.getProperties().toString() + ".");

  }

  private void missingParamsError(CloudSolrServer client, ModifiableSolrParams origParams)
      throws IOException, SolrServerException {

    SolrRequest request;
    try {
      request = new QueryRequest(origParams);
      request.setPath("/admin/collections");
      client.request(request);
      fail("Should have thrown a SolrException due to lack of a required parameter.");
    } catch (SolrException se) {
      assertTrue("Should have gotten a specific message back mentioning 'missing required parameter'. Got: " + se.getMessage(),
          se.getMessage().toLowerCase(Locale.ROOT).contains("missing required parameter:"));
    }
  }
}
