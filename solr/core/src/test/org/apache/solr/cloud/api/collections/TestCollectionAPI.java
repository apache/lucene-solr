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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.Lists;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.cloud.ZkTestServer;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.NamedList;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;

public class TestCollectionAPI extends ReplicaPropertiesBase {

  public static final String COLLECTION_NAME = "testcollection";
  public static final String COLLECTION_NAME1 = "testcollection1";

  public TestCollectionAPI() {
    schemaString = "schema15.xml";      // we need a string id
    sliceCount = 2;
  }

  @Test
  @ShardsFixed(num = 2)
  public void test() throws Exception {
    try (CloudSolrClient client = createCloudClient(null)) {
      CollectionAdminRequest.Create req;
      if (useTlogReplicas()) {
        req = CollectionAdminRequest.createCollection(COLLECTION_NAME, "conf1",2, 0, 1, 1);
      } else {
        req = CollectionAdminRequest.createCollection(COLLECTION_NAME, "conf1",2, 1, 0, 1);
      }
      client.request(req);
      createCollection(null, COLLECTION_NAME1, 1, 1, client, null, "conf1");
    }

    waitForCollection(cloudClient.getZkStateReader(), COLLECTION_NAME, 2);
    waitForCollection(cloudClient.getZkStateReader(), COLLECTION_NAME1, 1);
    waitForRecoveriesToFinish(COLLECTION_NAME, false);
    waitForRecoveriesToFinish(COLLECTION_NAME1, false);

    listCollection();
    clusterStatusNoCollection();
    clusterStatusWithCollection();
    clusterStatusWithCollectionAndShard();
    clusterStatusWithCollectionAndMultipleShards();
    clusterStatusWithRouteKey();
    clusterStatusAliasTest();
    clusterStatusRolesTest();
    clusterStatusBadCollectionTest();
    replicaPropTest();
    clusterStatusZNodeVersion();
    testCollectionCreationCollectionNameValidation();
    testReplicationFactorValidaton();
    testCollectionCreationShardNameValidation();
    testAliasCreationNameValidation();
    testShardCreationNameValidation();
    testNoConfigset();
    testModifyCollection(); // deletes replicationFactor property from collections, be careful adding new tests after this one!
  }

  private void assertMissingCollection(CloudSolrClient client, String collectionName) throws Exception {
    ClusterState clusterState = client.getZkStateReader().getClusterState();
    assertNull(clusterState.getCollectionOrNull(collectionName));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private void testModifyCollection() throws Exception {
    try (CloudSolrClient client = createCloudClient(null)) {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("action", CollectionParams.CollectionAction.MODIFYCOLLECTION.toString());
      params.set("collection", COLLECTION_NAME);
      params.set("replicationFactor", 25);
      @SuppressWarnings({"rawtypes"})
      SolrRequest request = new QueryRequest(params);
      request.setPath("/admin/collections");

      client.request(request);
      NamedList<Object> rsp = CollectionAdminRequest.getClusterStatus().setCollectionName(COLLECTION_NAME)
          .process(client).getResponse();
      NamedList<Object> cluster = (NamedList<Object>) rsp.get("cluster");
      assertNotNull("Cluster state should not be null", cluster);
      NamedList<Object> collections = (NamedList<Object>) cluster.get("collections");
      assertNotNull("Collections should not be null in cluster state", collections);
      assertEquals(1, collections.size());
      Map<String, Object> collection = (Map<String, Object>) collections.get(COLLECTION_NAME);
      int replicationFactor = Integer.parseInt(collection.get("replicationFactor").toString());
      assertEquals(25, replicationFactor);

      params = new ModifiableSolrParams();
      params.set("action", CollectionParams.CollectionAction.MODIFYCOLLECTION.toString());
      params.set("collection", COLLECTION_NAME);
      params.set("replicationFactor", "");
      request = new QueryRequest(params);
      request.setPath("/admin/collections");

      client.request(request);

      rsp = CollectionAdminRequest.getClusterStatus().setCollectionName(COLLECTION_NAME)
          .process(client).getResponse();
      System.out.println(rsp);
      cluster = (NamedList<Object>) rsp.get("cluster");
      assertNotNull("Cluster state should not be null", cluster);
      collections = (NamedList<Object>) cluster.get("collections");
      assertNotNull("Collections should not be null in cluster state", collections);
      assertEquals(1, collections.size());
      collection = (Map<String, Object>) collections.get(COLLECTION_NAME);
      assertNull(collection.get("replicationFactor"));

      params = new ModifiableSolrParams();
      params.set("action", CollectionParams.CollectionAction.MODIFYCOLLECTION.toString());
      params.set("collection", COLLECTION_NAME);
      params.set("non_existent_property", "");
      request = new QueryRequest(params);
      request.setPath("/admin/collections");

      try {
        client.request(request);
        fail("Trying to unset an unknown property should have failed");
      } catch (BaseHttpSolrClient.RemoteSolrException e) {
        // expected
        assertTrue(e.getMessage().contains("no supported values provided"));
      }
    }
  }

  private void testReplicationFactorValidaton() throws Exception {
    try (CloudSolrClient client = createCloudClient(null)) {
      //Test that you can't specify both replicationFactor and nrtReplicas
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("action", CollectionParams.CollectionAction.CREATE.toString());
      params.set("name", "test_repFactorColl");
      params.set("numShards", "1");
      params.set("replicationFactor", "1");
      params.set("nrtReplicas", "2");
      @SuppressWarnings({"rawtypes"})
      SolrRequest request = new QueryRequest(params);
      request.setPath("/admin/collections");

      try {
        client.request(request);
        fail();
      } catch (BaseHttpSolrClient.RemoteSolrException e) {
        final String errorMessage = e.getMessage();
        assertTrue(errorMessage.contains("Cannot specify both replicationFactor and nrtReplicas as they mean the same thing"));
      }

      //Create it again correctly
      CollectionAdminRequest.Create req = CollectionAdminRequest.createCollection("test_repFactorColl", "conf1", 1, 3, 0, 0);
      client.request(req);

      waitForCollection(cloudClient.getZkStateReader(), "test_repFactorColl", 1);
      waitForRecoveriesToFinish("test_repFactorColl", false);

      //Assert that replicationFactor has also been set to 3
      assertCountsForRepFactorAndNrtReplicas(client, "test_repFactorColl");

      params = new ModifiableSolrParams();
      params.set("action", CollectionParams.CollectionAction.MODIFYCOLLECTION.toString());
      params.set("collection", "test_repFactorColl");
      params.set("replicationFactor", "4");
      request = new QueryRequest(params);
      request.setPath("/admin/collections");
      client.request(request);

      assertCountsForRepFactorAndNrtReplicas(client, "test_repFactorColl");
    }
  }

  // See  SOLR-12013. We should report something back if the configset has mysteriously disappeared.
  private void testNoConfigset() throws Exception {
    String configSet = "delete_config";

    final String collection = "deleted_collection";
    try (CloudSolrClient client = createCloudClient(null)) {
      copyConfigUp(TEST_PATH().resolve("configsets"), "cloud-minimal", configSet, client.getZkHost());

      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("action", CollectionParams.CollectionAction.CREATE.toString());
      params.set("name", collection);
      params.set("numShards", "1");
      params.set("replicationFactor", "1");
      params.set("collection.configName", configSet);
      @SuppressWarnings({"rawtypes"})
      SolrRequest request = new QueryRequest(params);
      request.setPath("/admin/collections");

      client.request(request);

      waitForCollection(cloudClient.getZkStateReader(), collection, 1);
      waitForRecoveriesToFinish(collection, false);

      // Now try deleting the configset and doing a clusterstatus.
      String parent = ZkConfigManager.CONFIGS_ZKNODE + "/" + configSet;
      deleteThemAll(client.getZkStateReader().getZkClient(), parent);
      client.getZkStateReader().forciblyRefreshAllClusterStateSlow();

      final CollectionAdminRequest.ClusterStatus req = CollectionAdminRequest.getClusterStatus();
      NamedList<Object> rsp = client.request(req);
      @SuppressWarnings({"unchecked"})
      NamedList<Object> cluster = (NamedList<Object>) rsp.get("cluster");
      assertNotNull("Cluster state should not be null", cluster);
      @SuppressWarnings({"unchecked"})
      NamedList<Object> collections = (NamedList<Object>) cluster.get("collections");
      assertNotNull("Collections should not be null in cluster state", collections);
      assertNotNull("Testing to insure collections are returned", collections.get(COLLECTION_NAME1));
    }
  }

  private void deleteThemAll(SolrZkClient zkClient, String node) throws KeeperException, InterruptedException {
    List<String> kids = zkClient.getChildren(node, null, true);
    for (String kid : kids) {
      deleteThemAll(zkClient, node + "/" + kid);
    }
    zkClient.delete(node, -1, true);
  }

  private void assertCountsForRepFactorAndNrtReplicas(CloudSolrClient client, String collectionName) throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionParams.CollectionAction.CLUSTERSTATUS.toString());
    params.set("collection", collectionName);
    QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");

    NamedList<Object> rsp = client.request(request);
    @SuppressWarnings({"unchecked"})
    NamedList<Object> cluster = (NamedList<Object>) rsp.get("cluster");
    assertNotNull("Cluster state should not be null", cluster);
    @SuppressWarnings({"unchecked"})
    NamedList<Object> collections = (NamedList<Object>) cluster.get("collections");
    assertNotNull("Collections should not be null in cluster state", collections);
    assertEquals(1, collections.size());
    @SuppressWarnings({"unchecked"})
    Map<String, Object> collection = (Map<String, Object>) collections.get(collectionName);
    assertNotNull(collection);
    assertEquals(collection.get("replicationFactor"), collection.get("nrtReplicas"));
  }

  private void clusterStatusWithCollectionAndShard() throws IOException, SolrServerException {

    try (CloudSolrClient client = createCloudClient(null)) {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("action", CollectionParams.CollectionAction.CLUSTERSTATUS.toString());
      params.set("collection", COLLECTION_NAME);
      params.set("shard", SHARD1);
      @SuppressWarnings({"rawtypes"})
      SolrRequest request = new QueryRequest(params);
      request.setPath("/admin/collections");

      NamedList<Object> rsp = client.request(request);
      @SuppressWarnings({"unchecked"})
      NamedList<Object> cluster = (NamedList<Object>) rsp.get("cluster");
      assertNotNull("Cluster state should not be null", cluster);
      @SuppressWarnings({"unchecked"})
      NamedList<Object> collections = (NamedList<Object>) cluster.get("collections");
      assertNotNull("Collections should not be null in cluster state", collections);
      assertNotNull(collections.get(COLLECTION_NAME));
      assertEquals(1, collections.size());
      @SuppressWarnings({"unchecked"})
      Map<String, Object> collection = (Map<String, Object>) collections.get(COLLECTION_NAME);
      @SuppressWarnings({"unchecked"})
      Map<String, Object> shardStatus = (Map<String, Object>) collection.get("shards");
      assertEquals(1, shardStatus.size());
      @SuppressWarnings({"unchecked"})
      Map<String, Object> selectedShardStatus = (Map<String, Object>) shardStatus.get(SHARD1);
      assertNotNull(selectedShardStatus);

    }
  }

  private void clusterStatusWithCollectionAndMultipleShards() throws IOException, SolrServerException {
    try (CloudSolrClient client = createCloudClient(null)) {
      final CollectionAdminRequest.ClusterStatus request = new CollectionAdminRequest.ClusterStatus();
      request.setCollectionName(COLLECTION_NAME);
      request.setShardName(SHARD1 + "," + SHARD2);

      NamedList<Object> rsp = request.process(client).getResponse();
      @SuppressWarnings({"unchecked"})
      NamedList<Object> cluster = (NamedList<Object>) rsp.get("cluster");
      assertNotNull("Cluster state should not be null", cluster);
      @SuppressWarnings({"unchecked"})
      NamedList<Object> collections = (NamedList<Object>) cluster.get("collections");
      assertNotNull("Collections should not be null in cluster state", collections);
      assertNotNull(collections.get(COLLECTION_NAME));
      assertEquals(1, collections.size());
      @SuppressWarnings({"unchecked"})
      Map<String, Object> collection = (Map<String, Object>) collections.get(COLLECTION_NAME);
      @SuppressWarnings({"unchecked"})
      Map<String, Object> shardStatus = (Map<String, Object>) collection.get("shards");
      assertEquals(2, shardStatus.size());
      @SuppressWarnings({"unchecked"})
      Map<String, Object> firstSelectedShardStatus = (Map<String, Object>) shardStatus.get(SHARD1);
      assertNotNull(firstSelectedShardStatus);
      @SuppressWarnings({"unchecked"})
      Map<String, Object> secondSelectedShardStatus = (Map<String, Object>) shardStatus.get(SHARD2);
      assertNotNull(secondSelectedShardStatus);
    }
  }


  private void listCollection() throws IOException, SolrServerException {
    try (CloudSolrClient client = createCloudClient(null)) {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("action", CollectionParams.CollectionAction.LIST.toString());
      @SuppressWarnings({"rawtypes"})
      SolrRequest request = new QueryRequest(params);
      request.setPath("/admin/collections");

      NamedList<Object> rsp = client.request(request);
      @SuppressWarnings({"unchecked"})
      List<String> collections = (List<String>) rsp.get("collections");
      assertTrue("control_collection was not found in list", collections.contains("control_collection"));
      assertTrue(DEFAULT_COLLECTION + " was not found in list", collections.contains(DEFAULT_COLLECTION));
      assertTrue(COLLECTION_NAME + " was not found in list", collections.contains(COLLECTION_NAME));
      assertTrue(COLLECTION_NAME1 + " was not found in list", collections.contains(COLLECTION_NAME1));
    }

  }

  private void clusterStatusNoCollection() throws Exception {

    try (CloudSolrClient client = createCloudClient(null)) {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("action", CollectionParams.CollectionAction.CLUSTERSTATUS.toString());
      @SuppressWarnings({"rawtypes"})
      SolrRequest request = new QueryRequest(params);
      request.setPath("/admin/collections");

      NamedList<Object> rsp = client.request(request);
      @SuppressWarnings({"unchecked"})
      NamedList<Object> cluster = (NamedList<Object>) rsp.get("cluster");
      assertNotNull("Cluster state should not be null", cluster);
      @SuppressWarnings({"unchecked"})
      NamedList<Object> collections = (NamedList<Object>) cluster.get("collections");
      assertNotNull("Collections should not be null in cluster state", collections);
      assertNotNull(collections.get(COLLECTION_NAME1));
      assertEquals(4, collections.size());

      @SuppressWarnings({"unchecked"})
      List<String> liveNodes = (List<String>) cluster.get("live_nodes");
      assertNotNull("Live nodes should not be null", liveNodes);
      assertFalse(liveNodes.isEmpty());
    }

  }

  private void clusterStatusWithCollection() throws IOException, SolrServerException {
    try (CloudSolrClient client = createCloudClient(null)) {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("action", CollectionParams.CollectionAction.CLUSTERSTATUS.toString());
      params.set("collection", COLLECTION_NAME);
      @SuppressWarnings({"rawtypes"})
      SolrRequest request = new QueryRequest(params);
      request.setPath("/admin/collections");

      NamedList<Object> rsp = client.request(request);
      @SuppressWarnings({"unchecked"})
      NamedList<Object> cluster = (NamedList<Object>) rsp.get("cluster");
      assertNotNull("Cluster state should not be null", cluster);
      @SuppressWarnings({"unchecked"})
      NamedList<Object> collections = (NamedList<Object>) cluster.get("collections");
      assertNotNull("Collections should not be null in cluster state", collections);
      assertEquals(1, collections.size());
      @SuppressWarnings({"unchecked"})
      Map<String, Object> collection = (Map<String, Object>) collections.get(COLLECTION_NAME);
      assertNotNull(collection);
      assertEquals("conf1", collection.get("configName"));
//      assertEquals("1", collection.get("nrtReplicas"));
    }
  }

  @SuppressWarnings({"unchecked"})
  private void clusterStatusZNodeVersion() throws Exception {
    String cname = "clusterStatusZNodeVersion";
    try (CloudSolrClient client = createCloudClient(null)) {
      CollectionAdminRequest.createCollection(cname, "conf1", 1, 1).process(client);
      waitForRecoveriesToFinish(cname, true);

      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("action", CollectionParams.CollectionAction.CLUSTERSTATUS.toString());
      params.set("collection", cname);
      @SuppressWarnings({"rawtypes"})
      SolrRequest request = new QueryRequest(params);
      request.setPath("/admin/collections");

      NamedList<Object> rsp = client.request(request);
      NamedList<Object> cluster = (NamedList<Object>) rsp.get("cluster");
      assertNotNull("Cluster state should not be null", cluster);
      NamedList<Object> collections = (NamedList<Object>) cluster.get("collections");
      assertNotNull("Collections should not be null in cluster state", collections);
      assertEquals(1, collections.size());
      Map<String, Object> collection = (Map<String, Object>) collections.get(cname);
      assertNotNull(collection);
      assertEquals("conf1", collection.get("configName"));
      Integer znodeVersion = (Integer) collection.get("znodeVersion");
      assertNotNull(znodeVersion);

      CollectionAdminRequest.AddReplica addReplica = CollectionAdminRequest.addReplicaToShard(cname, "shard1");
      addReplica.process(client);
      waitForRecoveriesToFinish(cname, true);

      rsp = client.request(request);
      cluster = (NamedList<Object>) rsp.get("cluster");
      collections = (NamedList<Object>) cluster.get("collections");
      collection = (Map<String, Object>) collections.get(cname);
      Integer newVersion = (Integer) collection.get("znodeVersion");
      assertNotNull(newVersion);
      assertTrue(newVersion > znodeVersion);
    }
  }

  private static long totalexpectedV2Calls;

  @SuppressWarnings({"rawtypes"})
  public static SolrRequest setV2(SolrRequest req) {
    if (V2Request.v2Calls.get() == null) V2Request.v2Calls.set(new AtomicLong());
    totalexpectedV2Calls = V2Request.v2Calls.get().get();
    if (random().nextBoolean()) {
      req.setUseV2(true);
      req.setUseBinaryV2(random().nextBoolean());
      totalexpectedV2Calls++;
    }
    return req;
  }

  public static void assertV2CallsCount() {
    assertEquals(totalexpectedV2Calls, V2Request.v2Calls.get().get());
  }

  private void clusterStatusWithRouteKey() throws IOException, SolrServerException {
    try (CloudSolrClient client = createCloudClient(DEFAULT_COLLECTION)) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "a!123"); // goes to shard2. see ShardRoutingTest for details
      client.add(doc);
      client.commit();

      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("action", CollectionParams.CollectionAction.CLUSTERSTATUS.toString());
      params.set("collection", DEFAULT_COLLECTION);
      params.set(ShardParams._ROUTE_, "a!");
      @SuppressWarnings({"rawtypes"})
      SolrRequest request = new QueryRequest(params);
      request.setPath("/admin/collections");

      NamedList<Object> rsp = client.request(request);
      @SuppressWarnings({"unchecked"})
      NamedList<Object> cluster = (NamedList<Object>) rsp.get("cluster");
      assertNotNull("Cluster state should not be null", cluster);
      @SuppressWarnings({"unchecked"})
      NamedList<Object> collections = (NamedList<Object>) cluster.get("collections");
      assertNotNull("Collections should not be null in cluster state", collections);
      assertNotNull(collections.get(DEFAULT_COLLECTION));
      assertEquals(1, collections.size());
      @SuppressWarnings({"unchecked"})
      Map<String, Object> collection = (Map<String, Object>) collections.get(DEFAULT_COLLECTION);
      assertEquals("conf1", collection.get("configName"));
      @SuppressWarnings({"unchecked"})
      Map<String, Object> shardStatus = (Map<String, Object>) collection.get("shards");
      assertEquals(1, shardStatus.size());
      @SuppressWarnings({"unchecked"})
      Map<String, Object> selectedShardStatus = (Map<String, Object>) shardStatus.get(SHARD2);
      assertNotNull(selectedShardStatus);
    }
  }

  @SuppressWarnings({"unchecked"})
  private void clusterStatusAliasTest() throws Exception  {
    try (CloudSolrClient client = createCloudClient(null)) {
      // create an alias named myalias
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("action", CollectionParams.CollectionAction.CREATEALIAS.toString());
      params.set("name", "myalias");
      params.set("collections", DEFAULT_COLLECTION + "," + COLLECTION_NAME);
      @SuppressWarnings({"rawtypes"})
      SolrRequest request = new QueryRequest(params);
      request.setPath("/admin/collections");

      client.request(request);

      // request a collection that's part of an alias
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
      assertEquals("conf1", collection.get("configName"));
      List<String> collAlias = (List<String>) collection.get("aliases");
      assertEquals("Aliases not found", Lists.newArrayList("myalias"), collAlias);

      // status request on the alias itself
      params = new ModifiableSolrParams();
      params.set("action", CollectionParams.CollectionAction.CLUSTERSTATUS.toString());
      params.set("collection", "myalias");
      request = new QueryRequest(params);
      request.setPath("/admin/collections");

      // SOLR-12938 - this should NOT cause an exception
      rsp = client.request(request);

      cluster = (NamedList<Object>) rsp.get("cluster");
      assertNotNull("Cluster state should not be null", cluster);
      collections = (NamedList<Object>) cluster.get("collections");
      assertNotNull("Collections should not be null in cluster state", collections);
      assertNotNull(collections.get(DEFAULT_COLLECTION));
      assertNotNull(collections.get(COLLECTION_NAME));

      // status request on something neither an alias nor a collection itself
      params = new ModifiableSolrParams();
      params.set("action", CollectionParams.CollectionAction.CLUSTERSTATUS.toString());
      params.set("collection", "notAnAliasOrCollection");
      request = new QueryRequest(params);
      request.setPath("/admin/collections");

      // SOLR-12938 - this should still cause an exception
      try {
        client.request(request);
        fail("requesting status for 'notAnAliasOrCollection' should cause an exception from CLUSTERSTATUS" );
      } catch (RuntimeException e) {
        // success
      }
    }
  }

  private void clusterStatusRolesTest() throws Exception  {
    try (CloudSolrClient client = createCloudClient(null)) {
      client.connect();
      Replica replica = client.getZkStateReader().getLeaderRetry(DEFAULT_COLLECTION, SHARD1);

      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("action", CollectionParams.CollectionAction.ADDROLE.toString());
      params.set("node", replica.getNodeName());
      params.set("role", "overseer");
      @SuppressWarnings({"rawtypes"})
      SolrRequest request = new QueryRequest(params);
      request.setPath("/admin/collections");
      client.request(request);

      params = new ModifiableSolrParams();
      params.set("action", CollectionParams.CollectionAction.CLUSTERSTATUS.toString());
      params.set("collection", DEFAULT_COLLECTION);
      request = new QueryRequest(params);
      request.setPath("/admin/collections");

      NamedList<Object> rsp = client.request(request);
      @SuppressWarnings({"unchecked"})
      NamedList<Object> cluster = (NamedList<Object>) rsp.get("cluster");
      assertNotNull("Cluster state should not be null", cluster);
      @SuppressWarnings({"unchecked"})
      Map<String, Object> roles = (Map<String, Object>) cluster.get("roles");
      assertNotNull("Role information should not be null", roles);
      @SuppressWarnings({"unchecked"})
      List<String> overseer = (List<String>) roles.get("overseer");
      assertNotNull(overseer);
      assertEquals(1, overseer.size());
      assertTrue(overseer.contains(replica.getNodeName()));
    }
  }

  private void clusterStatusBadCollectionTest() throws Exception {
    try (CloudSolrClient client = createCloudClient(null)) {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("action", CollectionParams.CollectionAction.CLUSTERSTATUS.toString());
      params.set("collection", "bad_collection_name");
      @SuppressWarnings({"rawtypes"})
      SolrRequest request = new QueryRequest(params);
      request.setPath("/admin/collections");

      try {
        client.request(request);
        fail("Collection does not exist. An exception should be thrown");
      } catch (SolrException e) {
        //expected
        assertTrue(e.getMessage().contains("Collection: bad_collection_name not found"));
      }
    }
  }

  private void replicaPropTest() throws Exception {
    try (CloudSolrClient client = createCloudClient(null)) {
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

      @SuppressWarnings({"rawtypes"})
      SolrRequest request = new QueryRequest(params);
      request.setPath("/admin/collections");
      client.request(request);

      // The above should have set exactly one preferredleader...
      verifyPropertyVal(client, COLLECTION_NAME, c1_s1_r1, "preferredleader", "true");
      verifyUniquePropertyWithinCollection(client, COLLECTION_NAME, "preferredLeader");

      doPropertyAction(client,
          "action", CollectionParams.CollectionAction.ADDREPLICAPROP.toString(),
          "collection", COLLECTION_NAME,
          "shard", c1_s1,
          "replica", c1_s1_r2,
          "property", "preferredLeader",
          "property.value", "true");
      // The preferred leader property for shard1 should have switched to the other replica.
      verifyPropertyVal(client, COLLECTION_NAME, c1_s1_r2, "preferredleader", "true");
      verifyUniquePropertyWithinCollection(client, COLLECTION_NAME, "preferredLeader");

      doPropertyAction(client,
          "action", CollectionParams.CollectionAction.ADDREPLICAPROP.toString(),
          "collection", COLLECTION_NAME,
          "shard", c1_s2,
          "replica", c1_s2_r1,
          "property", "preferredLeader",
          "property.value", "true");

      // Now we should have a preferred leader in both shards...
      verifyPropertyVal(client, COLLECTION_NAME, c1_s1_r2, "preferredleader", "true");
      verifyPropertyVal(client, COLLECTION_NAME, c1_s2_r1, "preferredleader", "true");
      verifyUniquePropertyWithinCollection(client, COLLECTION_NAME, "preferredLeader");

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
      verifyUniquePropertyWithinCollection(client, COLLECTION_NAME, "preferredLeader");
      verifyUniquePropertyWithinCollection(client, COLLECTION_NAME1, "preferredLeader");

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
      verifyUniquePropertyWithinCollection(client, COLLECTION_NAME, "preferredLeader");
      verifyUniquePropertyWithinCollection(client, COLLECTION_NAME1, "preferredLeader");

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
      verifyUniquePropertyWithinCollection(client, COLLECTION_NAME, "preferredLeader");
      verifyUniquePropertyWithinCollection(client, COLLECTION_NAME1, "preferredLeader");

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
      verifyUniquePropertyWithinCollection(client, COLLECTION_NAME, "preferredLeader");
      verifyUniquePropertyWithinCollection(client, COLLECTION_NAME1, "preferredLeader");

      doPropertyAction(client,
          "action", CollectionParams.CollectionAction.ADDREPLICAPROP.toLower(),
          "collection", COLLECTION_NAME,
          "shard", c1_s1,
          "replica", c1_s1_r1,
          "property", "testprop",
          "property.value", "nonsense",
          OverseerCollectionMessageHandler.SHARD_UNIQUE, "true");

      verifyPropertyVal(client, COLLECTION_NAME, c1_s1_r2, "preferredleader", "true");
      verifyPropertyVal(client, COLLECTION_NAME, c1_s2_r1, "preferredleader", "true");
      verifyPropertyVal(client, COLLECTION_NAME, c1_s1_r1, "testprop", "nonsense");
      verifyPropertyVal(client, COLLECTION_NAME, c1_s1_r2, "prop", "silly");
      verifyUniquePropertyWithinCollection(client, COLLECTION_NAME, "preferredLeader");
      verifyUniquePropertyWithinCollection(client, COLLECTION_NAME1, "preferredLeader");


      doPropertyAction(client,
          "action", CollectionParams.CollectionAction.ADDREPLICAPROP.toLower(),
          "collection", COLLECTION_NAME,
          "shard", c1_s1,
          "replica", c1_s1_r1,
          "property", "property.testprop",
          "property.value", "true",
          OverseerCollectionMessageHandler.SHARD_UNIQUE, "false");

      verifyPropertyVal(client, COLLECTION_NAME, c1_s1_r2, "preferredleader", "true");
      verifyPropertyVal(client, COLLECTION_NAME, c1_s2_r1, "preferredleader", "true");
      verifyPropertyVal(client, COLLECTION_NAME, c1_s1_r1, "testprop", "true");
      verifyPropertyVal(client, COLLECTION_NAME, c1_s1_r2, "prop", "silly");
      verifyUniquePropertyWithinCollection(client, COLLECTION_NAME, "preferredLeader");
      verifyUniquePropertyWithinCollection(client, COLLECTION_NAME1, "preferredLeader");

      doPropertyAction(client,
          "action", CollectionParams.CollectionAction.DELETEREPLICAPROP.toLower(),
          "collection", COLLECTION_NAME,
          "shard", c1_s1,
          "replica", c1_s1_r1,
          "property", "property.testprop");

      verifyPropertyVal(client, COLLECTION_NAME, c1_s1_r2, "preferredleader", "true");
      verifyPropertyVal(client, COLLECTION_NAME, c1_s2_r1, "preferredleader", "true");
      verifyPropertyNotPresent(client, COLLECTION_NAME, c1_s1_r1, "testprop");
      verifyPropertyVal(client, COLLECTION_NAME, c1_s1_r2, "prop", "silly");
      verifyUniquePropertyWithinCollection(client, COLLECTION_NAME, "preferredLeader");
      verifyUniquePropertyWithinCollection(client, COLLECTION_NAME1, "preferredLeader");

      try {
        doPropertyAction(client,
            "action", CollectionParams.CollectionAction.ADDREPLICAPROP.toString(),
            "collection", COLLECTION_NAME,
            "shard", c1_s1,
            "replica", c1_s1_r1,
            "property", "preferredLeader",
            "property.value", "true",
            OverseerCollectionMessageHandler.SHARD_UNIQUE, "false");
        fail("Should have thrown an exception, setting shardUnique=false is not allowed for 'preferredLeader'.");
      } catch (SolrException se) {
        assertTrue("Should have received a specific error message",
            se.getMessage().contains("with the shardUnique parameter set to something other than 'true'"));
      }

      verifyPropertyVal(client, COLLECTION_NAME, c1_s1_r2, "preferredleader", "true");
      verifyPropertyVal(client, COLLECTION_NAME, c1_s2_r1, "preferredleader", "true");
      verifyPropertyNotPresent(client, COLLECTION_NAME, c1_s1_r1, "testprop");
      verifyPropertyVal(client, COLLECTION_NAME, c1_s1_r2, "prop", "silly");
      verifyUniquePropertyWithinCollection(client, COLLECTION_NAME, "preferredLeader");
      verifyUniquePropertyWithinCollection(client, COLLECTION_NAME1, "preferredLeader");

      Map<String, String> origProps = getProps(client, COLLECTION_NAME, c1_s1_r1,
          "state", "core", "node_name", "base_url");

      doPropertyAction(client,
          "action", CollectionParams.CollectionAction.ADDREPLICAPROP.toLower(),
          "collection", COLLECTION_NAME,
          "shard", c1_s1,
          "replica", c1_s1_r1,
          "property", "state",
          "property.value", "state_bad");

      doPropertyAction(client,
          "action", CollectionParams.CollectionAction.ADDREPLICAPROP.toLower(),
          "collection", COLLECTION_NAME,
          "shard", c1_s1,
          "replica", c1_s1_r1,
          "property", "core",
          "property.value", "core_bad");

      doPropertyAction(client,
          "action", CollectionParams.CollectionAction.ADDREPLICAPROP.toLower(),
          "collection", COLLECTION_NAME,
          "shard", c1_s1,
          "replica", c1_s1_r1,
          "property", "node_name",
          "property.value", "node_name_bad");

      doPropertyAction(client,
          "action", CollectionParams.CollectionAction.ADDREPLICAPROP.toLower(),
          "collection", COLLECTION_NAME,
          "shard", c1_s1,
          "replica", c1_s1_r1,
          "property", "base_url",
          "property.value", "base_url_bad");

      // The above should be on new proeprties.
      verifyPropertyVal(client, COLLECTION_NAME, c1_s1_r1, "state", "state_bad");
      verifyPropertyVal(client, COLLECTION_NAME, c1_s1_r1, "core", "core_bad");
      verifyPropertyVal(client, COLLECTION_NAME, c1_s1_r1, "node_name", "node_name_bad");
      verifyPropertyVal(client, COLLECTION_NAME, c1_s1_r1, "base_url", "base_url_bad");

      doPropertyAction(client,
          "action", CollectionParams.CollectionAction.DELETEREPLICAPROP.toLower(),
          "collection", COLLECTION_NAME,
          "shard", c1_s1,
          "replica", c1_s1_r1,
          "property", "state");

      doPropertyAction(client,
          "action", CollectionParams.CollectionAction.DELETEREPLICAPROP.toLower(),
          "collection", COLLECTION_NAME,
          "shard", c1_s1,
          "replica", c1_s1_r1,
          "property", "core");

      doPropertyAction(client,
          "action", CollectionParams.CollectionAction.DELETEREPLICAPROP.toLower(),
          "collection", COLLECTION_NAME,
          "shard", c1_s1,
          "replica", c1_s1_r1,
          "property", "node_name");

      doPropertyAction(client,
          "action", CollectionParams.CollectionAction.DELETEREPLICAPROP.toLower(),
          "collection", COLLECTION_NAME,
          "shard", c1_s1,
          "replica", c1_s1_r1,
          "property", "base_url");

      // They better not have been changed!
      for (Map.Entry<String, String> ent : origProps.entrySet()) {
        verifyPropertyVal(client, COLLECTION_NAME, c1_s1_r1, ent.getKey(), ent.getValue());
      }

      verifyPropertyNotPresent(client, COLLECTION_NAME, c1_s1_r1, "state");
      verifyPropertyNotPresent(client, COLLECTION_NAME, c1_s1_r1, "core");
      verifyPropertyNotPresent(client, COLLECTION_NAME, c1_s1_r1, "node_name");
      verifyPropertyNotPresent(client, COLLECTION_NAME, c1_s1_r1, "base_url");

    }
  }

  private void testCollectionCreationCollectionNameValidation() throws Exception {
    try (CloudSolrClient client = createCloudClient(null)) {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("action", CollectionParams.CollectionAction.CREATE.toString());
      params.set("name", "invalid@name#with$weird%characters");
      @SuppressWarnings({"rawtypes"})
      SolrRequest request = new QueryRequest(params);
      request.setPath("/admin/collections");

      try {
        client.request(request);
        fail();
      } catch (BaseHttpSolrClient.RemoteSolrException e) {
        final String errorMessage = e.getMessage();
        assertTrue(errorMessage.contains("Invalid collection"));
        assertTrue(errorMessage.contains("invalid@name#with$weird%characters"));
        assertTrue(errorMessage.contains("collection names must consist entirely of"));
      }
    }
  }
  
  private void testCollectionCreationShardNameValidation() throws Exception {
    try (CloudSolrClient client = createCloudClient(null)) {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("action", CollectionParams.CollectionAction.CREATE.toString());
      params.set("name", "valid_collection_name");
      params.set("router.name", "implicit");
      params.set("numShards", "1");
      params.set("shards", "invalid@name#with$weird%characters");
      @SuppressWarnings({"rawtypes"})
      SolrRequest request = new QueryRequest(params);
      request.setPath("/admin/collections");

      try {
        client.request(request);
        fail();
      } catch (BaseHttpSolrClient.RemoteSolrException e) {
        final String errorMessage = e.getMessage();
        assertTrue(errorMessage.contains("Invalid shard"));
        assertTrue(errorMessage.contains("invalid@name#with$weird%characters"));
        assertTrue(errorMessage.contains("shard names must consist entirely of"));
      }
    }
  }
  
  private void testAliasCreationNameValidation() throws Exception{
    try (CloudSolrClient client = createCloudClient(null)) {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("action", CollectionParams.CollectionAction.CREATEALIAS.toString());
      params.set("name", "invalid@name#with$weird%characters");
      params.set("collections", COLLECTION_NAME);
      @SuppressWarnings({"rawtypes"})
      SolrRequest request = new QueryRequest(params);
      request.setPath("/admin/collections");

      try {
        client.request(request);
        fail();
      } catch (BaseHttpSolrClient.RemoteSolrException e) {
        final String errorMessage = e.getMessage();
        assertTrue(errorMessage.contains("Invalid alias"));
        assertTrue(errorMessage.contains("invalid@name#with$weird%characters"));
        assertTrue(errorMessage.contains("alias names must consist entirely of"));
      }
    }
  }

  private void testShardCreationNameValidation() throws Exception {
    try (CloudSolrClient client = createCloudClient(null)) {
      client.connect();
      // Create a collection w/ implicit router
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("action", CollectionParams.CollectionAction.CREATE.toString());
      params.set("name", "valid_collection_name");
      params.set("shards", "a");
      params.set("router.name", "implicit");
      @SuppressWarnings({"rawtypes"})
      SolrRequest request = new QueryRequest(params);
      request.setPath("/admin/collections");
      client.request(request);

      params = new ModifiableSolrParams();
      params.set("action", CollectionParams.CollectionAction.CREATESHARD.toString());
      params.set("collection", "valid_collection_name");
      params.set("shard", "invalid@name#with$weird%characters");

      request = new QueryRequest(params);
      request.setPath("/admin/collections");

      try {
        client.request(request);
        fail();
      } catch (BaseHttpSolrClient.RemoteSolrException e) {
        final String errorMessage = e.getMessage();
        assertTrue(errorMessage.contains("Invalid shard"));
        assertTrue(errorMessage.contains("invalid@name#with$weird%characters"));
        assertTrue(errorMessage.contains("shard names must consist entirely of"));
      }
    }
  }

  // Expects the map will have keys, but blank values.
  private Map<String, String> getProps(CloudSolrClient client, String collectionName, String replicaName, String... props)
      throws KeeperException, InterruptedException {

    client.getZkStateReader().forceUpdateCollection(collectionName);
    ClusterState clusterState = client.getZkStateReader().getClusterState();
    final DocCollection docCollection = clusterState.getCollectionOrNull(collectionName);
    if (docCollection == null || docCollection.getReplica(replicaName) == null) {
      fail("Could not find collection/replica pair! " + collectionName + "/" + replicaName);
    }
    Replica replica = docCollection.getReplica(replicaName);
    Map<String, String> propMap = new HashMap<>();
    for (String prop : props) {
      propMap.put(prop, replica.getProperty(prop));
    }
    return propMap;
  }
  private void missingParamsError(CloudSolrClient client, ModifiableSolrParams origParams)
      throws IOException, SolrServerException {

    @SuppressWarnings({"rawtypes"})
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

  /**
   * After a failed attempt to create a collection (due to bad configs), assert that
   * the collection can be created with a good collection.
   */
  @Test
  @ShardsFixed(num = 2)
  public void testRecreateCollectionAfterFailure() throws Exception {
    // Upload a bad configset
    SolrZkClient zkClient = new SolrZkClient(zkServer.getZkHost(), ZkTestServer.TIMEOUT,
        ZkTestServer.TIMEOUT, null);
    ZkTestServer.putConfig("badconf", zkClient, "/solr", ZkTestServer.SOLRHOME, "bad-error-solrconfig.xml", "solrconfig.xml");
    ZkTestServer.putConfig("badconf", zkClient, "/solr", ZkTestServer.SOLRHOME, "schema-minimal.xml", "schema.xml");
    zkClient.close();

    try (CloudSolrClient client = createCloudClient(null)) {
      // first, try creating a collection with badconf
      BaseHttpSolrClient.RemoteSolrException rse = expectThrows(BaseHttpSolrClient.RemoteSolrException.class, () -> {
          CollectionAdminResponse rsp = CollectionAdminRequest.createCollection
              ("testcollection", "badconf", 1, 2).process(client);
      });
      assertNotNull(rse.getMessage());
      assertNotSame(0, rse.code());

      CollectionAdminResponse rsp = CollectionAdminRequest.createCollection
          ("testcollection", "conf1", 1, 2).process(client);
      assertNull(rsp.getErrorMessages());
      assertSame(0, rsp.getStatus());
    }
  }
}
