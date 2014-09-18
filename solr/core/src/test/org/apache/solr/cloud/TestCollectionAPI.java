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
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.NamedList;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
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
      createCollection(null, COLLECTION_NAME, 2, 1, 1, client, null, "conf1");
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
}
