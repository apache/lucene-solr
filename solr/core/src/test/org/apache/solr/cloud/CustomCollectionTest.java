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

import java.util.Map;

import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.solr.common.cloud.DocCollection.DOC_ROUTER;
import static org.apache.solr.common.cloud.ZkStateReader.MAX_SHARDS_PER_NODE;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICATION_FACTOR;
import static org.apache.solr.common.params.ShardParams._ROUTE_;

/**
 * Tests the Custom Sharding API.
 */
public class CustomCollectionTest extends SolrCloudTestCase {

  private static final int NODE_COUNT = 4;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(NODE_COUNT)
        .addConfig("conf", configset("cloud-dynamic"))
        .configure();
  }

  @Before
  public void ensureClusterEmpty() throws Exception {
    cluster.deleteAllCollections();
  }

  @Test
  public void testCustomCollectionsAPI() throws Exception {

    final String collection = "implicitcoll";
    int replicationFactor = TestUtil.nextInt(random(), 0, 3) + 2;
    int numShards = 3;
    int maxShardsPerNode = (((numShards + 1) * replicationFactor) / NODE_COUNT) + 1;

    CollectionAdminRequest.createCollectionWithImplicitRouter(collection, "conf", "a,b,c", replicationFactor)
        .setMaxShardsPerNode(maxShardsPerNode)
        .process(cluster.getSolrClient());

    DocCollection coll = getCollectionState(collection);
    assertEquals("implicit", ((Map) coll.get(DOC_ROUTER)).get("name"));
    assertNotNull(coll.getStr(REPLICATION_FACTOR));
    assertNotNull(coll.getStr(MAX_SHARDS_PER_NODE));
    assertNull("A shard of a Collection configured with implicit router must have null range",
        coll.getSlice("a").getRange());

    new UpdateRequest()
        .add("id", "6")
        .add("id", "7")
        .add("id", "8")
        .withRoute("a")
        .commit(cluster.getSolrClient(), collection);

    assertEquals(3, cluster.getSolrClient().query(collection, new SolrQuery("*:*")).getResults().getNumFound());
    assertEquals(0, cluster.getSolrClient().query(collection, new SolrQuery("*:*").setParam(_ROUTE_, "b")).getResults().getNumFound());
    assertEquals(3, cluster.getSolrClient().query(collection, new SolrQuery("*:*").setParam(_ROUTE_, "a")).getResults().getNumFound());

    cluster.getSolrClient().deleteByQuery(collection, "*:*");
    cluster.getSolrClient().commit(collection, true, true);
    assertEquals(0, cluster.getSolrClient().query(collection, new SolrQuery("*:*")).getResults().getNumFound());

    new UpdateRequest()
        .add("id", "9")
        .add("id", "10")
        .add("id", "11")
        .withRoute("c")
        .commit(cluster.getSolrClient(), collection);

    assertEquals(3, cluster.getSolrClient().query(collection, new SolrQuery("*:*")).getResults().getNumFound());
    assertEquals(0, cluster.getSolrClient().query(collection, new SolrQuery("*:*").setParam(_ROUTE_, "a")).getResults().getNumFound());
    assertEquals(3, cluster.getSolrClient().query(collection, new SolrQuery("*:*").setParam(_ROUTE_, "c")).getResults().getNumFound());

    //Testing CREATESHARD
    CollectionAdminRequest.createShard(collection, "x")
        .process(cluster.getSolrClient());
    waitForState("Expected shard 'x' to be active", collection, (n, c) -> {
      if (c.getSlice("x") == null)
        return false;
      for (Replica r : c.getSlice("x")) {
        if (r.getState() != Replica.State.ACTIVE)
          return false;
      }
      return true;
    });

    new UpdateRequest()
        .add("id", "66", _ROUTE_, "x")
        .commit(cluster.getSolrClient(), collection);
    // TODO - the local state is cached and causes the request to fail with 'unknown shard'
    // assertEquals(1, cluster.getSolrClient().query(collection, new SolrQuery("*:*").setParam(_ROUTE_, "x")).getResults().getNumFound());

  }

  @Test
  public void testRouteFieldForImplicitRouter() throws Exception {

    int numShards = 4;
    int replicationFactor = TestUtil.nextInt(random(), 0, 3) + 2;
    int maxShardsPerNode = ((numShards * replicationFactor) / NODE_COUNT) + 1;
    String shard_fld = "shard_s";

    final String collection = "withShardField";

    CollectionAdminRequest.createCollectionWithImplicitRouter(collection, "conf", "a,b,c,d", replicationFactor)
        .setMaxShardsPerNode(maxShardsPerNode)
        .setRouterField(shard_fld)
        .process(cluster.getSolrClient());

    new UpdateRequest()
        .add("id", "6", shard_fld, "a")
        .add("id", "7", shard_fld, "a")
        .add("id", "8", shard_fld, "b")
        .commit(cluster.getSolrClient(), collection);

    assertEquals(3, cluster.getSolrClient().query(collection, new SolrQuery("*:*")).getResults().getNumFound());
    assertEquals(1, cluster.getSolrClient().query(collection, new SolrQuery("*:*").setParam(_ROUTE_, "b")).getResults().getNumFound());
    assertEquals(2, cluster.getSolrClient().query(collection, new SolrQuery("*:*").setParam(_ROUTE_, "a")).getResults().getNumFound());

  }

  @Test
  public void testRouteFieldForHashRouter()throws Exception{
    String collectionName = "routeFieldColl";
    int numShards = 4;
    int replicationFactor = 2;
    int maxShardsPerNode = ((numShards * replicationFactor) / NODE_COUNT) + 1;
    String shard_fld = "shard_s";

    CollectionAdminRequest.createCollection(collectionName, "conf", numShards, replicationFactor)
        .setMaxShardsPerNode(maxShardsPerNode)
        .setRouterField(shard_fld)
        .process(cluster.getSolrClient());

    new UpdateRequest()
        .add("id", "6", shard_fld, "a")
        .add("id", "7", shard_fld, "a")
        .add("id", "8", shard_fld, "b")
        .commit(cluster.getSolrClient(), collectionName);

    assertEquals(3, cluster.getSolrClient().query(collectionName, new SolrQuery("*:*")).getResults().getNumFound());
    assertEquals(2, cluster.getSolrClient().query(collectionName, new SolrQuery("*:*").setParam(_ROUTE_, "a")).getResults().getNumFound());
    assertEquals(1, cluster.getSolrClient().query(collectionName, new SolrQuery("*:*").setParam(_ROUTE_, "b")).getResults().getNumFound());
    assertEquals(0, cluster.getSolrClient().query(collectionName, new SolrQuery("*:*").setParam(_ROUTE_, "c")).getResults().getNumFound());


    cluster.getSolrClient().deleteByQuery(collectionName, "*:*");
    cluster.getSolrClient().commit(collectionName);

    cluster.getSolrClient().add(collectionName, new SolrInputDocument("id", "100", shard_fld, "c!doc1"));
    cluster.getSolrClient().commit(collectionName);
    assertEquals(1, cluster.getSolrClient().query(collectionName, new SolrQuery("*:*").setParam(_ROUTE_, "c!")).getResults().getNumFound());

  }

  @Test
  public void testCreateShardRepFactor() throws Exception  {
    final String collectionName = "testCreateShardRepFactor";
    CollectionAdminRequest.createCollectionWithImplicitRouter(collectionName, "conf", "a,b", 1)
        .process(cluster.getSolrClient());

    CollectionAdminRequest.createShard(collectionName, "x")
        .process(cluster.getSolrClient());

    waitForState("Not enough active replicas in shard 'x'", collectionName, (n, c) -> {
      return c.getSlice("x").getReplicas().size() == 1;
    });

  }

}
