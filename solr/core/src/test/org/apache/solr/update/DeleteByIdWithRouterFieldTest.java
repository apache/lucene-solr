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

package org.apache.solr.update;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.TestUtil;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.LBSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.cloud.CloudInspectUtil;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class DeleteByIdWithRouterFieldTest extends SolrCloudTestCase {

  public static final String COLL = "test";
  public static final String ROUTE_FIELD = "field_s";
  public static final int NUM_SHARDS = 3;
  
  private static final List<SolrClient> clients = new ArrayList<>(); // not CloudSolrClient

  /** 
   * A randomized prefix to put on every route value.
   * This helps ensure that documents wind up on diff shards between diff test runs
   */
  private static String RVAL_PRE = null;
  
  @BeforeClass
  public static void setupClusterAndCollection() throws Exception {
    RVAL_PRE = TestUtil.randomRealisticUnicodeString(random());
    
    // sometimes use 2 replicas of every shard so we hit more interesting update code paths
    final int numReplicas = usually() ? 1 : 2;
    
    configureCluster(1 + (NUM_SHARDS * numReplicas) ) // we'll have one node that doesn't host any replicas
      .addConfig("conf", configset("cloud-minimal"))
      .configure();

    assertTrue(CollectionAdminRequest.createCollection(COLL, "conf", NUM_SHARDS, numReplicas)
               .setRouterField(ROUTE_FIELD)
               .process(cluster.getSolrClient())
               .isSuccess());
    
    cluster.getSolrClient().setDefaultCollection(COLL);

    ClusterState clusterState = cluster.getSolrClient().getClusterStateProvider().getClusterState();
    for (Replica replica : clusterState.getCollection(COLL).getReplicas()) {
      clients.add(getHttpSolrClient(replica.getCoreUrl()));
    }
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    IOUtils.close(clients);
    clients.clear();
    RVAL_PRE = null;
  }

  @After
  public void checkShardConsistencyAndCleanUp() throws Exception {
    checkShardsConsistentNumFound();
    assertEquals(0,
                 new UpdateRequest()
                 .deleteByQuery("*:*")
                 .setAction(UpdateRequest.ACTION.COMMIT, true, true)
                 .process(cluster.getSolrClient())
                 .getStatus());
  }
  
  private void checkShardsConsistentNumFound() throws Exception {
    final SolrParams params = params("q", "*:*", "distrib", "false");
    final DocCollection collection = cluster.getSolrClient().getZkStateReader().getClusterState().getCollection(COLL);
    for (Map.Entry<String,Slice> entry : collection.getActiveSlicesMap().entrySet()) {
      final String shardName = entry.getKey();
      final Slice slice = entry.getValue();
      final Replica leader = entry.getValue().getLeader();
      try (SolrClient leaderClient = getHttpSolrClient(leader.getCoreUrl())) {
        final SolrDocumentList leaderResults = leaderClient.query(params).getResults();
        for (Replica replica : slice) {
          try (SolrClient replicaClient = getHttpSolrClient(replica.getCoreUrl())) {
            final SolrDocumentList replicaResults = replicaClient.query(params).getResults();
            assertEquals("inconsistency w/leader: shard=" + shardName + "core=" + replica.getCoreName(),
                         Collections.emptySet(),
                         CloudInspectUtil.showDiff(leaderResults, replicaResults,
                                                   shardName + " leader: " + leader.getCoreUrl(),
                                                   shardName + ": " + replica.getCoreUrl()));
          }
        }
      }
    }
  }
  
  private SolrClient getRandomSolrClient() { 
    final int index = random().nextInt(clients.size() + 1);
    return index == clients.size() ? cluster.getSolrClient() : clients.get(index);
  }

  /** 
   * 100 docs with 2 digit ids and a route field that matches the last digit 
   * @see #del100Docs
   */
  private UpdateRequest add100Docs() {
    final UpdateRequest adds = new UpdateRequest();
    for (int x = 0; x <= 9; x++) {
      for (int y = 0; y <= 9; y++) {
        adds.add("id", x+""+y, ROUTE_FIELD, RVAL_PRE+y);
      }
    }
    return adds;
  }
  
  /** 
   * 100 doc deletions with 2 digit ids and a route field that matches the last digit 
   * @see #add100Docs
   */
  private UpdateRequest del100Docs() {
    final UpdateRequest dels = new UpdateRequest();
    for (int x = 0; x <= 9; x++) {
      for (int y = 0; y <= 9; y++) {
        dels.deleteById(x+""+y, RVAL_PRE+y);
      }
    }
    return dels;
  }

  public void testBlocksOfDeletes() throws Exception {

    assertEquals(0, add100Docs().setAction(UpdateRequest.ACTION.COMMIT, true, true).process(getRandomSolrClient()).getStatus());
    assertEquals(100, getRandomSolrClient().query(params("q", "*:*")).getResults().getNumFound());

    // sanity check a "block" of 1 delete
    assertEquals(0,
                 new UpdateRequest()
                 .deleteById("06", RVAL_PRE+"6")
                 .setAction(UpdateRequest.ACTION.COMMIT, true, true)
                 .process(getRandomSolrClient())
                 .getStatus());
    assertEquals(99, getRandomSolrClient().query(params("q", "*:*")).getResults().getNumFound());

    checkShardsConsistentNumFound();
    
    // block of 2 deletes w/diff routes
    assertEquals(0,
                 new UpdateRequest()
                 .deleteById("17", RVAL_PRE+"7")
                 .deleteById("18", RVAL_PRE+"8")
                 .setAction(UpdateRequest.ACTION.COMMIT, true, true)
                 .process(getRandomSolrClient())
                 .getStatus());
    assertEquals(97, getRandomSolrClient().query(params("q", "*:*")).getResults().getNumFound());
    
    checkShardsConsistentNumFound();

    // block of 2 deletes using single 'withRoute()' for both
    assertEquals(0,
                 new UpdateRequest()
                 .deleteById("29")
                 .deleteById("39")
                 .withRoute(RVAL_PRE+"9")
                 .setAction(UpdateRequest.ACTION.COMMIT, true, true)
                 .process(getRandomSolrClient())
                 .getStatus());
    assertEquals(95, getRandomSolrClient().query(params("q", "*:*")).getResults().getNumFound());
    
    checkShardsConsistentNumFound();
    
    { // block of 2 deletes w/ diff routes that are conditional on optimistic concurrency
      final Long v48 = (Long) getRandomSolrClient().query(params("q", "id:48", "fl", "_version_")).getResults().get(0).get("_version_");
      final Long v49 = (Long) getRandomSolrClient().query(params("q", "id:49", "fl", "_version_")).getResults().get(0).get("_version_");

      assertEquals(0,
                   new UpdateRequest()
                   .deleteById("48", RVAL_PRE+"8", v48)
                   .deleteById("49", RVAL_PRE+"9", v49)
                   .setAction(UpdateRequest.ACTION.COMMIT, true, true)
                   .process(getRandomSolrClient())
                   .getStatus());
    }
    assertEquals(93, getRandomSolrClient().query(params("q", "*:*")).getResults().getNumFound());
    
    checkShardsConsistentNumFound();
    
    { // block of 2 deletes using single 'withRoute()' for both that are conditional on optimistic concurrency
      final Long v58 = (Long) getRandomSolrClient().query(params("q", "id:58", "fl", "_version_")).getResults().get(0).get("_version_");
      final Long v68 = (Long) getRandomSolrClient().query(params("q", "id:68", "fl", "_version_")).getResults().get(0).get("_version_");

      assertEquals(0,
                   new UpdateRequest()
                   .deleteById("58", v58)
                   .deleteById("68", v68)
                   .withRoute(RVAL_PRE+"8")
                   .setAction(UpdateRequest.ACTION.COMMIT, true, true)
                   .process(getRandomSolrClient())
                   .getStatus());
    }
    assertEquals(91, getRandomSolrClient().query(params("q", "*:*")).getResults().getNumFound());
    
    checkShardsConsistentNumFound();
    
    // now delete all docs, including the ones we already deleted (shouldn't cause any problems)
    assertEquals(0, del100Docs().setAction(UpdateRequest.ACTION.COMMIT, true, true).process(getRandomSolrClient()).getStatus());
    assertEquals(0, getRandomSolrClient().query(params("q", "*:*")).getResults().getNumFound());
    
    checkShardsConsistentNumFound();
  }


  /**
   * Test that {@link UpdateRequest#getRoutesToCollection} correctly populates routes for all deletes
   */
  public void testGlassBoxUpdateRequestRoutesToShards() throws Exception {

    final DocCollection docCol = cluster.getSolrClient().getZkStateReader().getClusterState().getCollection(COLL);
    // we don't need "real" urls for all replicas, just something we can use as lookup keys for verification
    // so we'll use the shard names as "leader urls"
    final Map<String,List<String>> urlMap = docCol.getActiveSlices().stream().collect
      (Collectors.toMap(s -> s.getName(), s -> Collections.singletonList(s.getName())));

    // simplified rote info we'll build up with the shards for each delete (after sanity checking they have routing info at all)...
    final Map<String,String> actualDelRoutes = new LinkedHashMap<>();
    
    final Map<String,LBSolrClient.Req> rawDelRoutes = del100Docs().getRoutesToCollection(docCol.getRouter(), docCol, urlMap, params(), ROUTE_FIELD);
    for (LBSolrClient.Req lbreq : rawDelRoutes.values()) {
      assertTrue(lbreq.getRequest() instanceof UpdateRequest);
      final String shard = lbreq.getServers().get(0);
      final UpdateRequest req = (UpdateRequest) lbreq.getRequest();
      for (Map.Entry<String,Map<String,Object>> entry : req.getDeleteByIdMap().entrySet()) {
        final String id = entry.getKey();
        // quick sanity checks...
        assertNotNull(id + " has null values", entry.getValue());
        final Object route = entry.getValue().get(ShardParams._ROUTE_);
        assertNotNull(id + " has null route value", route);
        assertEquals("route value is wrong for id: " + id, RVAL_PRE + id.substring(id.length() - 1), route.toString());

        actualDelRoutes.put(id, shard);
      }
    }

    // look at the routes computed from the "adds" as the expected value for the routes of each "del"
    for (SolrInputDocument doc : add100Docs().getDocuments()) {
      final String id = doc.getFieldValue("id").toString();
      final String actualShard = actualDelRoutes.get(id);
      assertNotNull(id + " delete is missing?", actualShard);
      final Slice expectedShard = docCol.getRouter().getTargetSlice(id, doc, doc.getFieldValue(ROUTE_FIELD).toString(), params(), docCol);
      assertNotNull(id + " add route is null?", expectedShard);
      assertEquals("Wrong shard for delete of id: " + id,
                   expectedShard.getName(), actualShard);
    }

    // sanity check no one broke our test and made it a waste of time
    assertEquals(100, add100Docs().getDocuments().size());
    assertEquals(100, actualDelRoutes.entrySet().size());
  }
  
}

