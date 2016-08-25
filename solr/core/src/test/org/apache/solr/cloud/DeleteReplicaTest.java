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

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.FileUtils;
import org.apache.solr.util.TimeOut;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.OverseerCollectionMessageHandler.NUM_SLICES;
import static org.apache.solr.cloud.OverseerCollectionMessageHandler.ONLY_IF_DOWN;
import static org.apache.solr.common.cloud.ZkStateReader.MAX_SHARDS_PER_NODE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETEREPLICA;
import static org.apache.solr.common.util.Utils.makeMap;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.REQUESTSTATUS;
import org.apache.solr.client.solrj.response.RequestStatusState;


public class DeleteReplicaTest extends AbstractFullDistribZkTestBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected String getSolrXml() {
    return "solr.xml";
  }

  public DeleteReplicaTest() {
    sliceCount = 2;
  }

  @Test
  @ShardsFixed(num = 4)
  public void deleteLiveReplicaTest() throws Exception {
    String collectionName = "delLiveColl";
    try (CloudSolrClient client = createCloudClient(null)) {
      createCollection(collectionName, client);

      waitForRecoveriesToFinish(collectionName, false);

      DocCollection testcoll = getCommonCloudSolrClient().getZkStateReader()
          .getClusterState().getCollection(collectionName);

      Slice shard1 = null;
      Replica replica1 = null;

      // Get an active replica
      for (Slice slice : testcoll.getSlices()) {
        if(replica1 != null)
          break;
        if (slice.getState() == Slice.State.ACTIVE) {
          shard1 = slice;
          for (Replica replica : shard1.getReplicas()) {
            if (replica.getState() == Replica.State.ACTIVE) {
              replica1 = replica;
              break;
            }
          }
        }
      }

      if (replica1 == null) fail("no active replicas found");

      String dataDir = null;
      try (HttpSolrClient replica1Client = getHttpSolrClient(replica1.getStr("base_url"))) {
        CoreAdminResponse status = CoreAdminRequest.getStatus(replica1.getStr("core"), replica1Client);
        NamedList<Object> coreStatus = status.getCoreStatus(replica1.getStr("core"));
        dataDir = (String) coreStatus.get("dataDir");
      }
      try {
        // Should not be able to delete a replica that is up if onlyIfDown=true.
        tryToRemoveOnlyIfDown(collectionName, client, replica1, shard1.getName());
        fail("Should have thrown an exception here because the replica is NOT down");
      } catch (SolrException se) {
        assertEquals("Should see 400 here ", se.code(), 400);
        assertTrue("Expected DeleteReplica to fail because node state is 'active' but returned message was: " + se.getMessage(), se.getMessage().contains("with onlyIfDown='true', but state is 'active'"));
        // This bit is a little weak in that if we're screwing up and actually deleting the replica, we might get back
        // here _before_ the datadir is deleted. But I'd rather not introduce a delay here.
        assertTrue("dataDir for " + replica1.getName() + " should NOT have been deleted by deleteReplica API with onlyIfDown='true'",
            new File(dataDir).exists());
      }

      removeAndWaitForReplicaGone(collectionName, client, replica1, shard1.getName());
      assertFalse("dataDir for " + replica1.getName() + " should have been deleted by deleteReplica API", new File(dataDir).exists());
    }
  }


  protected void tryToRemoveOnlyIfDown(String collectionName, CloudSolrClient client, Replica replica, String shard) throws IOException, SolrServerException {
    Map m = makeMap("collection", collectionName,
        "action", DELETEREPLICA.toLower(),
        "shard", shard,
        "replica", replica.getName(),
        ONLY_IF_DOWN, "true");
    SolrParams params = new MapSolrParams(m);
    SolrRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    client.request(request);
  }

  static void removeAndWaitForReplicaGone(String COLL_NAME,
                                          CloudSolrClient client, Replica replica, String shard)
          throws SolrServerException, IOException, InterruptedException {
    Map m = makeMap("collection", COLL_NAME, "action", DELETEREPLICA.toLower(), "shard",
            shard, "replica", replica.getName());
    SolrParams params = new MapSolrParams(m);
    SolrRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    client.request(request);
    TimeOut timeout = new TimeOut(3, TimeUnit.SECONDS);
    boolean success = false;
    DocCollection testcoll = null;
    while (! timeout.hasTimedOut()) {
      testcoll = client.getZkStateReader()
              .getClusterState().getCollection(COLL_NAME);
      success = testcoll.getSlice(shard).getReplica(replica.getName()) == null;
      if (success) {
        log.info("replica cleaned up {}/{} core {}",
                shard + "/" + replica.getName(), replica.getStr("core"));
        log.info("current state {}", testcoll);
        break;
      }
      Thread.sleep(100);
    }
    assertTrue("Replica not cleaned up", success);
  }


  protected void tryRemoveReplicaByCountAndShard(String collectionName, CloudSolrClient client, int count, String shard) throws IOException, SolrServerException {
    Map m = makeMap("collection", collectionName,
            "action", DELETEREPLICA.toLower(),
            "shard", shard,
            "count", count);
    SolrParams params = new MapSolrParams(m);
    SolrRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    client.request(request);
  }


  protected void tryRemoveReplicaByCountAsync(String collectionName, CloudSolrClient client, int count, String requestid) throws IOException, SolrServerException {
    Map m = makeMap("collection", collectionName,
            "action", DELETEREPLICA.toLower(),
            "count", count,
            "async", requestid);
    SolrParams params = new MapSolrParams(m);
    SolrRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    client.request(request);
  }


  protected String trackRequestStatus(CloudSolrClient client, String requestId) throws IOException, SolrServerException {
    Map m = makeMap("action", REQUESTSTATUS.toLower(),
            "requestid", requestId);
    SolrParams params = new MapSolrParams(m);
    SolrRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    NamedList<Object> resultsList = client.request(request);
    NamedList innerResponse = (NamedList) resultsList.get("status");
    return (String) innerResponse.get("state");
  }



  protected void createCollection(String COLL_NAME, CloudSolrClient client) throws Exception {
    int replicationFactor = 2;
    int numShards = 2;
    int maxShardsPerNode = ((((numShards+1) * replicationFactor) / getCommonCloudSolrClient()
        .getZkStateReader().getClusterState().getLiveNodes().size())) + 1;

    Map<String, Object> props = makeMap(
        ZkStateReader.REPLICATION_FACTOR, replicationFactor,
        MAX_SHARDS_PER_NODE, maxShardsPerNode,
        NUM_SLICES, numShards);
    Map<String,List<Integer>> collectionInfos = new HashMap<>();
    createCollection(collectionInfos, COLL_NAME, props, client);
  }

  @Test
  @ShardsFixed(num = 2)
  public void deleteReplicaAndVerifyDirectoryCleanup() throws IOException, SolrServerException, InterruptedException {
    createCollection("deletereplica_test", 1, 2, 4);

    Replica leader = cloudClient.getZkStateReader().getLeaderRetry("deletereplica_test", "shard1");
    String baseUrl = (String) leader.get("base_url");
    String core = (String) leader.get("core");
    String leaderCoreName = leader.getName();

    String instanceDir;
    String dataDir;

    try (HttpSolrClient client = getHttpSolrClient(baseUrl)) {
      CoreAdminResponse statusResp = CoreAdminRequest.getStatus(core, client);
      NamedList r = statusResp.getCoreStatus().get(core);
      instanceDir = (String) r.findRecursive("instanceDir");
      dataDir = (String) r.get("dataDir");
    }

    //Confirm that the instance and data directory exist
    assertTrue("Instance directory doesn't exist", FileUtils.fileExists(instanceDir));
    assertTrue("DataDirectory doesn't exist", FileUtils.fileExists(dataDir));

    new CollectionAdminRequest.DeleteReplica()
        .setCollectionName("deletereplica_test")
        .setShardName("shard1")
        .setReplica(leaderCoreName)
        .process(cloudClient);

    Replica newLeader = cloudClient.getZkStateReader().getLeaderRetry("deletereplica_test", "shard1");

    assertFalse(leader.equals(newLeader));

    //Confirm that the instance and data directory were deleted by default

    assertFalse("Instance directory still exists", FileUtils.fileExists(instanceDir));
    assertFalse("DataDirectory still exists", FileUtils.fileExists(dataDir));
  }

  @Test
  @ShardsFixed(num = 4)
  public void deleteReplicaByCount() throws Exception {
    String collectionName = "deleteByCount";
    try (CloudSolrClient client = createCloudClient(null)) {
      createCollection(collectionName, 1, 3, 5);

      waitForRecoveriesToFinish(collectionName, false);

      DocCollection testcoll = getCommonCloudSolrClient().getZkStateReader()
              .getClusterState().getCollection(collectionName);
      Collection<Slice> slices = testcoll.getActiveSlices();
      assertEquals(slices.size(), 1);
      for (Slice individualShard:  slices) {
        assertEquals(individualShard.getReplicas().size(),3);
      }


      try {
        // Should not be able to delete 2 replicas (non leader ones for a given shard
        tryRemoveReplicaByCountAndShard(collectionName, client, 2, "shard1");
        testcoll = getCommonCloudSolrClient().getZkStateReader()
                .getClusterState().getCollection(collectionName);
        slices = testcoll.getActiveSlices();
        assertEquals(slices.size(), 1);
        for (Slice individualShard:  slices) {
          assertEquals(individualShard.getReplicas().size(),1);
        }

      } catch (SolrException se) {
        fail("Should have been able to remove the replica successfully");
      }

    }
  }

  @Test
  @ShardsFixed(num = 4)
  public void deleteReplicaByCountForAllShards() throws Exception {
    String collectionName = "deleteByCountNew";
    try (CloudSolrClient client = createCloudClient(null)) {
      createCollection(collectionName, 2, 2, 5);

      waitForRecoveriesToFinish(collectionName, false);

      DocCollection testcoll = getCommonCloudSolrClient().getZkStateReader()
              .getClusterState().getCollection(collectionName);
      Collection<Slice> slices = testcoll.getActiveSlices();
      assertEquals(slices.size(), 2);
      for (Slice individualShard:  slices) {
        assertEquals(individualShard.getReplicas().size(),2);
      }

      String requestIdAsync = "1000";

      try {
        // Should not be able to delete 2 replicas from all shards (non leader ones)
        tryRemoveReplicaByCountAsync(collectionName, client, 1, requestIdAsync);

        //Make sure request completes
        String requestStatus = trackRequestStatus(client, requestIdAsync);

        while ((!requestStatus.equals(RequestStatusState.COMPLETED.getKey()))  && (!requestStatus.equals(RequestStatusState.FAILED.getKey()))) {
          requestStatus = trackRequestStatus(client, requestIdAsync);
        }


        testcoll = getCommonCloudSolrClient().getZkStateReader()
                .getClusterState().getCollection(collectionName);
        slices = testcoll.getActiveSlices();
        assertEquals(slices.size(), 2);
        for (Slice individualShard:  slices) {
          assertEquals(individualShard.getReplicas().size(),1);
        }

      } catch (SolrException se) {
        fail("Should have been able to remove the replica successfully");
      }

    }


  }

}

