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

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
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
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.solr.cloud.OverseerCollectionProcessor.NUM_SLICES;
import static org.apache.solr.cloud.OverseerCollectionProcessor.ONLY_IF_DOWN;
import static org.apache.solr.common.cloud.ZkNodeProps.makeMap;
import static org.apache.solr.common.cloud.ZkStateReader.MAX_SHARDS_PER_NODE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETEREPLICA;

public class DeleteReplicaTest extends AbstractFullDistribZkTestBase {
  private CloudSolrClient client;
  
  @Override
  public void distribSetUp() throws Exception {
    super.distribSetUp();
    System.setProperty("numShards", Integer.toString(sliceCount));
    System.setProperty("solr.xml.persist", "true");
    client = createCloudClient(null);
  }

  @Override
  public void distribTearDown() throws Exception {
    super.distribTearDown();
    client.close();
  }

  protected String getSolrXml() {
    return "solr-no-core.xml";
  }

  public DeleteReplicaTest() {
    sliceCount = 2;
    checkCreatedVsState = false;
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
      try (HttpSolrClient replica1Client = new HttpSolrClient(replica1.getStr("base_url"))) {
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
    long endAt = System.currentTimeMillis() + 3000;
    boolean success = false;
    DocCollection testcoll = null;
    while (System.currentTimeMillis() < endAt) {
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
}
