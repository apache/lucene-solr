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

import static org.apache.solr.common.cloud.ZkStateReader.MAX_SHARDS_PER_NODE;
import static org.apache.solr.cloud.OverseerCollectionProcessor.NUM_SLICES;
import static org.apache.solr.common.cloud.ZkNodeProps.makeMap;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETEREPLICA;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
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
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

public class DeleteReplicaTest extends AbstractFullDistribZkTestBase {
  private CloudSolrServer client;
  
  @BeforeClass
  public static void beforeThisClass2() throws Exception {

  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    System.setProperty("numShards", Integer.toString(sliceCount));
    System.setProperty("solr.xml.persist", "true");
    client = createCloudClient(null);
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
    client.shutdown();
  }

  protected String getSolrXml() {
    return "solr-no-core.xml";
  }

  public DeleteReplicaTest() {
    fixShardCount = true;

    sliceCount = 2;
    shardCount = 4;

    checkCreatedVsState = false;
  }

  @Override
  public void doTest() throws Exception {
    deleteLiveReplicaTest();
  }

  private void deleteLiveReplicaTest() throws Exception {
    String collectionName = "delLiveColl";
    CloudSolrServer client = createCloudClient(null);
    try {
      createCollection(collectionName, client);
      
      waitForRecoveriesToFinish(collectionName, false);
      
      DocCollection testcoll = getCommonCloudSolrServer().getZkStateReader()
          .getClusterState().getCollection(collectionName);
      
      Slice shard1 = null;
      Replica replica1 = null;
      
      // Get an active replica
      for (Slice slice : testcoll.getSlices()) {
        if(replica1 != null)
          break;
        if ("active".equals(slice.getStr("state"))) {
          shard1 = slice;
          for (Replica replica : shard1.getReplicas()) {
            if ("active".equals(replica.getStr("state"))) {
              replica1 = replica;
              break;
            }
          }
        }
      }

      if (replica1 == null) fail("no active replicas found");

      HttpSolrServer replica1Server = new HttpSolrServer(replica1.getStr("base_url"));
      String dataDir = null;
      try {
        CoreAdminResponse status = CoreAdminRequest.getStatus(replica1.getStr("core"), replica1Server);
        NamedList<Object> coreStatus = status.getCoreStatus(replica1.getStr("core"));
        dataDir = (String) coreStatus.get("dataDir");
      } finally {
        replica1Server.shutdown();
      }
      try {
        // Should not be able to delete a replica that is up if onlyIfDown=true.
        tryToRemoveOnlyIfDown(collectionName, client, replica1, shard1.getName());
        fail("Should have thrown an exception here because the replica is NOT down");
      } catch (SolrException se) {
        assertEquals("Should see 400 here ", se.code(), 400);
        assertTrue("Should have had a good message here", se.getMessage().contains("with onlyIfDown='true', but state is 'active'"));
        // This bit is a little weak in that if we're screwing up and actually deleting the replica, we might get back
        // here _before_ the datadir is deleted. But I'd rather not introduce a delay here.
        assertTrue("dataDir for " + replica1.getName() + " should NOT have been deleted by deleteReplica API with onlyIfDown='true'",
            new File(dataDir).exists());
      }

      removeAndWaitForReplicaGone(collectionName, client, replica1, shard1.getName());
      assertFalse("dataDir for " + replica1.getName() + " should have been deleted by deleteReplica API", new File(dataDir).exists());
    } finally {
      client.shutdown();
    }
  }

  protected void tryToRemoveOnlyIfDown(String collectionName, CloudSolrServer client, Replica replica, String shard) throws IOException, SolrServerException {
    Map m = makeMap("collection", collectionName,
        "action", DELETEREPLICA.toLower(),
        "shard", shard,
        "replica", replica.getName(),
        ZkStateReader.ONLY_IF_DOWN, "true");
    SolrParams params = new MapSolrParams(m);
    SolrRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    client.request(request);
  }

  protected void removeAndWaitForReplicaGone(String COLL_NAME,
      CloudSolrServer client, Replica replica, String shard)
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
      testcoll = getCommonCloudSolrServer().getZkStateReader()
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

  protected void createCollection(String COLL_NAME, CloudSolrServer client) throws Exception {
    int replicationFactor = 2;
    int numShards = 2;
    int maxShardsPerNode = ((((numShards+1) * replicationFactor) / getCommonCloudSolrServer()
        .getZkStateReader().getClusterState().getLiveNodes().size())) + 1;

    Map<String, Object> props = makeMap(
        ZkStateReader.REPLICATION_FACTOR, replicationFactor,
        MAX_SHARDS_PER_NODE, maxShardsPerNode,
        NUM_SLICES, numShards);
    Map<String,List<Integer>> collectionInfos = new HashMap<>();
    createCollection(collectionInfos, COLL_NAME, props, client);
  }
}
