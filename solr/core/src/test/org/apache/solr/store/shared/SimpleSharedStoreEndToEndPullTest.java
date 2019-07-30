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
package org.apache.solr.store.shared;

import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.store.blob.client.CoreStorageClient;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * A simple end-to-end pull test for collections using a shared store
 */
public class SimpleSharedStoreEndToEndPullTest extends SolrCloudSharedStoreTestCase {
  
  private static Path sharedStoreRootPath;
  
  @BeforeClass
  public static void setupClass() throws Exception {
    sharedStoreRootPath = createTempDir("tempDir");    
  }
  
  @After
  public void teardownTest() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
    // clean up the shared store after each test. The temp dir should clean up itself after the
    // test class finishes
    FileUtils.cleanDirectory(sharedStoreRootPath.toFile());
  }
  
  /**
   * Tests that if an update gets processed by a leader and the leader pushes to the shared store,
   * then a replica receiving a query should pull the deltas from the shared store
   */
  @Test
  public void testCorePullSucceeds() throws Exception {
    setupCluster(2);
    CloudSolrClient cloudClient = cluster.getSolrClient();
    
    // setup the test harness
    CoreStorageClient storageClient1 = setupLocalBlobStoreClient(sharedStoreRootPath, DEFAULT_BLOB_DIR_NAME);
    setupTestSharedClientForNode(getBlobStorageProviderTestInstance(storageClient1), cluster.getJettySolrRunner(0));
    CoreStorageClient storageClient2 = setupLocalBlobStoreClient(sharedStoreRootPath, DEFAULT_BLOB_DIR_NAME);
    setupTestSharedClientForNode(getBlobStorageProviderTestInstance(storageClient2), cluster.getJettySolrRunner(1));
    
    String collectionName = "sharedCollection";
    int maxShardsPerNode = 1;
    int numReplicas = 2;
    // specify a comma-delimited string of shard names for multiple shards when using
    // an implicit router
    String shardNames = "shard1";
    setupSharedCollectionWithShardNames(collectionName, maxShardsPerNode, numReplicas, shardNames);
    
    // send an update to the cluster
    UpdateRequest updateReq = new UpdateRequest();
    updateReq.add("id", "1");
    updateReq.commit(cloudClient, collectionName);
    
    // get the leader replica and follower replicas
    DocCollection collection = cloudClient.getZkStateReader().getClusterState().getCollection(collectionName);
    Replica shardLeaderReplica = collection.getLeader("shard1");
    Replica followerReplica = null;
    for (Replica repl : collection.getSlice("shard1").getReplicas()) {
      if (repl.getName() != shardLeaderReplica.getName()) {
        followerReplica = repl;
        break;
      }
    }
    
    // verify the update wasn't forwarded to the follower and it didn't commit by checking the core
    // this gives us confidence that the subsequent query we do triggers the pull
    CoreContainer replicaCC = getCoreContainer(followerReplica.getNodeName());
    SolrCore core = null;
    SolrClient followerDirectClient = null;
    SolrClient leaderDirectClient = null;
    try {
      core = replicaCC.getCore(followerReplica.getCoreName());
      // the follower should only have the default segments file
      assertEquals(1, core.getDeletionPolicy().getLatestCommit().getFileNames().size());
      
      // query the leader directly to verify it should have the document
      leaderDirectClient = getHttpSolrClient(shardLeaderReplica.getBaseUrl() + "/" + shardLeaderReplica.getCoreName());
      ModifiableSolrParams params = new ModifiableSolrParams();
      params
        .set("q", "*:*")
        .set("distrib", "false");
      QueryResponse resp = leaderDirectClient.query(params);
      assertEquals(1, resp.getResults().getNumFound());
      assertEquals("1", (String) resp.getResults().get(0).getFieldValue("id"));
      
      // query the follower directly to trigger the pull, this query should yield no results
      // as we don't wait
      followerDirectClient = getHttpSolrClient(followerReplica.getBaseUrl() + "/" + followerReplica.getCoreName());
      resp = followerDirectClient.query(params);
      assertEquals(0, resp.getResults().getNumFound());
      
      // TODO super ugly and inappropriate but the pull shouldn't take long. At some point we'll
      // make our end-to-end async testing nicer by supporting test listeners for the async tasks 
      Thread.sleep(5000);
      
      // do another query to verify we've pulled everything
      resp = followerDirectClient.query(params);
      
      // verify we pulled
      assertTrue(core.getDeletionPolicy().getLatestCommit().getFileNames().size() > 1);
      
      // verify the document is present
      assertEquals(1, resp.getResults().getNumFound());
      assertEquals("1", (String) resp.getResults().get(0).getFieldValue("id"));
    } finally {
      leaderDirectClient.close();
      followerDirectClient.close();
      core.close();
    }
  }
}
