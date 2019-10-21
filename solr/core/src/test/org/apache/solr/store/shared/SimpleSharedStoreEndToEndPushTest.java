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
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.store.blob.client.BlobCoreMetadata;
import org.apache.solr.store.blob.client.CoreStorageClient;
import org.apache.solr.store.blob.util.BlobStoreUtils;
import org.apache.solr.store.shared.metadata.SharedShardMetadataController;
import org.apache.solr.store.shared.metadata.SharedShardMetadataController.SharedShardVersionMetadata;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * A simple end-to-end push test for collections using a shared store 
 */
public class SimpleSharedStoreEndToEndPushTest extends SolrCloudSharedStoreTestCase {
  
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
   * Verify that doing sending a single update with commit=true pushes to the shared store on a 
   * shared-based collection
   */
  @Test
  public void testUpdatePushesToBlobSuccess() throws Exception {
    setupCluster(1);
    CloudSolrClient cloudClient = cluster.getSolrClient();
    
    // setup the test harness
    CoreStorageClient storageClient = setupLocalBlobStoreClient(sharedStoreRootPath, DEFAULT_BLOB_DIR_NAME);
    setupTestSharedClientForNode(getBlobStorageProviderTestInstance(storageClient), cluster.getJettySolrRunner(0));
    
    String collectionName = "sharedCollection";
    int maxShardsPerNode = 1;
    int numReplicas = 1;
    // specify a comma-delimited string of shard names for multiple shards when using
    // an implicit router
    String shardNames = "shard1";
    setupSharedCollectionWithShardNames(collectionName, maxShardsPerNode, numReplicas, shardNames);
   
    // send an update to the cluster
    UpdateRequest updateReq = new UpdateRequest();
    updateReq.add("id", "1");
    updateReq.commit(cloudClient, collectionName);
    
    // verify we can find the document
    QueryRequest queryReq = new QueryRequest(new SolrQuery("*:*"));
    QueryResponse queryRes = queryReq.process(cluster.getSolrClient(), collectionName);
    
    assertEquals(1, queryRes.getResults().size());
    
    // get the replica's core and metadata controller
    DocCollection collection = cloudClient.getZkStateReader().getClusterState().getCollection(collectionName);
    Replica shardLeaderReplica = collection.getLeader("shard1");
    CoreContainer leaderCC = getCoreContainer(shardLeaderReplica.getNodeName());
    SharedShardMetadataController metadataController = leaderCC.getSharedStoreManager().getSharedShardMetadataController();
    try (SolrCore leaderCore = leaderCC.getCore(shardLeaderReplica.getCoreName())) {
      SharedShardVersionMetadata shardMetadata = metadataController.readMetadataValue(collectionName, "shard1");
      Map<String, Object> props = collection.getSlice("shard1").getProperties();
      
      String sharedShardName = (String) props.get(ZkStateReader.SHARED_SHARD_NAME);
      String blobCoreMetadataName = BlobStoreUtils.buildBlobStoreMetadataName(shardMetadata.getMetadataSuffix());
      
      // verify that we pushed the core to blob
      assertTrue(storageClient.coreMetadataExists(sharedShardName, blobCoreMetadataName));
      
      // verify the index files match locally with blob
      BlobCoreMetadata bcm = storageClient.pullCoreMetadata(sharedShardName, blobCoreMetadataName);
      assertEquals(leaderCore.getDeletionPolicy().getLatestCommit().getFileNames().size(),
          bcm.getBlobFiles().length);
    }
  }
}
