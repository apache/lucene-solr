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

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Replica.Type;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.store.blob.client.CoreStorageClient;
import org.apache.solr.store.shared.SharedCoreConcurrencyController;
import org.apache.solr.store.shared.SharedCoreConcurrencyController.SharedCoreVersionMetadata;
import org.apache.solr.store.shared.SolrCloudSharedStoreTestCase;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests related to shared storage based collections, i.e. collections having only replicas of type {@link Type#SHARED}.
 */
public class SimpleSharedStorageCollectionTest extends SolrCloudSharedStoreTestCase {

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
   * Test that verifies that a basic collection creation command for a "shared" type collection 
   * completes successfully
   */
  @Test
  public void testCreateCollection() throws Exception {
    setupCluster(3);
    setupSolrNodes();
    String collectionName = "BlobBasedCollectionName1";
    CloudSolrClient cloudClient = cluster.getSolrClient();
    
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName, 1, 0).setSharedIndex(true).setSharedReplicas(1);
    create.process(cloudClient).getResponse();
    
    waitForState("Timed-out wait for collection to be created", collectionName, clusterShape(1, 1));
    assertTrue(cloudClient.getZkStateReader().getZkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, false));
  }

  /**
   * Test that verifies that adding a NRT replica to a shared collection fails but adding a SHARED replica
   * to a shard collection completes successfully
   */
  @Test
  public void testAddReplica() throws Exception {
    setupCluster(3);
    setupSolrNodes();
    String collectionName = "BlobBasedCollectionName2";
    CloudSolrClient cloudClient = cluster.getSolrClient();
    
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName, 1, 0).setSharedIndex(true).setSharedReplicas(1);

    // Create the collection
    create.process(cloudClient).getResponse();
    waitForState("Timed-out wait for collection to be created", collectionName, clusterShape(1, 1));
    assertTrue(cloudClient.getZkStateReader().getZkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, false));

    try {
      // Let the request fail cleanly just in case, but in reality it fails with an exception since we throw a Runtime from down below
      CollectionAdminRequest.addReplicaToShard(collectionName, "shard1", Replica.Type.NRT)
          .process(cloudClient);
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Can't add a NRT replica to a collection backed by shared storage"));
    }

    // Adding a SHARED replica is expected to work ok
    assertTrue(CollectionAdminRequest.addReplicaToShard(collectionName, "shard1", Replica.Type.SHARED)
        .process(cloudClient).isSuccess());
  }
  
  /**
   * Test that verifies that creating a collection, deleting it, and re-creating it will evict any pre-existing
   * cached core metadata information from {@link SharedCoreConcurrencyController}. See SOLR-14134
   */
  @Test
  public void testCreateCollectionEvictsExistingMetadata() throws Exception {
    setupCluster(1);
    CloudSolrClient cloudClient = cluster.getSolrClient();
    String collectionName = "SharedCollection";
    int maxShardsPerNode = 1;
    int numReplicas = 1;
    String shardNames = "shard1";
    
    // setup testing components
    setupSolrNodes();
    AtomicInteger evictionCount = new AtomicInteger(0);
    SharedCoreConcurrencyController concurrencyController = 
        configureTestSharedConcurrencyControllerForNode(cluster.getJettySolrRunner(0), evictionCount);
    
    setupSharedCollectionWithShardNames(collectionName, maxShardsPerNode, numReplicas, shardNames);
    
    // do an indexing request to populate the cache entry
    UpdateRequest updateReq = new UpdateRequest();
    updateReq.add("id", "1");
    updateReq.process(cloudClient, collectionName);
    
    // get the single replica for the collection and verify its cache entry has been populated
    DocCollection collection = cloudClient.getZkStateReader().getClusterState().getCollection(collectionName);
    Replica shardLeaderReplica = collection.getLeader(shardNames);
    SharedCoreVersionMetadata scvm = concurrencyController.getCoreVersionMetadata(collectionName, shardNames, shardLeaderReplica.getCoreName());

    // there should be only one update and no evictions
    assertEquals(1, scvm.getVersion());
    assertEquals(0, evictionCount.get());
    
    // delete the collection
    CollectionAdminRequest.Delete delete = CollectionAdminRequest.deleteCollection(collectionName);
    delete.process(cloudClient).getResponse();
    cloudClient.getZkStateReader().waitForState(collectionName, 60, 
        TimeUnit.SECONDS, (collectionState) -> collectionState == null);
    
    // recreate the collection and assert the cached entry gets evicted
    setupSharedCollectionWithShardNames(collectionName, maxShardsPerNode, numReplicas, shardNames);
    assertEquals(1, evictionCount.get());
    
    // CoreVersionMetadata should be at its default values defined in 
    // {@link SharedCoreConcurrencyController#initializeCoreVersionMetadata}
    scvm = concurrencyController.getCoreVersionMetadata(collectionName, shardNames, shardLeaderReplica.getCoreName());
    assertEquals(-1, scvm.getVersion());
    assertEquals(null, scvm.getMetadataSuffix());
    assertEquals(null, scvm.getBlobCoreMetadata());
  }
  
  private void setupSolrNodes() throws Exception {
    for (JettySolrRunner process : cluster.getJettySolrRunners()) {
      CoreStorageClient storageClient = setupLocalBlobStoreClient(sharedStoreRootPath, DEFAULT_BLOB_DIR_NAME);
      setupTestSharedClientForNode(getBlobStorageProviderTestInstance(storageClient), process);
    }
  }

  private SharedCoreConcurrencyController configureTestSharedConcurrencyControllerForNode(JettySolrRunner runner,
      AtomicInteger evictionCount) {
    SharedCoreConcurrencyController concurrencyController = 
        new SharedCoreConcurrencyController(runner.getCoreContainer()) {
      
      @Override
      public boolean removeCoreVersionMetadataIfPresent(String coreName) {
        if (super.removeCoreVersionMetadataIfPresent(coreName)) {
          evictionCount.incrementAndGet();
          return true;
        }
        return false;
      }      
    };
    setupTestSharedConcurrencyControllerForNode(concurrencyController, runner);
    return concurrencyController;
  }
}
