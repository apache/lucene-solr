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

package org.apache.solr.store.blob.util;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.store.blob.client.CoreStorageClient;
import org.apache.solr.store.shared.SolrCloudSharedStoreTestCase;
import org.apache.solr.store.shared.metadata.SharedShardMetadataController;
import org.apache.solr.store.shared.metadata.SharedShardMetadataController.SharedShardVersionMetadata;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for {@link BlobStoreUtils}
 */
public class BlobStoreUtilsTest extends SolrCloudSharedStoreTestCase { 
  
  private static Path sharedStoreRootPath;
  
  private String collectionName;
  private String shardName;
  private Replica newReplica;
  private CoreContainer cc;
  
  private static CoreStorageClient storageClient;
  
  @BeforeClass
  public static void setupTestClass() throws Exception {
    assumeWorkingMockito();
    sharedStoreRootPath = createTempDir("tempDir");
    storageClient = setupLocalBlobStoreClient(sharedStoreRootPath, DEFAULT_BLOB_DIR_NAME);
    
    assumeWorkingMockito();
  }
  
  @After
  public void doAfter() throws Exception {
    shutdownCluster();
  }
  
  /**
   * testSyncLocalCoreWithSharedStore_syncSkipOnDefault checks that syncLocalCoreWithSharedStore 
   * will skip sync if metadataSuffix is set to default in the ZK.
   */
  @Test
  public void testSyncLocalCoreWithSharedStore_syncSkipOnDefault() throws Exception {
    setupCluster(1);
    setupTestSharedClientForNode(getBlobStorageProviderTestInstance(storageClient), cluster.getJettySolrRunner(0));
    
    collectionName = "sharedCol" + UUID.randomUUID();
    shardName = "shard" + UUID.randomUUID();
    CloudSolrClient cloudClient = cluster.getSolrClient();
    setupSharedCollectionWithShardNames(collectionName, 1, 1, shardName);
        
    DocCollection collection = cloudClient.getZkStateReader().getClusterState().getCollection(collectionName);
    newReplica = collection.getReplicas().get(0);
    cc = getCoreContainer(newReplica.getNodeName());
    
    CoreStorageClient blobClientSpy = Mockito.spy(storageClient);    
    try {
      SharedShardVersionMetadata shardVersionMetadata = new SharedShardVersionMetadata(0, SharedShardMetadataController.METADATA_NODE_DEFAULT_VALUE);
      BlobStoreUtils.syncLocalCoreWithSharedStore(collectionName, newReplica.getCoreName(), shardName, cc, shardVersionMetadata, true);
      verify(blobClientSpy, never()).pullCoreMetadata(anyString(), anyString());
    } catch (Exception ex){
      fail("syncLocalCoreWithSharedStore failed with exception: " + ex.getMessage());
    } 
  }
  
  /**
   * testSyncLocalCoreWithSharedStore_missingBlob checks that syncLocalCoreWithSharedStore 
   * will throw exception if core.metadata file is missing from the sharedStore.
   */
  @Test
  public void testSyncLocalCoreWithSharedStore_missingBlob() throws Exception {
    setupCluster(1);
    setupTestSharedClientForNode(getBlobStorageProviderTestInstance(storageClient), cluster.getJettySolrRunner(0));
    
    collectionName = "sharedCol" + UUID.randomUUID();
    shardName = "shard" + UUID.randomUUID();
    CloudSolrClient cloudClient = cluster.getSolrClient();
    setupSharedCollectionWithShardNames(collectionName, 1, 1, shardName);
        
    DocCollection collection = cloudClient.getZkStateReader().getClusterState().getCollection(collectionName);
    newReplica = collection.getReplicas().get(0);
    cc = getCoreContainer(newReplica.getNodeName());
    
    try {
      SharedShardVersionMetadata shardVersionMetadata = new SharedShardVersionMetadata(0, UUID.randomUUID().toString());
      BlobStoreUtils.syncLocalCoreWithSharedStore(collectionName, newReplica.getCoreName(), shardName, cc, shardVersionMetadata, true);
      fail("syncLocalCoreWithSharedStore should throw exception if shared store doesn't have the core.metadata file.");
    } catch (Exception ex){
      String expectedException = "cannot get core.metadata file from shared store";
      assertTrue(ex.getMessage().contains(expectedException)); 
    } 
  }
  
  /**
   * testSyncLocalCoreWithSharedStore_syncEquivalent checks that syncLocalCoreWithSharedStore 
   * doesn't throw an exception if shared store and local files, already are in sync.
   */
  @Test
  public void testSyncLocalCoreWithSharedStore_syncEquivalent() throws Exception {
    setupCluster(1);
    setupTestSharedClientForNode(getBlobStorageProviderTestInstance(storageClient), cluster.getJettySolrRunner(0));
    
    CloudSolrClient cloudClient = cluster.getSolrClient();
    
    collectionName = "sharedCol" + UUID.randomUUID();
    shardName = "shard" + UUID.randomUUID();
    setupSharedCollectionWithShardNames(collectionName, 1, 1, shardName);
    
    DocCollection collection = cloudClient.getZkStateReader().getClusterState().getCollection(collectionName);
    newReplica = collection.getReplicas().get(0);
    cc = getCoreContainer(newReplica.getNodeName());
    
    CoreStorageClient blobClientSpy = Mockito.spy(storageClient);
    // Add a document.
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", "1");
    doc.setField("cat", "cat123");
    UpdateRequest req = new UpdateRequest();
    req.add(doc);
    req.commit(cloudClient, collectionName);
    try {
      SharedShardMetadataController metadataController = cc.getSharedStoreManager().getSharedShardMetadataController();
      SharedShardVersionMetadata shardVersionMetadata = metadataController.readMetadataValue(collectionName, shardName);
      // we push and already have the latest updates so we should not pull here
      BlobStoreUtils.syncLocalCoreWithSharedStore(collectionName, newReplica.getCoreName(), shardName, cc, shardVersionMetadata, true);
      verify(blobClientSpy, never()).pullCoreMetadata(anyString(), anyString());
    } catch (Exception ex) { 
      fail("syncLocalCoreWithSharedStore failed with exception: " + ex.getMessage());
    }
  }
  
  /**
   * testSyncLocalCoreWithSharedStore_syncSuccess checks that syncLocalCoreWithSharedStore 
   * pulls index files from blob if missing locally and present in blob
   */
  @Test
  public void testSyncLocalCoreWithSharedStore_syncSuccess() throws Exception {
    setupCluster(2);
    
    // configure same client for each runner, this isn't a concurrency test so this is fine
    for (JettySolrRunner runner : cluster.getJettySolrRunners()) {
      setupTestSharedClientForNode(getBlobStorageProviderTestInstance(storageClient), runner);
    }
    
    // set up two nodes with one shard and two replicas 
    collectionName = "sharedCol" + UUID.randomUUID();
    shardName = "shard" + UUID.randomUUID();
    CloudSolrClient cloudClient = cluster.getSolrClient();
    setupSharedCollectionWithShardNames(collectionName, 1, 2, shardName);
    
    CoreStorageClient blobClientSpy = Mockito.spy(storageClient);
    // Add a document.
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", "1");
    doc.setField("cat", "cat123");
    UpdateRequest req = new UpdateRequest();
    req.add(doc);
    req.commit(cluster.getSolrClient(), collectionName);
    
    // get the follower replica    
    DocCollection collection = cloudClient.getZkStateReader().getClusterState().getCollection(collectionName);
    Replica leaderReplica = collection.getLeader(shardName);
    Replica follower = null;
    for (Replica replica : collection.getReplicas()) {
      if (!replica.getName().equals(leaderReplica.getName())) {
        follower = replica;
        break;
      }
    }
    
    // verify this last update didn't happen on the follower, it should only have its default segment file
    cc = getCoreContainer(follower.getNodeName());
    SolrCore core = cc.getCore(follower.getCoreName());    
    assertEquals(1, core.getDeletionPolicy().getLatestCommit().getFileNames().size());
    
    try {
      SharedShardMetadataController metadataController = cc.getSharedStoreManager().getSharedShardMetadataController();
      SharedShardVersionMetadata shardVersionMetadata = metadataController.readMetadataValue(collectionName, shardName);
      // we pushed on the leader, try sync on the follower
      BlobStoreUtils.syncLocalCoreWithSharedStore(collectionName, follower.getCoreName(), shardName, cc, shardVersionMetadata, true);
      
      // did we pull?
      assertTrue(core.getDeletionPolicy().getLatestCommit().getFileNames().size() > 1);
      
      // query just the replica we pulled on
      try (SolrClient directClient = getHttpSolrClient(follower.getBaseUrl() + "/" + follower.getCoreName())) { 
        ModifiableSolrParams params = new ModifiableSolrParams();
        params
          .set("q", "*:*")
          .set("distrib", "false");
        QueryResponse resp = directClient.query(params);
        assertEquals(1, resp.getResults().getNumFound());
        assertEquals("cat123", (String) resp.getResults().get(0).getFieldValue("cat"));
      }
    } catch (Exception ex) { 
      fail("syncLocalCoreWithSharedStore failed with exception: " + ex.getMessage());
    } finally {
      core.close();
    }
  }

  /**
   * Tests that the core properties returned by {@link BlobStoreUtils#getSharedCoreProperties(ZkStateReader, DocCollection, Replica)}
   * match the core properties of a core created in normal flow i.e. create collection, add shard or add replica. They all
   * should essentially produce same set of properties. Here we are using create collection. 
   * 
   * These properties are used to create a missing core against a SHARED replica. 
   */
  @Test
  public void testMissingSharedCoreProperties() throws Exception {
    setupCluster(1);
    collectionName = "sharedCol" + UUID.randomUUID();
    shardName = "shard" + UUID.randomUUID();
    setupSharedCollectionWithShardNames(collectionName, 1, 1, shardName);
    CloudSolrClient cloudClient = cluster.getSolrClient();
    DocCollection coll = cloudClient.getZkStateReader().getClusterState().getCollection(collectionName);
    Replica rep = coll.getLeader(shardName);
    cc = getCoreContainer(rep.getNodeName());

    Path corePropertiesPath = cc.getCoreRootDirectory().resolve(rep.getCoreName()).resolve(CORE_PROPERTIES_FILENAME);
    Properties expectedCoreProperties = new Properties();
    try (InputStreamReader is = new InputStreamReader(new FileInputStream(corePropertiesPath.toFile()), StandardCharsets.UTF_8)) {
      expectedCoreProperties.load(is);
    }

    Map<String, String> coreProperties = BlobStoreUtils.getSharedCoreProperties(cloudClient.getZkStateReader(), coll, rep);

    // name is separately passed as core name, therefore, it is not part of the core properties
    expectedCoreProperties.remove("name");
    /** see comment inside {@link BlobStoreUtils#getSharedCoreProperties(ZkStateReader, DocCollection, Replica)}*/
    expectedCoreProperties.remove("numShards");

    assertEquals("wrong number of core properties", expectedCoreProperties.size(), coreProperties.size());
    for (Object key : expectedCoreProperties.keySet()) {
      assertTrue(key + " is missing", coreProperties.containsKey(key));
      assertEquals(key + "'s value is wrong", expectedCoreProperties.get(key), coreProperties.get(key));
    }
  }
}
