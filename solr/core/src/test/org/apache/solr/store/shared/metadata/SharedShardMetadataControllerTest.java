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
package org.apache.solr.store.shared.metadata;

import java.util.Arrays;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.BadVersionException;
import org.apache.solr.client.solrj.cloud.autoscaling.VersionedData;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests for {@link SharedShardMetadataController}
 */
public class SharedShardMetadataControllerTest extends SolrCloudTestCase {

  static final String TEST_COLLECTION_NAME = "testCollectionName1";
  static final String TEST_SHARD_NAME = "testShardName";
  
  static String metadataNodePath;
  
  static SharedShardMetadataController shardMetadataController; 
  static SolrCloudManager cloudManager; 
  
  @BeforeClass
  public static void setupCluster() throws Exception {    
    configureCluster(1)
      .addConfig("conf", configset("cloud-minimal"))
      .configure();
    
    cloudManager = cluster.getJettySolrRunner(0).getCoreContainer().
        getZkController().getSolrCloudManager();
    
    // setup a shared collection
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(TEST_COLLECTION_NAME, 1, 0)
        .setSharedIndex(true)
        .setSharedReplicas(1);
    create.process(cluster.getSolrClient());
    
    waitForState("Timed-out wait for collection to be created", TEST_COLLECTION_NAME, clusterShape(1, 1));
    
    shardMetadataController = new SharedShardMetadataController(cloudManager);
    metadataNodePath = shardMetadataController.getMetadataBasePath(TEST_COLLECTION_NAME, TEST_SHARD_NAME) + 
        "/" + SharedShardMetadataController.SUFFIX_NODE_NAME;
    
    assumeWorkingMockito();
  }
  
  @After
  public void cleanup() throws Exception {
    cluster.getZkClient().clean(metadataNodePath);
  }
  
  /**
   * Test that we create and persist a metadata node
   */
  @Test
  public void testSetupMetadataNode() throws Exception {    
    shardMetadataController.ensureMetadataNodeExists(TEST_COLLECTION_NAME, TEST_SHARD_NAME);
    assertTrue(cluster.getZkClient().exists(metadataNodePath, false));
  }
  
  /**
   * Test that we fail to create the metadata node if we attempt to create it on a collection that is not
   * of type shared
   */
  @Test
  public void testSetupMetadataNodeFailsOnNonSharedCollection() throws Exception {
    // setup a non-shared collection
    String nonSharedCollectionName = "notShared";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(nonSharedCollectionName, 1, 1);
    create.process(cluster.getSolrClient());
    
    waitForState("Timed-out wait for collection to be created", nonSharedCollectionName, clusterShape(1, 1));
    try {
      shardMetadataController.ensureMetadataNodeExists(nonSharedCollectionName, "notSharedShard");
      fail();
    } catch (SolrException ex) {
      // we should fail
    } catch (Exception ex) {
      fail("Unexpected exception " + ex);
    }
  }
  
  /*
   * Test that we can update the metadata node without passing a version check value (pass -1) 
   */
  @Test
  public void testUpdateMetadataNode() throws Exception {    
    shardMetadataController.ensureMetadataNodeExists(TEST_COLLECTION_NAME, TEST_SHARD_NAME);
    assertTrue(cluster.getZkClient().exists(metadataNodePath, false));
    
    String testMetadataValue = "testValue";
    shardMetadataController.updateMetadataValueWithVersion(TEST_COLLECTION_NAME, TEST_SHARD_NAME, 
        testMetadataValue, -1);
    Stat stat = new Stat();
    byte[] data = cluster.getZkClient().getData(metadataNodePath, null, stat, false);
    
    Map<String, Object> readData = (Map<String, Object>) Utils.fromJSON(data); 
    assertEquals(testMetadataValue, readData.get(SharedShardMetadataController.SUFFIX_NODE_NAME));
  }
  
  /*
   * Test that we can update the metadata node passing a version check value and that we fail if the
   * version doesn't match
   */
  @Test
  public void testConditionalUpdateOnMetadataNode() throws Exception {    
    shardMetadataController.ensureMetadataNodeExists(TEST_COLLECTION_NAME, TEST_SHARD_NAME);
    assertTrue(cluster.getZkClient().exists(metadataNodePath, false));
    
    String testMetadataValue = "testValue1";
    
    // setup with an initial value by writing
    shardMetadataController.updateMetadataValueWithVersion(TEST_COLLECTION_NAME, TEST_SHARD_NAME, 
        testMetadataValue, -1);
    Stat stat = new Stat();
    byte[] data = cluster.getZkClient().getData(metadataNodePath, null, stat, false);
    
    Map<String, Object> readData = (Map<String, Object>) Utils.fromJSON(data);
    assertEquals(testMetadataValue, readData.get(SharedShardMetadataController.SUFFIX_NODE_NAME));
    
    int version = stat.getVersion();
    // try a conditional update that should pass and return a VersionedData instance with
    // the right written value and incremented version
    testMetadataValue = "testValue2";
    VersionedData versionedData = shardMetadataController.updateMetadataValueWithVersion(TEST_COLLECTION_NAME, TEST_SHARD_NAME, 
        testMetadataValue, version);
    
    
    // the version monotonically increases, increments on updates. We should expect only one update
    readData = (Map<String, Object>) Utils.fromJSON(versionedData.getData());
    assertEquals(testMetadataValue, readData.get(SharedShardMetadataController.SUFFIX_NODE_NAME));
    assertEquals(version + 1, versionedData.getVersion());
    
    // try a conditional update that fails with the wrong version number
    try {
      versionedData = shardMetadataController.updateMetadataValueWithVersion(TEST_COLLECTION_NAME, TEST_SHARD_NAME, 
          testMetadataValue, 100);
      fail();
    } catch (SolrException ex) {
      Throwable t = ex.getCause();
      // we should fail specifically for solr's BadVersionException in this test
      assertTrue(t instanceof BadVersionException);
    } catch (Exception ex) {
      fail();
    }
  }
  
  /**
   * Test reading the metadata node returns the expected value
   */
  public void testReadMetadataNode() throws Exception {
    shardMetadataController.ensureMetadataNodeExists(TEST_COLLECTION_NAME, TEST_SHARD_NAME);
    assertTrue(cluster.getZkClient().exists(metadataNodePath, false));
    
    String testMetadataValue = "testValue1";
    // setup with an initial value by writing
    shardMetadataController.updateMetadataValueWithVersion(TEST_COLLECTION_NAME, TEST_SHARD_NAME, 
        testMetadataValue, -1);
    
    VersionedData versionedData = shardMetadataController.readMetadataValue(TEST_COLLECTION_NAME, TEST_SHARD_NAME);
    
    Map<String, Object> readData = (Map<String, Object>) Utils.fromJSON(versionedData.getData());
    assertEquals(testMetadataValue, readData.get(SharedShardMetadataController.SUFFIX_NODE_NAME));
  }
  
  /**
   * Test reading/updating the metadata node when it doesn't exist fails 
   */
  public void testAccessingNonExistentNodeFails() throws Exception {
    String testMetadataValue = "testValue1";
    
    try {    
      // setup with an initial value by writing
      shardMetadataController.updateMetadataValueWithVersion(TEST_COLLECTION_NAME, TEST_SHARD_NAME, 
          testMetadataValue, -1);
      fail();
    } catch (SolrException ex) {
      Throwable t = ex.getCause();
      assertTrue(t instanceof NoSuchElementException);
    }
    
    try {    
      // setup with an initial value by writing
      shardMetadataController.readMetadataValue(TEST_COLLECTION_NAME, TEST_SHARD_NAME);
      fail();
    } catch (SolrException ex) {
      Throwable t = ex.getCause();
      assertTrue(t instanceof NoSuchElementException);
    }
  }
  
  /**
   * Test that successful conditional updates caches in VersionedData in memory
   */
  @Test
  public void testSuccessfulUpdateCaches() throws Exception {
    // reset cache 
    shardMetadataController.getVersionedDataCache().clear();
    
    shardMetadataController.ensureMetadataNodeExists(TEST_COLLECTION_NAME, TEST_SHARD_NAME);
    assertTrue(cluster.getZkClient().exists(metadataNodePath, false));
    
    String testMetadataValue = "testValue1";
    
    // setup with an initial value by writing
    VersionedData newData = shardMetadataController.updateMetadataValueWithVersion(TEST_COLLECTION_NAME, TEST_SHARD_NAME, 
        testMetadataValue, -1);
    String cacheKey = shardMetadataController.getCacheKey(TEST_COLLECTION_NAME, TEST_SHARD_NAME);
    VersionedData cachedData = shardMetadataController.getVersionedDataCache().get(cacheKey);
    // verify the VersionedData returned is the same as the one cached
    assertNotNull(cachedData);
    assertEquals(newData.getVersion(), cachedData.getVersion());
    assertTrue(Arrays.equals(newData.getData(), cachedData.getData()));
  }
  
  /**
   * Test that if an entry a VersionedData exists for a given collection and shard, and a conditional
   * update fails, we remove the existing cached entry
   */
  @Test
  public void testUnsuccessfulUpdateRemovesCacheEntry() throws Exception {
    // reset cache 
    shardMetadataController.getVersionedDataCache().clear();
    
    shardMetadataController.ensureMetadataNodeExists(TEST_COLLECTION_NAME, TEST_SHARD_NAME);
    assertTrue(cluster.getZkClient().exists(metadataNodePath, false));
    
    String testMetadataValue = "testValue1";
    
    // setup with an initial value by writing
    shardMetadataController.updateMetadataValueWithVersion(TEST_COLLECTION_NAME, TEST_SHARD_NAME, 
        testMetadataValue, -1);
    String cacheKey = shardMetadataController.getCacheKey(TEST_COLLECTION_NAME, TEST_SHARD_NAME);
    // verify our entry is cached now
    VersionedData cachedData = shardMetadataController.getVersionedDataCache().get(cacheKey);
    assertNotNull(cachedData);
    
    // write a value that should fail with a bad version value
    try {
      shardMetadataController.updateMetadataValueWithVersion(TEST_COLLECTION_NAME, TEST_SHARD_NAME, 
          testMetadataValue, 100);
      fail("Updating metadata value with the wrong version should have failed");
    } catch (Exception ex) {
      
    }
    
    // verify the cached value was removed
    cachedData = shardMetadataController.getVersionedDataCache().get(cacheKey);
    assertNull(cachedData);
  }
  
  /**
   * Test that if an entry a VersionedData exists for a given collection and shard, it is returned
   * on read if specified so.
   */
  @Test
  public void testReadReturnsCachedEntry() throws Exception {
    // set up mocks to verify we read from the cache
    ConcurrentHashMap<String, VersionedData> cacheSpy = Mockito.spy(new ConcurrentHashMap<String, VersionedData>());
    SolrCloudManager cloudManagerMock = Mockito.mock(SolrCloudManager.class);
    DistribStateManager distribStateManagerMock = Mockito.mock(DistribStateManager.class);
    Mockito.when(cloudManagerMock.getDistribStateManager()).thenReturn(distribStateManagerMock);
    
    SharedShardMetadataController shardMetadataControllerWithMock = 
        new SharedShardMetadataController(cloudManagerMock, cacheSpy); 
    
    // set up some fake data
    String cacheKey = shardMetadataControllerWithMock.getCacheKey(TEST_COLLECTION_NAME, TEST_SHARD_NAME);
    VersionedData mockData = new VersionedData(1, new byte[] {}, CreateMode.EPHEMERAL, "test");
    shardMetadataControllerWithMock.getVersionedDataCache().put(cacheKey, mockData);
    
    // read and verify we pull from the cache if the value exists
    shardMetadataControllerWithMock.readMetadataValue(TEST_COLLECTION_NAME, TEST_SHARD_NAME, true);
    
    // verify we pull from cache and not zookeeper
    Mockito.verify(cacheSpy).get(shardMetadataControllerWithMock.getCacheKey(TEST_COLLECTION_NAME, TEST_SHARD_NAME));
    Mockito.verifyZeroInteractions(distribStateManagerMock);
  }
  
  /**
   * Test that if readFromCache is specified when reading the metadata node and an entry in the
   * cache for the given collection and shard doesn't exist, we read directly from zookeeper
   */
  @Test
  public void testReadRetrievesFromZooKeeper() throws Exception {
    ConcurrentHashMap<String, VersionedData> cacheSpy = Mockito.spy(new ConcurrentHashMap<String, VersionedData>());
    SolrCloudManager cloudManagerMock = Mockito.mock(SolrCloudManager.class);
    DistribStateManager distribStateManagerMock = Mockito.mock(DistribStateManager.class);
    
    Mockito.when(cloudManagerMock.getDistribStateManager()).thenReturn(distribStateManagerMock);
    Mockito.when(distribStateManagerMock.getData(Mockito.any(), Mockito.any())).thenReturn(null);
    
    SharedShardMetadataController shardMetadataControllerWithMock = 
        new SharedShardMetadataController(cloudManagerMock, cacheSpy); 
    
    // read and verify we pull from the cache if the value exists
    shardMetadataControllerWithMock.readMetadataValue(TEST_COLLECTION_NAME, TEST_SHARD_NAME, true);
    
    // verify we're reading from zookeeper and not cache (even though there's no real data there)
    Mockito.verify(distribStateManagerMock, Mockito.times(1)).getData(Mockito.any(), Mockito.any());
  }
  
  /**
   * Verify we can clear the cached version for some collection and shard
   */
  public void testClearCachedVersion() throws Exception {
    // set up mocks to verify we read from the cache
    ConcurrentHashMap<String, VersionedData> cacheSpy = Mockito.spy(new ConcurrentHashMap<String, VersionedData>());
    SolrCloudManager cloudManagerMock = Mockito.mock(SolrCloudManager.class);
    DistribStateManager distribStateManagerMock = Mockito.mock(DistribStateManager.class);
    
    Mockito.when(cloudManagerMock.getDistribStateManager()).thenReturn(distribStateManagerMock);
    Mockito.when(distribStateManagerMock.getData(Mockito.any(), Mockito.any())).thenReturn(null);
    
    SharedShardMetadataController shardMetadataControllerWithMock = 
        new SharedShardMetadataController(cloudManagerMock, cacheSpy);
    
    // set up some fake data
    String cacheKey = shardMetadataControllerWithMock.getCacheKey(TEST_COLLECTION_NAME, TEST_SHARD_NAME);
    VersionedData mockData = new VersionedData(1, new byte[] {}, CreateMode.EPHEMERAL, "test");
    shardMetadataControllerWithMock.getVersionedDataCache().put(cacheKey, mockData);
    
    assertEquals(1, shardMetadataControllerWithMock.getVersionedDataCache().size());
    // try to clean and verify 
    shardMetadataControllerWithMock.clearCachedVersion(TEST_COLLECTION_NAME, TEST_SHARD_NAME);
    Mockito.verify(cacheSpy).remove(Mockito.any());
    assertEquals(0, shardMetadataControllerWithMock.getVersionedDataCache().size());
  }
}