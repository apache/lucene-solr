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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.store.blob.client.BlobCoreMetadata;
import org.apache.solr.store.blob.client.CoreStorageClient;
import org.apache.solr.store.shared.SharedCoreConcurrencyController.SharedConcurrencyMetadataCacheBuilder;
import org.apache.solr.store.shared.SharedCoreConcurrencyController.SharedCoreVersionMetadata;
import org.apache.solr.store.shared.TimeAwareLruCache.CacheRemovalTrigger;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests around removal, eviction, and loading of entries from a {@link TimeAwareLruCache}
 * implementation in the {@link SharedCoreConcurrencyController}
 */
public class SharedCoreConcurrencyCacheTest extends SolrCloudSharedStoreTestCase {

  /**
   * Path for local shared store
   */
  private static Path sharedStoreRootPath;
  
  private TimeAwareLruCache<String, SharedCoreVersionMetadata> cache;
  
  @BeforeClass
  public static void setupClass() throws Exception {
    sharedStoreRootPath = createTempDir("tempDir");
  }
  
  /**
   * Verify that if we add entries beyond the max size, we evict the LRU entry
   * from the cache
   */
  @Test
  public void testSizeBasedEviction() throws Exception {
    int maxSize = 3;
    long expirationTimeSeconds = 60;
    
    List<RemovalContext> removals = new LinkedList<>();
    cache = getEvictingCacheForTest(maxSize, expirationTimeSeconds, (k, v, c) -> {
      removals.add(new RemovalContext(k, v, c));
    });
    cache.put("key1", getSharedCoreVersionMetadataForTest(1));
    cache.put("key2", getSharedCoreVersionMetadataForTest(1));
    cache.put("key3", getSharedCoreVersionMetadataForTest(1));
    cache.put("key4", getSharedCoreVersionMetadataForTest(1));
    // call cleanUp to invoke any pending maintenance operations as result of a cache eviction policy
    cache.cleanUp();
    assertNull(cache.get("key1"));
    assertNotNull(cache.get("key2"));
    
    // verify eviction was a result of a size violation and the least recently used is evicted
    assertEquals(1, removals.size());
    assertEquals("key1", removals.get(0).key);
    assertRemovalCausesContains(removals, "SIZE", 1);
  }
  
  /**
   * Verify if an entry hasn't been accessed within the expiration duration,
   * it gets evicted from the cache
   */
  @Test
  public void testTimeBasedEviction() throws Exception {
    int maxSize = 5;
    long expirationTimeSeconds = 5;
    
    CountDownLatch latch = new CountDownLatch(4);
    List<RemovalContext> removals = new LinkedList<>();
    cache = getEvictingCacheForTest(maxSize, expirationTimeSeconds, (k, v, c) -> {
      latch.countDown();
      removals.add(new RemovalContext(k, v, c));
    });
    cache.put("key1", getSharedCoreVersionMetadataForTest(1));
    cache.put("key2", getSharedCoreVersionMetadataForTest(1));
    cache.put("key3", getSharedCoreVersionMetadataForTest(1));
    cache.put("key4", getSharedCoreVersionMetadataForTest(1));
    // call cleanUp to invoke any pending maintenance operations as result of a cache eviction policy
    // The removal listener is invoked async and cleanUp needs to be invoked on a separate thread
    // from the test thread after the expirationTimeSeconds
    ScheduledExecutorService ses = scheduleCleanup(expirationTimeSeconds);
    latch.await(expirationTimeSeconds*2, TimeUnit.SECONDS);
    
    // verify all evictions were a result of time expirations
    assertEquals(4, removals.size());
    assertRemovalCausesContains(removals, "EXPIRED", 4);
    assertNull(cache.get("key1"));
    assertNull(cache.get("key2"));
    assertNull(cache.get("key3"));
    assertNull(cache.get("key4"));
    ses.shutdownNow();
  }
  
  /**
   * Verify simple add, remove, and replacement operations on the cache
   */
  @Test
  public void testAddRemove() throws Exception {
    int maxSize = 3;
    long expirationTimeSeconds = 30;
    List<RemovalContext> removals = new LinkedList<>();
    cache = getEvictingCacheForTest(maxSize, expirationTimeSeconds, (k, v, c) -> {
      removals.add(new RemovalContext(k, v, c));
    });
    
    // verify simple put
    cache.put("key1", getSharedCoreVersionMetadataForTest(1));
    SharedCoreVersionMetadata metadata = cache.get("key1");
    assertNotNull(metadata);
    assertEquals(1, metadata.getVersion());
    assertEquals(0, removals.size());
    
    // verify put overwrites pre-existing key value pair
    cache.put("key1", getSharedCoreVersionMetadataForTest(2));
    metadata = cache.get("key1");
    
    // call cleanUp to invoke any pending maintenance operations
    cache.cleanUp();
    assertNotNull(metadata);
    assertEquals(2, metadata.getVersion());
    assertEquals(1, removals.size());
    assertRemovalCausesContains(removals, "REPLACED", 1);
    removals.clear();
    
    // verify invalidate and removals, removal listeners invoke async so we just check the list 
    // contains what's expected
    assertTrue(cache.invalidate("key1"));
    assertNull(cache.get("key1"));
    
    // call cleanUp to invoke any pending maintenance operations
    cache.cleanUp();
    assertEquals(1, removals.size());
    assertRemovalCausesContains(removals, "EXPLICIT", 1);
    removals.clear();
    
    // verify computeIfAbsent
    assertNull(cache.get("key2"));
    cache.computeIfAbsent("key2", k -> {
      return getSharedCoreVersionMetadataForTest(-1);
    });
    metadata = cache.get("key2");
    assertNotNull(metadata);
    assertEquals(-1, metadata.getVersion());
  }
  
  /**
   * Verifies eviction of entries in the {@link SharedCoreConcurrencyController} metadata cache 
   * during end to end indexing processes
   */
  @Test
  public void testEndToEndEviction() throws Exception {
    setupCluster(1);
    int maxSize = 3;
    long expirationTimeSeconds = 10;
    CloudSolrClient client = cluster.getSolrClient();
    
    // setup test controller and cache
    JettySolrRunner solrProcess = cluster.getJettySolrRunner(0);
    CoreStorageClient storageClient = setupLocalBlobStoreClient(sharedStoreRootPath, DEFAULT_BLOB_DIR_NAME);
    setupTestSharedClientForNode(getBlobStorageProviderTestInstance(storageClient), solrProcess);
    
    cache = getEvictingCacheForTest(maxSize, expirationTimeSeconds, null);
    SharedCoreConcurrencyController testController = getConcurrencyControllerForTest(solrProcess, cache);
    setupTestSharedConcurrencyControllerForNode(testController, solrProcess);
    
    // create a 3 shard collection
    String collectionName = "shared";
    int maxShardsPerNode = 50;
    int numReplicas = 1;
    String shardNames = "shard1,shard2,shard3";
    setupSharedCollectionWithShardNames(collectionName, maxShardsPerNode, numReplicas, shardNames);
    
    // index on each shard 
    indexDoc(collectionName, "1", "shard1");
    indexDoc(collectionName, "2", "shard2");
    
    // verify replicas were populated in the cache
    DocCollection collection = client.getZkStateReader().getClusterState().getCollection(collectionName);
    String shardCoreName1 = collection.getLeader("shard1").getCoreName();
    String shardCoreName2 = collection.getLeader("shard2").getCoreName();
    String shardCoreName3 = collection.getLeader("shard3").getCoreName();
    
    // shard1 core should have been accessed
    SharedCoreVersionMetadata metadata = cache.get(shardCoreName1);
    assertNotNull(metadata);
    assertEquals(1, metadata.getVersion());
    
    // shard2 core should have been accessed
    metadata = cache.get(shardCoreName2);
    assertNotNull(metadata);
    assertEquals(1, metadata.getVersion());

    // shard3 core should not have been accessed
    assertNull(cache.get(shardCoreName3));
  }
  
  private ScheduledExecutorService scheduleCleanup(long delay) throws Exception {
    ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
    executorService.schedule(() -> cache.cleanUp(), delay + 1, TimeUnit.SECONDS);
    return executorService;
  }
  
  // the shared collection created in this test uses the implicit router so we can specify a _route_ param
  private void indexDoc(String collectionName, String id, String shardId) throws IOException, SolrServerException {
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", id);
    doc.addField("_route_", shardId);
    
    UpdateRequest updateReq = new UpdateRequest();
    updateReq.add(doc);
    updateReq.process(cluster.getSolrClient(), collectionName); 
  }
  
  private SharedCoreConcurrencyController getConcurrencyControllerForTest(JettySolrRunner solrProcess, 
      TimeAwareLruCache<String, SharedCoreVersionMetadata> testCache) {
    return new SharedCoreConcurrencyController(solrProcess.getCoreContainer()) {
      @Override
      protected TimeAwareLruCache<String, SharedCoreVersionMetadata> buildMetadataCache() {
        return testCache;
      }
    };
  }
  
  private SharedCoreVersionMetadata getSharedCoreVersionMetadataForTest(int version) {
    String metadataSuffix = null;
    BlobCoreMetadata bcm = null;
    boolean softGuaranteeOfEquality = false;
    ReentrantReadWriteLock corePullLock = null;
    ReentrantLock corePushLock = null;
    return new SharedCoreVersionMetadata(version, metadataSuffix, bcm, softGuaranteeOfEquality, corePullLock, corePushLock);
  }
  
  // trigger can be null
  private TimeAwareLruCache<String, SharedCoreVersionMetadata> getEvictingCacheForTest(
      int maxSize, long expirationTimeSeconds, CacheRemovalTrigger<String, SharedCoreVersionMetadata> trigger) {  
    SharedConcurrencyMetadataCacheBuilder builder = new SharedConcurrencyMetadataCacheBuilder();
    return builder.maxSize(maxSize)
      .removalTrigger(trigger)
      .expirationTimeSeconds(expirationTimeSeconds)
      .build();
  }
  
  // assert the list of removal contexts contain the expectedCount number of removalCauses 
  private void assertRemovalCausesContains(List<RemovalContext> removals, String expectedCause, int expectedCount) {
    int count = 0;
    List<String> descriptions = removals.stream().map(context -> context.description).collect(Collectors.toList());
    for (String desc : descriptions) {
      if (desc.equals(expectedCause)) {
        count++;
      }
    }
    assertEquals(expectedCount, count);
  }
  
  private class RemovalContext {
    String key;
    SharedCoreVersionMetadata value;
    String description;
    
    public RemovalContext(String key, SharedCoreVersionMetadata value, String desc) {
      this.key = key;
      this.value = value;
      this.description = desc;
    }
  }
}
