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
package org.apache.solr.store.blob.process;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import org.apache.solr.store.blob.client.BlobCoreMetadata;
import org.apache.solr.store.blob.process.CorePullTask.PullCoreCallback;
import org.apache.solr.store.blob.process.CorePullTask.PullTaskMerger;
import org.apache.solr.store.blob.process.CorePullerFeeder.PullCoreInfo;
import org.apache.solr.store.blob.process.CorePullerFeeder.PullCoreInfoMerger;
import org.apache.solr.store.blob.util.DeduplicatingList;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Tests for {@link DeduplicatingList} using {@link PullTaskMerger} and
 * {@link PullCoreInfoMerger}.
 */
public class PullMergeDeduplicationTest {
  
  // not relevant in pulls but value required
  private static final String NEW_METADATA_SUFFIX = "undefined";
  
  private final String COLLECTION_NAME_1 = "collection1";
  private final String COLLECTION_NAME_2 = "collection2";
  private final String SHARD_NAME = "_shard_";
  private final String CORE_NAME = "_core_";
  private final String SHARED_NAME = "_sharedName_";

  /**
   * Verifies that {@link PullCoreInfoMerger} correctly merges two PullCoreInfos
   * together
   */
  @Test
  public void testPullInfoMergerMergesSuccessfully() {
    PullCoreInfoMerger merger = new CorePullerFeeder.PullCoreInfoMerger();
    
    // merging two exactly the same PCI should return exactly the same
    String lastReadMetadataSuffix = "randomString";
    PullCoreInfo pullCoreInfo1 = getTestPullCoreInfo(COLLECTION_NAME_1, 
        lastReadMetadataSuffix, NEW_METADATA_SUFFIX, 5, true, true);
    PullCoreInfo pullCoreInfo2 = getTestPullCoreInfo(COLLECTION_NAME_1, 
        lastReadMetadataSuffix, NEW_METADATA_SUFFIX, 5, true, true);
    
    PullCoreInfo mergedPullCoreInfo = merger.merge(pullCoreInfo1, pullCoreInfo2);
    assertPullCoreInfo(mergedPullCoreInfo, pullCoreInfo2); 
    
    // boolean flags on merge flip
    pullCoreInfo1 = getTestPullCoreInfo(COLLECTION_NAME_1, 
        lastReadMetadataSuffix, NEW_METADATA_SUFFIX, 5, false, true);
    pullCoreInfo2 = getTestPullCoreInfo(COLLECTION_NAME_1, 
        lastReadMetadataSuffix, NEW_METADATA_SUFFIX, 5, true, false);
    
    mergedPullCoreInfo = merger.merge(pullCoreInfo1, pullCoreInfo2);
    assertPullCoreInfo(
        getTestPullCoreInfo(COLLECTION_NAME_1, lastReadMetadataSuffix, NEW_METADATA_SUFFIX, 5, true, true), 
        mergedPullCoreInfo);
    
    // boolean flags on merge flip
    pullCoreInfo1 = getTestPullCoreInfo(COLLECTION_NAME_1, 
        lastReadMetadataSuffix, NEW_METADATA_SUFFIX, 5, true, false);
    pullCoreInfo2 = getTestPullCoreInfo(COLLECTION_NAME_1, 
        lastReadMetadataSuffix, NEW_METADATA_SUFFIX, 5, false, true);
    
    mergedPullCoreInfo = merger.merge(pullCoreInfo1, pullCoreInfo2);
    assertPullCoreInfo(
        getTestPullCoreInfo(COLLECTION_NAME_1, lastReadMetadataSuffix, NEW_METADATA_SUFFIX, 5, true, true), 
        mergedPullCoreInfo);
    
    // ensure the booleans are merged correctly when versions are equivalent 
    pullCoreInfo1 = getTestPullCoreInfo(COLLECTION_NAME_1, 
        lastReadMetadataSuffix, NEW_METADATA_SUFFIX, 5, true, false);
    pullCoreInfo2 = getTestPullCoreInfo(COLLECTION_NAME_1, 
        lastReadMetadataSuffix, NEW_METADATA_SUFFIX, 5, true, false);
    mergedPullCoreInfo = merger.merge(pullCoreInfo1, pullCoreInfo2);
    assertPullCoreInfo(
        getTestPullCoreInfo(COLLECTION_NAME_1, lastReadMetadataSuffix, NEW_METADATA_SUFFIX, 5, true, false), 
        mergedPullCoreInfo);
    
    // somehow if the versions are the same but lastReadMetadataSuffix are not, we fail 
    // due to programming error
    pullCoreInfo1 = getTestPullCoreInfo(COLLECTION_NAME_1, 
        "notRandomString", NEW_METADATA_SUFFIX, 5, false, false);
    pullCoreInfo2 = getTestPullCoreInfo(COLLECTION_NAME_1, 
        lastReadMetadataSuffix, NEW_METADATA_SUFFIX, 5, false, false );
    try {
      merger.merge(pullCoreInfo1, pullCoreInfo2);
      fail();
    } catch (Throwable e) {
      // success
    }
    
    // higher version wins 
    pullCoreInfo1 = getTestPullCoreInfo(COLLECTION_NAME_1, 
        "differentSuffix", NEW_METADATA_SUFFIX, 6, true, true);
    pullCoreInfo2 = getTestPullCoreInfo(COLLECTION_NAME_1, 
        lastReadMetadataSuffix, NEW_METADATA_SUFFIX, 5, false, false);
    
    mergedPullCoreInfo = merger.merge(pullCoreInfo1, pullCoreInfo2);
    assertPullCoreInfo(
        getTestPullCoreInfo(COLLECTION_NAME_1, "differentSuffix", NEW_METADATA_SUFFIX, 6, true, true), 
        mergedPullCoreInfo);
  }
  
  /**
   * Verifies that {@link PullCoreInfoMerger} used in a {@link DeduplicatingList} merges
   * added data successfully
   */
  @Test
  public void testDeduplicatingListWithPullCoreInfoMerge() throws Exception {
    DeduplicatingList<String, PullCoreInfo> dedupList = 
        new DeduplicatingList<>(10, new CorePullerFeeder.PullCoreInfoMerger());
    
    String lastReadMetadataSuffix = "randomString";
    PullCoreInfo pullCoreInfo = getTestPullCoreInfo(COLLECTION_NAME_1, 
        lastReadMetadataSuffix, NEW_METADATA_SUFFIX, 5, true, true);
    dedupList.addDeduplicated(pullCoreInfo, false);
    assertEquals(1, dedupList.size());
    
    // add the same PCI should merge it and only result in once instance
    dedupList.addDeduplicated(pullCoreInfo, false);
    assertEquals(1, dedupList.size());
    
    assertPullCoreInfo(
        getTestPullCoreInfo(COLLECTION_NAME_1, lastReadMetadataSuffix, NEW_METADATA_SUFFIX, 5, true, true), 
        dedupList.removeFirst());
    
    // add a different PCI that should still be merged
    dedupList.addDeduplicated(pullCoreInfo, false);
    
    pullCoreInfo = getTestPullCoreInfo(COLLECTION_NAME_1, 
        "differentString", NEW_METADATA_SUFFIX, 6, false, false);
    dedupList.addDeduplicated(pullCoreInfo, false);
    
    assertEquals(1, dedupList.size());
    assertPullCoreInfo(
        getTestPullCoreInfo(COLLECTION_NAME_1, "differentString", NEW_METADATA_SUFFIX, 6, false, false), 
        dedupList.removeFirst());
    
    dedupList.addDeduplicated(pullCoreInfo, false);
    pullCoreInfo = getTestPullCoreInfo(COLLECTION_NAME_2, 
        "differentString", NEW_METADATA_SUFFIX, 6, false, false);
    // add a different PCI that should not be merged
    dedupList.addDeduplicated(pullCoreInfo, false);
    assertEquals(2, dedupList.size());
    
    // check the integrity of the first 
    assertPullCoreInfo(
        getTestPullCoreInfo(COLLECTION_NAME_1, "differentString", NEW_METADATA_SUFFIX, 6, false, false), 
        dedupList.removeFirst());
  }
  
  /**
   * Verifies that {@link PullTaskMerger} correctly merges two {@link CorePullTask}
   * together
   */
  @Test
  public void testPullTaskMergerMergesSuccessfully() throws Exception {
    PullTaskMerger merger = new CorePullTask.PullTaskMerger();

    // verify that merging two of the same CorePullTask is no-op and the merge results in exactly
    // the same tasks
    CorePullTask expectedCallbackTask = getTestCorePullTask(COLLECTION_NAME_1, 
        /* queuedTimeMs */ 0, /* attempts */ 0, /* lastAttemptTimestamp */ 0, null);
    PullCoreCallback callback = getTestAssertingCallback(expectedCallbackTask);
    CorePullTask task1 = getTestCorePullTask(COLLECTION_NAME_1, 0, 0, 0, callback);
    CorePullTask task2 = getTestCorePullTask(COLLECTION_NAME_1, 0, 0, 0, callback);
    assertCorePullTask(task1, merger.merge(task1, task2));
    
    // verify that merging keeps the larger LastAttemptTimestamp and smaller attempts and queued time
    // expectedCallbackTask should always match the second task argument provided to merge
    expectedCallbackTask = getTestCorePullTask(COLLECTION_NAME_1, 0, 3, 1, null);
    callback = getTestAssertingCallback(expectedCallbackTask);
    task1 = getTestCorePullTask(COLLECTION_NAME_1, 10, 5, 10, callback);
    task2 = getTestCorePullTask(COLLECTION_NAME_1, 0, 3, 1, callback);
    
    CorePullTask expectedTask = getTestCorePullTask(COLLECTION_NAME_1, 0, 3, 10, null);
    assertCorePullTask(expectedTask, merger.merge(task1, task2));
  }
  
  /**
   * Verifies that {@link PullTaskMerger} used in a {@link DeduplicatingList} merges
   * added data successfully
   */
  @Test
  public void testDeduplicatingListWithPullTaskMerger() throws Exception {
    DeduplicatingList<String, CorePullTask> dedupList = 
        new DeduplicatingList<>(10, new CorePullTask.PullTaskMerger());
    
    // verify that merging two of the same CorePullTask is no-op and the merge results in exactly
    // the same tasks
    CorePullTask expectedCallbackTask = getTestCorePullTask(COLLECTION_NAME_1, 0, 0, 0, null);
    PullCoreCallback callback = getTestAssertingCallback(expectedCallbackTask);
    CorePullTask task1 = getTestCorePullTask(COLLECTION_NAME_1, 0, 0, 0, callback);
    CorePullTask task2 = getTestCorePullTask(COLLECTION_NAME_1, 0, 0, 0, callback);
    dedupList.addDeduplicated(task1, false);
    assertEquals(1, dedupList.size());
    
    // note the callback here will evaluate if the task2 matches the expectedCallbackTask;
    // this order is defined in addDeduplicated.
    dedupList.addDeduplicated(task2, false);
    assertEquals(1, dedupList.size());
    // the 'merged' should just equal the original added task
    assertCorePullTask(task1, dedupList.removeFirst());
  }
  
  // return a callback and verify that the callback's propagated merged task matches the provided taskToBeMerged
  private PullCoreCallback getTestAssertingCallback(CorePullTask expectedCallbackTask) {
    return new PullCoreCallback() {
      @Override
      public void finishedPull(CorePullTask pullTask, BlobCoreMetadata blobMetadata, CoreSyncStatus status,
          String message) throws InterruptedException {
        assertNull(blobMetadata);
        assertCorePullTask(expectedCallbackTask, pullTask);
        assertEquals(CoreSyncStatus.TASK_MERGED, status);
        assertEquals("CorePullTask merged with duplicate task in queue.", message);
      }
    };
  }
  
  private CorePullTask getTestCorePullTask(String dedupKey, long queuedTimeMs, int attempts, long lastAttemptTimestamp, PullCoreCallback callback) {
    // assume PullCoreInfo merging works correctly, just create the same static one per CorePullTask
    // with the given dedup key
    PullCoreInfo pullCoreInfo = getTestPullCoreInfo(dedupKey, 
        "randomString", NEW_METADATA_SUFFIX, 5, true, true);
    
    CorePullTask pullTask = new CorePullTask(/* coreContainer */ null, pullCoreInfo, queuedTimeMs,
        attempts, lastAttemptTimestamp, /* callback */ callback, Maps.newHashMap(), Sets.newHashSet());
    return pullTask;
  }
  
  private PullCoreInfo getTestPullCoreInfo(String collectionName, String lastReadMetadataSuffix,
      String newMetadataSuffix, int version, boolean createCoreIfAbsent, boolean waitForSearcher) {
    return new PullCoreInfo(collectionName, 
        collectionName + SHARD_NAME,
        collectionName + CORE_NAME,
        collectionName + SHARED_NAME,
        lastReadMetadataSuffix, newMetadataSuffix, version, 
        createCoreIfAbsent, waitForSearcher);
  }
  
  private void assertCorePullTask(CorePullTask expected, CorePullTask actual) {
    assertEquals("CorePullTask attempts do not match", expected.getAttempts(), actual.getAttempts());
    assertEquals("CorePullTask lastAttempTimestamps do not match", expected.getLastAttemptTimestamp(), actual.getLastAttemptTimestamp());
    assertEquals("CorePullTask dedupKeys do not match", expected.getDedupeKey(), actual.getDedupeKey());
    assertEquals("CorePullTask queuedTimed do not match", expected.getQueuedTimeMs(), actual.getQueuedTimeMs());
    assertPullCoreInfo(expected.getPullCoreInfo(), actual.getPullCoreInfo());
  }
  
  private void assertPullCoreInfo(PullCoreInfo expected, PullCoreInfo actual) {
    assertEquals("PullCoreInfos collection names do not match", expected.getCollectionName(), actual.getCollectionName());
    assertEquals("PullCoreInfos core names do not match", expected.getCoreName(), actual.getCoreName());
    assertEquals("PullCoreInfos shard names do not match", expected.getShardName(), actual.getShardName());
    assertEquals("PullCoreInfos sharedStore names do not match", expected.getSharedStoreName(), actual.getSharedStoreName());
    assertEquals("PullCoreInfos lastReadMetadataSuffix do not match", expected.getLastReadMetadataSuffix(), actual.getLastReadMetadataSuffix());
    assertEquals("PullCoreInfos zkVersions do not match", expected.getZkVersion(), actual.getZkVersion());
    assertEquals("PullCoreInfos shouldCreateCoreifAbsent flag do not match", expected.shouldCreateCoreIfAbsent(), actual.shouldCreateCoreIfAbsent());
    assertEquals("PullCoreInfos shouldWaitForSearcher flag do not match", expected.shouldWaitForSearcher(), actual.shouldWaitForSearcher());
  }
}
