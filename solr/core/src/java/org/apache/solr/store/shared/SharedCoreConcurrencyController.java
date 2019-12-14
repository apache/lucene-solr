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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.annotations.VisibleForTesting;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.StringUtils;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.servlet.HttpSolrCall;
import org.apache.solr.store.blob.client.BlobCoreMetadata;
import org.apache.solr.store.blob.metadata.PushPullData;
import org.apache.solr.store.blob.process.CorePullTask;
import org.apache.solr.store.blob.process.CorePullerThread;
import org.apache.solr.store.blob.process.CorePusher;
import org.apache.solr.store.blob.util.BlobStoreUtils;
import org.apache.solr.store.shared.metadata.SharedShardMetadataController;
import org.apache.solr.store.shared.metadata.SharedShardMetadataController.SharedShardVersionMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class helps coordinate synchronization of concurrent indexing, pushes and pulls
 * happening on a core of a shared collection {@link DocCollection#getSharedIndex()}
 * The objective of synchronization is not only to be functionally correct but also efficient i.e.
 * we don't unnecessarily fail things that are happening concurrently on a single node.
 * Following paragraphs explain those needs and how are those addressed.
 *
 * General requirement: The source of truth for shared cores is shared store. We pull from shared store if a core is
 * locally absent or is stale. We push to shared store at the end of indexing. Queries can only trigger pulls. Whereas,
 * indexing does a pull (only if the core is stale) then local indexing and finally a push to shared store. It is
 * important that local indexing always happen on source of truth that was in the shared store when the batch arrived,
 * so that at the push time new source of truth is whatever was there before plus new local indexing on top of that. In
 * scenarios below we will talk about concurrent local indexing. Solr already takes care of that. Our job is to make 
 * sure that shared cores play nicely and efficiently with that i.e. we do not end up completely serializing each
 * indexing batch.
 *
 *
 * Scenario#1: Since leadership can change anytime, we need to protect the source of truth from being incorrectly
 * overwritten by an indexing batch on ghost leader (was a real leader when received the batch but at the time of push
 * world has changed).
 * Solution: We use optimistic concurrency by maintaining a unique identifier (metadataSuffix) in zookeeper that is
 * changed at each push. At push time we make sure that metadataSuffix is still at the value on which we started the
 * local indexing i.e. no one else became the leader while we were indexing locally and did a push to shared store.
 * If metadataSuffix in zookeeper happens to have changed we fail the indexing batch.
 * {@link SharedShardVersionMetadata} is representative of that unique identifier in zookeeper.
 *
 * Scenario#2: A pull is going on and another pull comes in, they will step over each other. Even if we pull into a 
 * new index directory on each pull we will have to ensure that pull corresponding to later contents of shared store wins. 
 * Solution: We serialize pulls.
 * {@link #getCorePullLock(String, String, String)}'s write lock is used for that purpose.
 *
 * Scenario#3: The core is stale and an indexing batch comes in, refreshes it and proceeds to local indexing. After the
 * local indexing is finished and before the push to shared store, a query comes in and triggers the pull. Because of
 * local indexing, local contents are different from shared store, pull will go ahead and overwrite them with what is on
 * shared store(it being the source of truth). Now the indexing batch will go ahead and finish its push and declare 
 * success. The only problem is it did not push anything since whatever it had indexed locally was undone by the query pull.
 * Note! In this scenario query pull can also be replaced by a pull of another concurrent indexing request.
 * Solution: We need a cache telling us the last shared store state that was pulled in successfully i.e. a value of
 * metadataSuffix that we also for conditional zk update at push time(as explained earlier scenario#1). This cache not
 * only provides functional correctness but also prevents from unnecessary comparison of local and shared store contents.
 * {@link #coresVersionMetadata} is that per core cache.
 *
 * Scenario#4: Local indexing is finished and server goes into a long GC pause and loses leadership. The new leader makes
 * progress on indexing. The old leader comes out of GC pause. Before the push of stalled batch resumes, a query comes
 * in and triggers a pull. That pull will overwrite local index with whatever the new leader has pushed to shared store.
 * We will lose the local indexing of stalled batch. Because of successful pull, our cache now matches with what's in
 * shared store. The push will be successful but end up pushing nothing.
 * Note! In this scenario query pull can also be replaced by a pull of another concurrent indexing request(assuming old
 * leader becomes leader again).
 * Solution: We need to protect indexing(including pushes) from pulls.
 * {@link #getCorePullLock(String, String, String)}'s read lock is used for that protection.
 *
 * Scenario#5: Two concurrent indexing threads finish up their local indexing and are ready to push. One pushes and advances
 * metadaSuffix in zookeeper. The second one being on previous cached version would fail on conditional zk update. This
 * will functionally be correct but inefficient since second's batch progress will go to waste.
 * Solution: We serialize pushes to shared store. So that concurrent pushes on the same node do not fail.
 * {@link #getCorePushLock(String, String, String)} is used for that serialization.
 * Corollary: When an indexing thread goes into push phase it will not only push its own batch rather it will end up pushing 
 * all other batches that have been committed up till that point. Later on, the threads responsible for other batches will
 * just do a touch and go(assuming no other batch came afterwards). In other words, push phase of indexing can be described 
 * as: "push if there is something to be pushed (my batch could be part of it or someone else has already pushed that for me)"
 *
 *
 * Miscellaneous Notes:
 * 1. Since we are doing synchronous pushes of files from index directory to shared store we only support hard commits
 *    for shared collections. We ensure that each batch has a hard commit {@link HttpSolrCall#addCommitIfAbsent()}.
 * 2. In steady state a leader does not need to consult zookeeper all the time since it is the only one updating the
 *    metadataSuffix. For that we rely on {@link SharedCoreVersionMetadata#softGuaranteeOfEquality}. Leaders set it to 
 *    "true" when they successfully pull from or push to shared store. If we happen to be incorrect, conditional update
 *    of zookeeper will fail at push time.
 * 3. We also cache {@link SharedCoreVersionMetadata#blobCoreMetadata} so that in steady state leaders don't need to read
 *    it from shared store at push time.
 * 4. {@link #coresVersionMetadata} cache:
 *    a. It is a {@link ConcurrentHashMap} and consists of immutable values, therefore, updating it is an atomic operation.
 *    b. We initialize it only once with values never to be found in zookeeper{@link #initializeCoreVersionMetadata(String, String, String)}.
 *    c. We can only update {@link #coresVersionMetadata} under either a pull write lock or push lock along with pull read lock.
 *       {@link #updateCoreVersionMetadata(String, String, String, SharedCoreVersionMetadata, SharedCoreVersionMetadata)}
 */
public class SharedCoreConcurrencyController {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  /**
   * Time indexing thread needs to wait to try acquiring pull write lock before checking if someone else has already done the pull.
   */
  public static int SECONDS_TO_WAIT_INDEXING_PULL_WRITE_LOCK = 5;
  /**
   * Max attempts by indexing thread to try acquiring pull write lock before bailing out. Ideally bail out scenario should never happen.
   * If it does then either we are too slow in pulling and can tune this value or something else is wrong.
   */
  public static int MAX_ATTEMPTS_INDEXING_PULL_WRITE_LOCK = 10;

  private final CoreContainer cores;
  /**
   * This cache maintains the shared store version the each core is at or ahead of(core has to sometimes be ahead of
   * shared store given indexing first happens locally before being propagated to shared store).
   * todo: need to add eviction strategy.
   */
  private final ConcurrentHashMap<String, SharedCoreVersionMetadata> coresVersionMetadata;

  public SharedCoreConcurrencyController(CoreContainer cores) {
    this.cores = cores;
    coresVersionMetadata = new ConcurrentHashMap<>();
  }

  /**
   * Returns true if {@link SharedCoreVersionMetadata} and {@link SharedShardVersionMetadata} represent the same version; otherwise false
   */
  public boolean areVersionsEqual(SharedCoreVersionMetadata coreVersionMetadata, SharedShardVersionMetadata shardVersionMetadata) {
    boolean isVersionNumberSame = coreVersionMetadata.getVersion() == shardVersionMetadata.getVersion();
    boolean isMetadataSuffixSame = StringUtils.equals(coreVersionMetadata.getMetadataSuffix(), shardVersionMetadata.getMetadataSuffix());

    if (isVersionNumberSame && isMetadataSuffixSame) {
      return true;
    }
    if (isVersionNumberSame || isMetadataSuffixSame) {
      log.warn(String.format("Why only one of version number and metadata suffix matches?" +
              " coreVersionNumber=%s shardVersionNumber=%s" +
              " coreMetadataSuffix=%s shardMetadataSuffix=%s",
          coreVersionMetadata.getVersion(), shardVersionMetadata.getVersion(),
          coreVersionMetadata.getMetadataSuffix(), shardVersionMetadata.getMetadataSuffix()));
    }
    return false;
  }

  /**
   * Logs the current {@link SharedCoreStage} a core is at.
   */
  public void recordState(String collectionName, String shardName, String coreName, SharedCoreStage stage) {
    log.info(String.format("RecordSharedCoreStage: collection=%s shard=%s core=%s stage=%s", collectionName, shardName, coreName, stage));
  }

  /**
   * Returns a {@link ReentrantReadWriteLock} corresponding to the core. It protects pulls from each other and indexing from pulls.
   * A write lock is required whenever pulling contents into a core from shared store.
   * A read lock is required for the whole duration of indexing on the core(including the push to shared store {@link CorePusher#pushCoreToBlob(PushPullData)}.)
   */
  public ReentrantReadWriteLock getCorePullLock(String collectionName, String shardName, String coreName) {
    SharedCoreVersionMetadata coreVersionMetadata = getOrCreateCoreVersionMetadata(collectionName, shardName, coreName);
    return coreVersionMetadata.getCorePullLock();
  }

  /**
   * Returns a {@link ReentrantLock} corresponding to the core. It protects shared store pushes from each other.
   * This lock is required for pushing the core to shared store {@link CorePusher#pushCoreToBlob(PushPullData)}.
   */
  public ReentrantLock getCorePushLock(String collectionName, String shardName, String coreName) {
    SharedCoreVersionMetadata coreVersionMetadata = getOrCreateCoreVersionMetadata(collectionName, shardName, coreName);
    return coreVersionMetadata.getCorePushLock();
  }

  /**
   * Returns {@link SharedCoreVersionMetadata} representing shared store version the core is at or ahead of (core has
   * to sometimes be ahead of shared store given indexing first happens locally before being propagated to shared store).
   */
  public SharedCoreVersionMetadata getCoreVersionMetadata(String collectionName, String shardName, String coreName) {
    return getOrCreateCoreVersionMetadata(collectionName, shardName, coreName);
  }

  /**
   * Updates {@link SharedCoreVersionMetadata} for the core with passed in
   * {@link SharedShardVersionMetadata} and {@link BlobCoreMetadata}
   */
  public void updateCoreVersionMetadata(String collectionName, String shardName, String coreName,
                                        SharedShardVersionMetadata shardVersionMetadata, BlobCoreMetadata blobCoreMetadata) {
    updateCoreVersionMetadata(collectionName, shardName, coreName, shardVersionMetadata, blobCoreMetadata, false);
  }

  /**
   * Updates {@link SharedCoreVersionMetadata} for the core with passed in
   * {@link SharedShardVersionMetadata}, {@link BlobCoreMetadata} and {@code softGuaranteeOfEquality}
   */
  public void updateCoreVersionMetadata(String collectionName, String shardName, String coreName,
                                        SharedShardVersionMetadata shardVersionMetadata, BlobCoreMetadata blobCoreMetadata,
                                        boolean softGuaranteeOfEquality) {
    SharedCoreVersionMetadata currentMetadata = getOrCreateCoreVersionMetadata(collectionName, shardName, coreName);
    SharedCoreVersionMetadata updatedMetadata = currentMetadata.updatedOf(shardVersionMetadata.getVersion(), shardVersionMetadata.getMetadataSuffix(),
        blobCoreMetadata, softGuaranteeOfEquality);
    updateCoreVersionMetadata(collectionName, shardName, coreName, currentMetadata, updatedMetadata);
  }

  /**
   * Updates {@link SharedCoreVersionMetadata} for the core with passed in
   * {@code softGuaranteeOfEquality}
   */
  public void updateCoreVersionMetadata(String collectionName, String shardName, String coreName, boolean softGuaranteeOfEquality) {
    SharedCoreVersionMetadata currentMetadata = getOrCreateCoreVersionMetadata(collectionName, shardName, coreName);
    SharedCoreVersionMetadata updatedMetadata = currentMetadata.updatedOf(softGuaranteeOfEquality);
    updateCoreVersionMetadata(collectionName, shardName, coreName, currentMetadata, updatedMetadata);
  }

  private void updateCoreVersionMetadata(String collectionName, String shardName, String coreName, SharedCoreVersionMetadata currentMetadata, SharedCoreVersionMetadata updatedMetadata) {
    // either have a pull write lock or push lock along with pull read lock
    // TODO: Core split does not acquire these locks because as of current understanding there is no concurrency involved.
    //       But at least for semantic correctness we should acquire locks in that path too and enable this assert.
    // assert (currentMetadata.getCorePullLock().getWriteHoldCount() > 0) ||
    //    (currentMetadata.getCorePushLock().getHoldCount() > 0 && currentMetadata.getCorePullLock().getReadHoldCount() > 0);
    log.info(String.format("updateCoreVersionMetadata: collection=%s shard=%s core=%s  current={%s} updated={%s}",
        collectionName, shardName, coreName, currentMetadata.toString(), updatedMetadata.toString()));
    coresVersionMetadata.put(coreName, updatedMetadata);
  }

  private SharedCoreVersionMetadata getOrCreateCoreVersionMetadata(String collectionName, String shardName, String coreName) {
    SharedCoreVersionMetadata coreVersionMetadata = coresVersionMetadata.get(coreName);
    if (coreVersionMetadata != null) {
      // already present
      return coreVersionMetadata;
    }
    return initializeCoreVersionMetadata(collectionName, shardName, coreName);
  }

  private SharedCoreVersionMetadata initializeCoreVersionMetadata(String collectionName, String shardName, String coreName) {
    // computeIfAbsent to ensure we only do single initialization
    return coresVersionMetadata.computeIfAbsent(coreName, k -> {
      // TODO: This metadata should not be created here. It should only be created on shard creation or zk recovery time.
      ensureShardVersionMetadataNodeExists(collectionName, shardName);
      // a value not to be found as a zk node version
      int version = -1;
      String metadataSuffix = null;
      BlobCoreMetadata blobCoreMetadata = null;
      boolean softGuaranteeOfEquality = false;
      /** Should only be created once at initialization time, subsequent updates should reuse same lock instance
       *  see {@link SharedCoreVersionMetadata#updatedOf(int, String, BlobCoreMetadata, boolean)} and other overload.
       *
       *  We don't need fair ordering policy for this lock, which normally has lower throughput.
       *  We rely on softGuaranteeOfEquality and isLeader at query time so that queries may not contend
       *  on this lock and let the steady state indexing do its job without contention. see  {@link CorePullerThread#run()} for details.
       *
       *  On indexing side we rely on read lock and we can have multiple readers just fine. Write lock is only needed
       *  in a fail over scenario(leader changed) where we need to pull from shared store but that is only needed to be done by one thread.
       *  Therefore we acquire write lock with a timeout and check for that condition after the timeout. Therefore no
       *  concern of starvation there either.
       *  */
      ReentrantReadWriteLock corePullLock = new ReentrantReadWriteLock();
      /** Should only be created once at initialization time, subsequent updates should reuse same lock instance
       *  see {@link SharedCoreVersionMetadata#updatedOf(int, String, BlobCoreMetadata, boolean)} and other overload
       *
       *  We don't need fair ordering policy for this lock, which normally has lower throughput.
       *  see {@link CorePusher#pushCoreToBlob(PushPullData)} for details */
      ReentrantLock corePushLock = new ReentrantLock();
      return new SharedCoreVersionMetadata(version, metadataSuffix, blobCoreMetadata, softGuaranteeOfEquality, corePullLock, corePushLock);
    });
  }

  @VisibleForTesting
  protected void ensureShardVersionMetadataNodeExists(String collectionName, String shardName) {
    SharedShardMetadataController metadataController = cores.getSharedStoreManager().getSharedShardMetadataController();
    try {
      // creates the metadata node if it doesn't exist
      metadataController.ensureMetadataNodeExists(collectionName, shardName);
    } catch (IOException ioe) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          String.format("Unable to ensure metadata for collection=%s shard=%s", collectionName, shardName), ioe);
    }
  }

  /**
   * This represents metadata that need to be cached for a core of a shared collection {@link DocCollection#getSharedIndex()}
   * so that it can be properly synchronized for concurrent indexing, pushes and pulls.
   * <p>
   * The locks only need to be initialized once and can potentially be kept in a separate structure from version information. For that
   * we would need to pay cost for another map in {@link SharedCoreConcurrencyController}. To avoid another map we are keeping locks here
   * and making sure they are not re-initialized.
   */
  public static class SharedCoreVersionMetadata {
    /**
     * Value originating from a ZooKeeper node used to handle conditionally and safely update the
     * core.metadata file written to the shared store.
     */
    private final int version;
    /**
     * Unique value representing the state of shared store with which the core is at least sync with.
     */
    private final String metadataSuffix;
    /**
     * {@link BlobCoreMetadata} representing the state corresponding to {@link #metadataSuffix}
     */
    private final BlobCoreMetadata blobCoreMetadata;
    /**
     * Whether there is a soft guarantee of being in sync with {@link SharedShardVersionMetadata} of the shard.
     * In steady state this guarantee is provided for leader cores when they push
     * {@link CorePusher#pushCoreToBlob(PushPullData)}
     * and pull
     * {@link BlobStoreUtils#syncLocalCoreWithSharedStore(String, String, String, CoreContainer, SharedShardVersionMetadata, boolean)}
     * {@link CorePullTask#pullCoreFromBlob(boolean)} ()}
     * since followers cannot index. In presence of this guarantee we can skip consulting zookeeper before processing an indexing batch.
     */
    private final boolean softGuaranteeOfEquality;
    /**
     * See comments on {@link #getCorePullLock(String, String, String)}
     */
    private final ReentrantReadWriteLock corePullLock;
    /**
     * See comments on {@link #getCorePushLock(String, String, String)}
     */
    private final ReentrantLock corePushLock;

    private SharedCoreVersionMetadata(int version, String metadataSuffix, BlobCoreMetadata blobCoreMetadata,
                                      boolean softGuaranteeOfEquality, ReentrantReadWriteLock corePullLock, ReentrantLock corePushLock) {
      this.version = version;
      this.metadataSuffix = metadataSuffix;
      this.blobCoreMetadata = blobCoreMetadata;
      this.softGuaranteeOfEquality = softGuaranteeOfEquality;
      this.corePullLock = corePullLock;
      this.corePushLock = corePushLock;
    }

    public int getVersion() {
      return version;
    }

    public BlobCoreMetadata getBlobCoreMetadata() {
      return blobCoreMetadata;
    }

    public String getMetadataSuffix() {
      return metadataSuffix;
    }

    public boolean isSoftGuaranteeOfEquality() {
      return softGuaranteeOfEquality;
    }

    private ReentrantReadWriteLock getCorePullLock() {
      return corePullLock;
    }

    private ReentrantLock getCorePushLock() {
      return corePushLock;
    }

    private SharedCoreVersionMetadata updatedOf(int version, String metadataSuffix, BlobCoreMetadata blobCoreMetadata, boolean softGuaranteeOfEquality) {
      return new SharedCoreVersionMetadata(version, metadataSuffix, blobCoreMetadata, softGuaranteeOfEquality, corePullLock, corePushLock);
    }

    private SharedCoreVersionMetadata updatedOf(boolean softGuaranteeOfEquality) {
      return new SharedCoreVersionMetadata(version, metadataSuffix, blobCoreMetadata, softGuaranteeOfEquality, corePullLock, corePushLock);
    }

    @Override
    public String toString() {
      return String.format("version=%s  metadataSuffix=%s softGuaranteeOfEquality=%s", version, metadataSuffix, softGuaranteeOfEquality);
    }
  }

  /**
   * Various stages a core of a shared collection {@link DocCollection#getSharedIndex()} might go through during indexing and querying.
   */
  public enum SharedCoreStage {
    /**
     * Necessary locks have been acquired and we have started to pull from the shared store.
     */
    BLOB_PULL_STARTED,
    /**
     * Pull(either successful or failed) has ended and we are about to release the necessary locks.
     */
    BLOB_PULL_FINISHED,
    /**
     * We have received an indexing batch but necessary locks have not been acquired yet.
     */
    INDEXING_BATCH_RECEIVED,
    /**
     * We are passed the shared store pull stage(if applicable) and are in sync with shared store.
     * Now we will proceed with local indexing.
     */
    LOCAL_INDEXING_STARTED,
    /**
     * Local indexing finished but not have been pushed to shared store.
     */
    LOCAL_INDEXING_FINISHED,
    /**
     * Necessary locks have been acquired and push to shared store has started.
     */
    BLOB_PUSH_STARTED,
    /**
     * Files have been pushed to blob.
     */
    BLOB_PUSHED,
    /**
     * Zookeeper has been successfully updated with new metadata.
     */
    ZK_UPDATE_FINISHED,
    /**
     * Local cache {@link #coresVersionMetadata} has been successfully updated with new metadata.
     */
    LOCAL_CACHE_UPDATE_FINISHED,
    /**
     * Push(either successful or failed) has ended and we are about to release the necessary locks.
     */
    BLOB_PUSH_FINISHED,
    /**
     * Indexing batch(either successful or failed) has ended and we are about to release the necessary locks.
     */
    INDEXING_BATCH_FINISHED
  }
}
