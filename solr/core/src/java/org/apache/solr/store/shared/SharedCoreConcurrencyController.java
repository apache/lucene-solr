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
     * {@link CorePullTask#pullCoreFromBlob()}
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
    BlobPullStarted,
    /**
     * Pull(either successful or failed) has ended and we are about to release the necessary locks.
     */
    BlobPullFinished,
    /**
     * We have received an indexing batch but necessary locks have not been acquired yet.
     */
    IndexingBatchReceived,
    /**
     * We are passed the shared store pull stage(if applicable) and are in sync with shared store.
     * Now we will proceed with local indexing.
     */
    LocalIndexingStarted,
    /**
     * Local indexing finished but not have been pushed to shared store.
     */
    LocalIndexingFinished,
    /**
     * Necessary locks have been acquired and push to shared store has started.
     */
    BlobPushStarted,
    /**
     * Files have been pushed to blob.
     */
    BlobPushed,
    /**
     * Zookeeper has been successfully updated with new metadata.
     */
    ZkUpdateFinished,
    /**
     * Local cache {@link #coresVersionMetadata} has been successfully updated with new metadata.
     */
    LocalCacheUpdateFinished,
    /**
     * Push(either successful or failed) has ended and we are about to release the necessary locks.
     */
    BlobPushFinished
  }
}
