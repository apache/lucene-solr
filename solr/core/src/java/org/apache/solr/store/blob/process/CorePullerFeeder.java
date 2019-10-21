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

import java.lang.invoke.MethodHandles;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.store.blob.client.BlobCoreMetadata;
import org.apache.solr.store.blob.metadata.BlobCoreSyncer;
import org.apache.solr.store.blob.metadata.PushPullData;
import org.apache.solr.store.blob.util.DeduplicatingList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A pull version of {@link CoreSyncFeeder} then will continually ({@link #feedTheMonsters()}) to load up a work queue (
 * {@link #pullTaskQueue}) with such tasks {@link CorePullTask} to keep the created threads busy :) The tasks will be
 * pulled from {@link CorePullTracker} to which Solr code notifies queried cores which are stale locally and need to be
 * fetched from blob.
 */
public class CorePullerFeeder extends CoreSyncFeeder {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected static final String PULLER_THREAD_PREFIX = "puller";

  private static final int numPullerThreads = 5; // TODO : make configurable

  private final CorePullTask.PullCoreCallback callback;

  protected final DeduplicatingList<String, CorePullTask> pullTaskQueue;

  /** Cores unknown locally that got created as part of the pull process but for which no data has been pulled yet
   * from Blob store. If we ignore this transitory state, these cores can be accessed locally and simply look empty.
   * We'd rather treat threads attempting to access such cores like threads attempting to access an unknown core and
   * do a pull (or more likely wait for an ongoing pull to finish).<p>
   *
   * Note, it is the client's responsibility to synchronize accesses
   */
  private final Set<String> coresCreatedNotPulledYet = Sets.newHashSet();

  protected CorePullerFeeder(CoreContainer cores) {
    super(cores, numPullerThreads);
    this.pullTaskQueue = new DeduplicatingList<>(ALMOST_MAX_WORKER_QUEUE_SIZE, new CorePullTask.PullTaskMerger());
    this.callback = new CorePullResult();
  }

  /**
   * Returns a _hint_ that the given core might be locally empty because it is awaiting pull from Blob store.
   * This is just a hint because as soon as the lock is released when the method returns, the status of the core could change.
   */
  public static boolean isEmptyCoreAwaitingPull(CoreContainer cores, String corename) {
    CorePullerFeeder cpf = cores.getSharedStoreManager().getBlobProcessManager().getCorePullerFeeder();
    Set<String> coresCreatedNotPulledYet = cpf.getCoresCreatedNotPulledYet();
    synchronized (coresCreatedNotPulledYet) {
      return coresCreatedNotPulledYet.contains(corename);
    }
  }

  @Override
  public Runnable getSyncer() {
    return new CorePullerThread(this, pullTaskQueue);
  }

  @Override
  String getMonsterThreadName() {
    return PULLER_THREAD_PREFIX;
  }
  
  protected CorePullTask.PullCoreCallback getCorePullTaskCallback() {
    return callback;
  }

  protected Set<String> getCoresCreatedNotPulledYet() {
    return coresCreatedNotPulledYet;
  }

  @Override
  void feedTheMonsters() throws InterruptedException {
    while (cores.getSharedStoreManager() == null) {
      // todo: Fix cyclic initialization sequence
      // if thread starts early it will be killed since the initialization of sharedStoreManager has triggered the
      // creation of this thread and following line will throw NPE.
    }
    CorePullTracker tracker = cores.getSharedStoreManager().getCorePullTracker();
    final long minMsBetweenLogs = 15000;
    long lastLoggedTimestamp = 0L;
    long syncsEnqueuedSinceLastLog = 0; // This is the non-deduped count
    while (shouldContinueRunning()) {
      // This call will block if there are no stale cores queried and nothing to pull
      PullCoreInfo pci = tracker.getCoreToPull();

      // Add the core to the list consumed by the thread doing the actual work
      CorePullTask pt = new CorePullTask(cores, pci, getCorePullTaskCallback(), coresCreatedNotPulledYet);
      pullTaskQueue.addDeduplicated(pt, /* isReenqueue */ false);
      syncsEnqueuedSinceLastLog++;

      // Log if it's time (we did at least one pull otherwise we would be still blocked in the calls above)
      final long now = System.currentTimeMillis();
      final long msSinceLastLog = now - lastLoggedTimestamp;
      if (msSinceLastLog > minMsBetweenLogs) {
        log.info("Since last pull log " + msSinceLastLog + " ms ago, added "
            + syncsEnqueuedSinceLastLog + " cores to pull from blob. Last one is core with "
            + "shared blob name " + pci.getSharedStoreName());
        lastLoggedTimestamp = now;
        syncsEnqueuedSinceLastLog = 0;
      }
    }
  }

  /**
   * Structure with whatever data we need to track on each core we need to pull from Blob store. This will be
   * deduplicated on core name (the same core requiring two pulls from Blob will only be recorded one if the first
   * pull has not been processed yet).
   */
  public static class PullCoreInfo extends PushPullData implements DeduplicatingList.Deduplicatable<String> {

    private final boolean waitForSearcher;
    private final boolean createCoreIfAbsent;

    PullCoreInfo(PushPullData data, boolean createCoreIfAbsent, boolean waitForSearcher) {
      super(data.getCollectionName(), data.getShardName(), data.getCoreName(), data.getSharedStoreName());
      this.waitForSearcher = waitForSearcher;
      this.createCoreIfAbsent = createCoreIfAbsent;
    }

    PullCoreInfo(String collectionName, String shardName, String coreName, String sharedStoreName,
                 boolean createCoreIfAbsent, boolean waitForSearcher) {
      super(collectionName, shardName, coreName, sharedStoreName);
      this.waitForSearcher = waitForSearcher;
      this.createCoreIfAbsent = createCoreIfAbsent;
    }

    @Override
    public String getDedupeKey() {
      return coreName;
    }

    public boolean shouldWaitForSearcher() {
      return waitForSearcher;
    }

    public boolean shouldCreateCoreIfAbsent() {
      return createCoreIfAbsent;
    }
  }

  /**
   * We only want one entry in the list for each shard, so when a second entry arrives, we merge them on 
   * their shared store name
   */
  static class PullCoreInfoMerger implements DeduplicatingList.Merger<String, PullCoreInfo> {
    @Override
    public PullCoreInfo merge(PullCoreInfo v1, PullCoreInfo v2) {
      return mergePullCoreInfos(v1, v2);
    }

    static PullCoreInfo mergePullCoreInfos(PullCoreInfo v1, PullCoreInfo v2) {
      assert v1.getSharedStoreName().equals(v2.getSharedStoreName());
      assert v1.getDedupeKey().equals(v2.getDedupeKey());
      assert v1.getCoreName().equals(v2.getCoreName());

      // Merging the version number here implies an ordering on the pull operation
      // enqueued as we want higher version-ed operations to be what the pulling
      // mechanisms pull off of due to presence of metadataSuffix information
      // Therefore these flags are dependent on which version in either PullCoreInfos 
      // is higher EXCEPT in the case where they are the same
      boolean waitForSearcher = false;
      boolean createCoreIfAbsent = false;

      // if one needs to wait then merged will have to wait as well
      waitForSearcher = v1.waitForSearcher || v2.waitForSearcher;
      // if one wants to create core if absent then merged will have to create as well
      createCoreIfAbsent = v1.createCoreIfAbsent || v2.createCoreIfAbsent;

      return new PullCoreInfo(v1.getCollectionName(), v1.getShardName(), v1.getCoreName(),
          v1.getSharedStoreName(), createCoreIfAbsent, waitForSearcher);
    }
  }

  /**
   * When a {@link CorePullerThread} finishes its work, it's calling an instance of this class.
   */
  private class CorePullResult implements CorePullTask.PullCoreCallback {

    @Override
    public void finishedPull(CorePullTask pullTask, BlobCoreMetadata blobMetadata, CoreSyncStatus status, String message)
        throws InterruptedException {
      try {
        // TODO given for now we consider environment issues as blob/corruption issues, not sure retrying currently makes sense. 
        // See comment in CorePushPull.pullUpdateFromBlob() regarding thrown exception
        PullCoreInfo pullCoreInfo = pullTask.getPullCoreInfo();
        if (status.isTransientError() && pullTask.getAttempts() < MAX_ATTEMPTS) {
          pullTask.setAttempts(pullTask.getAttempts() + 1);
          pullTask.setLastAttemptTimestamp(System.currentTimeMillis());
          pullTaskQueue.addDeduplicated(pullTask, true);
          log.info(String.format("Pulling core %s failed with transient error. Retrying. Last status=%s attempts=%s . %s",
              pullCoreInfo.getSharedStoreName(), status, pullTask.getAttempts(), message == null ? "" : message));
          return;
        }
        
        if (status.isSuccess()) {
          log.info(String.format("Pulling core %s succeeded. Last status=%s attempts=%s . %s",
              pullCoreInfo.getSharedStoreName(), status, pullTask.getAttempts(), message == null ? "" : message));
        } else {
          log.warn(String.format("Pulling core %s failed. Giving up. Last status=%s attempts=%s . %s",
              pullCoreInfo.getSharedStoreName(), status, pullTask.getAttempts(), message == null ? "" : message));
        }
        BlobCoreSyncer syncer = cores.getSharedStoreManager().getBlobCoreSyncer();
        syncer.finishedPull(pullCoreInfo.getSharedStoreName(), status, blobMetadata, message);
      } catch (InterruptedException ie) {
        close();
        throw ie;
      }
    }
  }
}