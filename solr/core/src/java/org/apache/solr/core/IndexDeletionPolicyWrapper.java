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
package org.apache.solr.core;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.solr.core.snapshots.SolrSnapshotMetaDataManager;
import org.apache.solr.update.SolrIndexWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper for an IndexDeletionPolicy instance.
 * <p>
 * Provides features for looking up IndexCommit given a version. Allows reserving index
 * commit points for certain amounts of time to support features such as index replication
 * or snapshooting directly out of a live index directory.
 * <p>
 * <b>NOTE</b>: The {@link #clone()} method returns <tt>this</tt> in order to make
 * this {@link IndexDeletionPolicy} instance trackable across {@link IndexWriter}
 * instantiations. This is correct because each core has its own
 * {@link IndexDeletionPolicy} and never has more than one open {@link IndexWriter}.
 *
 * @see org.apache.lucene.index.IndexDeletionPolicy
 */
public final class IndexDeletionPolicyWrapper extends IndexDeletionPolicy {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  private final IndexDeletionPolicy deletionPolicy;
  private volatile Map<Long, IndexCommit> solrVersionVsCommits = new ConcurrentHashMap<>();
  private final Map<Long, Long> reserves = new ConcurrentHashMap<>();
  private volatile IndexCommit latestCommit;
  private final ConcurrentHashMap<Long, AtomicInteger> savedCommits = new ConcurrentHashMap<>();
  private final SolrSnapshotMetaDataManager snapshotMgr;

  public IndexDeletionPolicyWrapper(IndexDeletionPolicy deletionPolicy, SolrSnapshotMetaDataManager snapshotMgr) {
    this.deletionPolicy = deletionPolicy;
    this.snapshotMgr = snapshotMgr;
  }

  /**
   * Gets the most recent commit point
   * <p>
   * It is recommended to reserve a commit point for the duration of usage so that
   * it is not deleted by the underlying deletion policy
   *
   * @return the most recent commit point
   */
  public IndexCommit getLatestCommit() {
    return latestCommit;
  }

  public IndexDeletionPolicy getWrappedDeletionPolicy() {
    return deletionPolicy;
  }

  /**
   * Set the duration for which commit point is to be reserved by the deletion policy.
   *
   * @param indexGen gen of the commit point to be reserved
   * @param reserveTime  time in milliseconds for which the commit point is to be reserved
   */
  public void setReserveDuration(Long indexGen, long reserveTime) {
    long timeToSet = System.nanoTime() + TimeUnit.NANOSECONDS.convert(reserveTime, TimeUnit.MILLISECONDS);
    for(;;) {
      Long previousTime = reserves.put(indexGen, timeToSet);

      // this is the common success case: the older time didn't exist, or
      // came before the new time.
      if (previousTime == null || previousTime <= timeToSet) {
        log.debug("Commit point reservation for generation {} set to {} (requested reserve time of {})",
            indexGen, timeToSet, reserveTime);
        break;
      }

      // At this point, we overwrote a longer reservation, so we want to restore the older one.
      // the problem is that an even longer reservation may come in concurrently
      // and we don't want to overwrite that one too.  We simply keep retrying in a loop
      // with the maximum time value we have seen.
      timeToSet = previousTime;      
    }
  }

  private void cleanReserves() {
    long currentTime = System.nanoTime();
    for (Map.Entry<Long, Long> entry : reserves.entrySet()) {
      if (entry.getValue() < currentTime) {
        reserves.remove(entry.getKey());
      }
    }
  }

  private List<IndexCommitWrapper> wrap(List<? extends IndexCommit> list) {
    List<IndexCommitWrapper> result = new ArrayList<>();
    for (IndexCommit indexCommit : list) result.add(new IndexCommitWrapper(indexCommit));
    return result;
  }

  /** Permanently prevent this commit point from being deleted.
   * A counter is used to allow a commit point to be correctly saved and released
   * multiple times. */
  public synchronized void saveCommitPoint(Long indexCommitGen) {
    AtomicInteger reserveCount = savedCommits.get(indexCommitGen);
    if (reserveCount == null) reserveCount = new AtomicInteger();
    reserveCount.incrementAndGet();
    savedCommits.put(indexCommitGen, reserveCount);
  }

  /** Release a previously saved commit point */
  public synchronized void releaseCommitPoint(Long indexCommitGen) {
    AtomicInteger reserveCount = savedCommits.get(indexCommitGen);
    if (reserveCount == null) return;// this should not happen
    if (reserveCount.decrementAndGet() <= 0) {
      savedCommits.remove(indexCommitGen);
    }
  }

  /**
   * Internal use for Lucene... do not explicitly call.
   */
  @Override
  public void onInit(List<? extends IndexCommit> list) throws IOException {
    List<IndexCommitWrapper> wrapperList = wrap(list);
    deletionPolicy.onInit(wrapperList);
    updateCommitPoints(wrapperList);
    cleanReserves();
  }

  /**
   * Internal use for Lucene... do not explicitly call.
   */
  @Override
  public void onCommit(List<? extends IndexCommit> list) throws IOException {
    List<IndexCommitWrapper> wrapperList = wrap(list);
    deletionPolicy.onCommit(wrapperList);
    updateCommitPoints(wrapperList);
    cleanReserves();
  }

  private class IndexCommitWrapper extends IndexCommit {
    IndexCommit delegate;


    IndexCommitWrapper(IndexCommit delegate) {
      this.delegate = delegate;
    }

    @Override
    public String getSegmentsFileName() {
      return delegate.getSegmentsFileName();
    }

    @Override
    public Collection getFileNames() throws IOException {
      return delegate.getFileNames();
    }

    @Override
    public Directory getDirectory() {
      return delegate.getDirectory();
    }

    @Override
    public void delete() {
      Long gen = delegate.getGeneration();
      Long reserve = reserves.get(gen);
      if (reserve != null && System.nanoTime() < reserve) return;
      if (savedCommits.containsKey(gen)) return;
      if (snapshotMgr.isSnapshotted(gen)) return;
      delegate.delete();
    }

    @Override
    public int getSegmentCount() {
      return delegate.getSegmentCount();
    }

    @Override
    public boolean equals(Object o) {
      return delegate.equals(o);
    }

    @Override
    public int hashCode() {
      return delegate.hashCode();
    }

    @Override
    public long getGeneration() {
      return delegate.getGeneration();
    }

    @Override
    public boolean isDeleted() {
      return delegate.isDeleted();
    }

    @Override
    public Map getUserData() throws IOException {
      return delegate.getUserData();
    }    
  }

  /**
   * @param gen the gen of the commit point
   * @return a commit point corresponding to the given version
   */
  public IndexCommit getCommitPoint(Long gen) {
    return solrVersionVsCommits.get(gen);
  }

  /**
   * Gets the commit points for the index.
   * This map instance may change between commits and commit points may be deleted.
   * It is recommended to reserve a commit point for the duration of usage
   *
   * @return a Map of version to commit points
   */
  public Map<Long, IndexCommit> getCommits() {
    return solrVersionVsCommits;
  }

  private void updateCommitPoints(List<IndexCommitWrapper> list) {
    Map<Long, IndexCommit> map = new ConcurrentHashMap<>();
    for (IndexCommitWrapper wrapper : list) {
      if (!wrapper.isDeleted())
        map.put(wrapper.delegate.getGeneration(), wrapper.delegate);
    }
    solrVersionVsCommits = map;
    if (!list.isEmpty()) {
      latestCommit = ((list.get(list.size() - 1)).delegate);
    }
  }

  public static long getCommitTimestamp(IndexCommit commit) throws IOException {
    final Map<String,String> commitData = commit.getUserData();
    String commitTime = commitData.get(SolrIndexWriter.COMMIT_TIME_MSEC_KEY);
    if (commitTime != null) {
      return Long.parseLong(commitTime);
    } else {
      return 0;
    }
  }
}

