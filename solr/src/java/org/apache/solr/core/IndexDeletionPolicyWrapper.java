package org.apache.solr.core;
/**
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

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A wrapper for an IndexDeletionPolicy instance.
 * <p/>
 * Provides features for looking up IndexCommit given a version. Allows reserving index
 * commit points for certain amounts of time to support features such as index replication
 * or snapshooting directly out of a live index directory.
 *
 *
 * @see org.apache.lucene.index.IndexDeletionPolicy
 */
public class IndexDeletionPolicyWrapper implements IndexDeletionPolicy {
  private final IndexDeletionPolicy deletionPolicy;
  private volatile Map<Long, IndexCommit> solrVersionVsCommits = new ConcurrentHashMap<Long, IndexCommit>();
  private final Map<Long, Long> reserves = new ConcurrentHashMap<Long,Long>();
  private volatile IndexCommit latestCommit;
  private final ConcurrentHashMap<Long, AtomicInteger> savedCommits = new ConcurrentHashMap<Long, AtomicInteger>();

  public IndexDeletionPolicyWrapper(IndexDeletionPolicy deletionPolicy) {
    this.deletionPolicy = deletionPolicy;
  }

  /**
   * Gets the most recent commit point
   * <p/>
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
   * @param indexVersion version of the commit point to be reserved
   * @param reserveTime  time in milliseconds for which the commit point is to be reserved
   */
  public void setReserveDuration(Long indexVersion, long reserveTime) {
    long timeToSet = System.currentTimeMillis() + reserveTime;
    for(;;) {
      Long previousTime = reserves.put(indexVersion, timeToSet);

      // this is the common success case: the older time didn't exist, or
      // came before the new time.
      if (previousTime == null || previousTime <= timeToSet) break;

      // At this point, we overwrote a longer reservation, so we want to restore the older one.
      // the problem is that an even longer reservation may come in concurrently
      // and we don't want to overwrite that one too.  We simply keep retrying in a loop
      // with the maximum time value we have seen.
      timeToSet = previousTime;      
    }
  }

  private void cleanReserves() {
    long currentTime = System.currentTimeMillis();
    for (Map.Entry<Long, Long> entry : reserves.entrySet()) {
      if (entry.getValue() < currentTime) {
        reserves.remove(entry.getKey());
      }
    }
  }

  private List<IndexCommitWrapper> wrap(List<IndexCommit> list) {
    List<IndexCommitWrapper> result = new ArrayList<IndexCommitWrapper>();
    for (IndexCommit indexCommit : list) result.add(new IndexCommitWrapper(indexCommit));
    return result;
  }

  /** Permanently prevent this commit point from being deleted.
   * A counter is used to allow a commit point to be correctly saved and released
   * multiple times. */
  public synchronized void saveCommitPoint(Long indexCommitVersion) {
    AtomicInteger reserveCount = savedCommits.get(indexCommitVersion);
    if (reserveCount == null) reserveCount = new AtomicInteger();
    reserveCount.incrementAndGet();
    savedCommits.put(indexCommitVersion, reserveCount);
  }

  /** Release a previously saved commit point */
  public synchronized void releaseCommitPoint(Long indexCommitVersion) {
    AtomicInteger reserveCount = savedCommits.get(indexCommitVersion);
    if (reserveCount == null) return;// this should not happen
    if (reserveCount.decrementAndGet() <= 0) {
      savedCommits.remove(indexCommitVersion);
    }
  }


  /**
   * Internal use for Lucene... do not explicitly call.
   */
  public void onInit(List list) throws IOException {
    List<IndexCommitWrapper> wrapperList = wrap(list);
    deletionPolicy.onInit(wrapperList);
    updateCommitPoints(wrapperList);
    cleanReserves();
  }

  /**
   * Internal use for Lucene... do not explicitly call.
   */
  public void onCommit(List list) throws IOException {
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
      Long version = delegate.getVersion();
      Long reserve = reserves.get(version);
      if (reserve != null && System.currentTimeMillis() < reserve) return;
      if(savedCommits.containsKey(version)) return;
      delegate.delete();
    }

    @Override
    public boolean isOptimized() {
      return delegate.isOptimized();
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
    public long getVersion() {
      return delegate.getVersion();
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
    public long getTimestamp() throws IOException {
      return delegate.getTimestamp();
    }

    @Override
    public Map getUserData() throws IOException {
      return delegate.getUserData();
    }    
  }

  /**
   * @param version the version of the commit point
   * @return a commit point corresponding to the given version
   */
  public IndexCommit getCommitPoint(Long version) {
    return solrVersionVsCommits.get(version);
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
    Map<Long, IndexCommit> map = new ConcurrentHashMap<Long, IndexCommit>();
    for (IndexCommitWrapper wrapper : list) {
      if (!wrapper.isDeleted())
        map.put(wrapper.getVersion(), wrapper.delegate);
    }
    solrVersionVsCommits = map;
    latestCommit = ((list.get(list.size() - 1)).delegate);
  }
}

