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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;
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
  private final SolrSnapshotMetaDataManager snapshotMgr;

  /**
   * The set of all known commits <em>after</em> the last completed call to {@link #onInit} or 
   * {@link #onCommit} on our {@link #getWrappedDeletionPolicy()}.
   * 
   * <p>
   * This map is atomically replaced by {@linke #updateKnownCommitPoints}.  
   * The keys are the {@link IndexCommit#getGeneration} of each commit
   * </p>
   *
   * @see #getAndSaveCommitPoint
   * @see #getCommits
   * @see #updateKnownCommitPoints
   */
  private volatile Map<Long, IndexCommit> knownCommits = new ConcurrentHashMap<>();

  /**
   * The most recent commit included in call to {@link #onInit} or 
   * {@link #onCommit} <em>beore</em> delegating to our {@link #getWrappedDeletionPolicy()}.
   *
   * <p>
   * <b>NOTE:</b> This may be null if there is not yet a single commit to our index.
   * </p>
   * 
   * <p>
   * This commit is implicitly protected from deletion in {@link IndexCommitWrapper#delete}
   * </p>
   * 
   * @see #getLatestCommit
   * @see #updateLatestCommit
   */
  private volatile IndexCommit latestCommit;

  /**
   * The set of all commit generations htat have been reserved for for some amount of time.
   * <p>
   * The keys of the {@link IndexCommit#getGeneration} of a commit, the values are the 
   * {@link System#nanoTime} that the commit should be reserved until.
   * </p>
   * 
   * @see #setReserveDuration
   * @see #cleanReserves
   */
  private final Map<Long, Long> reserves = new ConcurrentHashMap<>();

  /**
   * The set of all commit generations that have been saved until explicitly released
   * <p>
   * The keys of the {@link IndexCommit#getGeneration} of a commit, the values are 
   * a reference count of the number of callers who have "saved" this commit.  
   * {@link #releaseCommitPoint} automatically removes mappings once the ref count reaches 0.
   * </p>
   * 
   * @see #getAndSaveLatestCommit
   * @see #saveCommitPoint
   * @see #releaseCommitPoint
   */
  private final Map<Long, AtomicInteger> savedCommits = new ConcurrentHashMap<>();

  public IndexDeletionPolicyWrapper(IndexDeletionPolicy deletionPolicy, SolrSnapshotMetaDataManager snapshotMgr) {
    this.deletionPolicy = deletionPolicy;
    this.snapshotMgr = snapshotMgr;
  }

  /**
   * Returns the most recent commit point.
   * <p>
   * <b>NOTE:</b> This method makes no garuntee that the commit returned still exists as the 
   * moment this method completes.  Callers are encouraged to use {@link #getAndSaveLatestCommit} instead.
   * </p>
   *
   * @return the most recent commit point, or null if there have not been any commits
   * @see #getAndSaveLatestCommit
   */
  public IndexCommit getLatestCommit() {
    return latestCommit;
  }
  
  /**
   * Atomically Saves (via reference counting) &amp; Returns the most recent commit point.
   * <p>
   * If the return value is non-null, then the caller <em>MUST</em> call {@link #releaseCommitPoint} 
   * when finished using it in order to decrement the reference count, or the commit will be preserved 
   * in the Directory forever.
   * </p>
   *
   * @return the most recent commit point, or null if there have not been any commits
   * @see #saveCommitPoint
   * @see #releaseCommitPoint
   */
  public synchronized IndexCommit getAndSaveLatestCommit() {
    final IndexCommit commit = getLatestCommit();
    if (null != commit) {
      saveCommitPoint(commit.getGeneration());
    }
    return commit;
  }

  /**
   * Atomically Saves (via reference counting) &amp; Returns the specified commit if available.
   * <p>
   * If the return value is non-null, then the caller <em>MUST</em> call {@link #releaseCommitPoint} 
   * when finished using it in order to decrement the reference count, or the commit will be preserved 
   * in the Directory forever.
   * </p>
   *
   * @return the commit point with the specified generation, or null if not available
   * @see #saveCommitPoint
   * @see #releaseCommitPoint
   */
  public synchronized IndexCommit getAndSaveCommitPoint(Long generation) {
    if (null == generation) {
      throw new NullPointerException("generation to get and save must not be null");
    }
    final IndexCommit commit = knownCommits.get(generation);
    if ( (null != commit && false != commit.isDeleted())
         || (null == commit && null != latestCommit && generation < latestCommit.getGeneration()) ) {
      throw new IllegalStateException
        ("Specified index generation is too old to be saved: " + generation);
    }
    final AtomicInteger refCount
      = savedCommits.computeIfAbsent(generation, s -> { return new AtomicInteger(); });
    final int currentCount = refCount.incrementAndGet();
    log.debug("Saving generation={}, refCount={}", generation, currentCount);
    return commit;
  }
  
  public IndexDeletionPolicy getWrappedDeletionPolicy() {
    return deletionPolicy;
  }

  /**
   * Set the duration for which commit point is to be reserved by the deletion policy.
   * <p>
   * <b>NOTE:</b> This method does not make any garuntees that the specified index generation exists, 
   * or that the specified generation has not already ben deleted.  The only garuntee is that 
   * <em>if</em> the specified generation exists now, or is created at some point in the future, then 
   * it will be resered for <em>at least</em> the specified <code>reserveTime</code>.
   * </p>
   *
   * @param indexGen gen of the commit point to be reserved
   * @param reserveTime durration in milliseconds (relative to 'now') for which the commit point is to be reserved
   */
  public void setReserveDuration(Long indexGen, long reserveTime) {
    // since 'reserves' is a concurrent HashMap, we don't need to synchronize this method as long as all
    // operations on 'reserves' are done atomically.
    //
    // Here we'll use Map.merge to ensure that we atomically replace any existing timestamp if
    // and only if our new reservation timetsamp is larger.
    final long reserveAsNanoTime
      = System.nanoTime() + TimeUnit.NANOSECONDS.convert(reserveTime, TimeUnit.MILLISECONDS);
    reserves.merge(indexGen, reserveAsNanoTime, BinaryOperator.maxBy(Comparator.naturalOrder()));
  }

  private void cleanReserves() {
    final long currentNanoTime = System.nanoTime();
    // use removeIf to ensure we're removing "old" entries atomically
    reserves.entrySet().removeIf(e -> e.getValue() < currentNanoTime);
  }

  private List<IndexCommitWrapper> wrap(List<? extends IndexCommit> list) {
    List<IndexCommitWrapper> result = new ArrayList<>();
    for (IndexCommit indexCommit : list) result.add(new IndexCommitWrapper(indexCommit));
    return result;
  }

  /** 
   * Permanently prevent this commit point from being deleted (if it has not already) using a refrence count.
   * <p> 
   * <b>NOTE:</b> Callers <em>MUST</em> call {@link #releaseCommitPoint} when finished using it
   * in order to decrement the reference count, or the commit will be preserved in the Directory forever.
   * </p>
   *
   * @param generation the generation of the IndexComit to save until released
   * @see #getAndSaveLatestCommit
   * @see #getAndSaveCommitPoint
   * @see #releaseCommitPoint
   * @throws IllegalStateException if generation is already too old to be saved
   */
  public synchronized void saveCommitPoint(Long generation) {
    getAndSaveCommitPoint(generation); // will handle the logic for us, just ignore the results
  }

  /** 
   * Release a previously saved commit point.
   * <p>
   * This is a convinience wrapper around {@link #releaseCommitPoint(Long)} that will ignore null input.
   * </p>
   */
  public synchronized void releaseCommitPoint(IndexCommit commit) {
    if (null != commit) {
      releaseCommitPoint(commit.getGeneration());
    }
  }
  
  /** 
   * Release a previously saved commit point.
   * <p>
   * This method does not enforce that that the specified generation has previously been saved, 
   * or even that it's 'non-null'.  But if both are true then it will decrement the reference 
   * count for the specified generation.
   * </p>
   */
  public synchronized void releaseCommitPoint(Long generation) {
    if (null == generation) {
      return;
    }
    final AtomicInteger refCount = savedCommits.get(generation);
    if (null != refCount) { // shouldn't happen if balanced save/release calls in callers
      final int currentCount = refCount.decrementAndGet();
      log.debug("Released generation={}, refCount={}", generation, currentCount);
      if (currentCount <= 0) {
        savedCommits.remove(generation); // counter no longer needed;
      }
    }
  }

  /**
   * Internal use for Lucene... do not explicitly call.
   * <p>
   * This Impl passes the list of commits to the delegate Policy <em>AFTER</em> wrapping each 
   * commit in a proxy class that only proxies {@link IndexCommit#delete} if they are not already saved.
   * </p>
   */
  @Override
  public void onInit(List<? extends IndexCommit> list) throws IOException {
    List<IndexCommitWrapper> wrapperList = wrap(list);
    updateLatestCommit(wrapperList);
    deletionPolicy.onInit(wrapperList);
    updateKnownCommitPoints(wrapperList);
    cleanReserves();
  }

  /**
   * Internal use for Lucene... do not explicitly call.
   * <p>
   * This Impl passes the list of commits to the delegate Policy <em>AFTER</em> wrapping each 
   * commit in a proxy class that only proxies {@link IndexCommit#delete} if they are not already saved.
   * </p>
   */
  @Override
  public void onCommit(List<? extends IndexCommit> list) throws IOException {
    List<IndexCommitWrapper> wrapperList = wrap(list);
    updateLatestCommit(wrapperList);
    deletionPolicy.onCommit(wrapperList);
    updateKnownCommitPoints(wrapperList);
    cleanReserves();
  }

  /** 
   * Wrapper class that synchronizes the {@link IndexCommit#delete} calls and only passes 
   * them to the wrapped commit if they should not be saved or reserved.
   */
  private class IndexCommitWrapper extends IndexCommit {
    final IndexCommit delegate;

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
      // Box it now to prevent multiple autoboxing when doing multiple map lookups
      final Long gen = delegate.getGeneration();

      // synchronize on the policy wrapper so that we don't delegate the delete call
      // concurrently with another thread trying to save this commit
      synchronized (IndexDeletionPolicyWrapper.this) {
        if ( (System.nanoTime() < reserves.getOrDefault(gen, 0L)) ||
             savedCommits.containsKey(gen) ||
             snapshotMgr.isSnapshotted(gen) ||
             (null != latestCommit && gen.longValue() == latestCommit.getGeneration()) ) {
          return; // skip deletion
        }
        log.debug("Deleting generation={}", gen);
        delegate.delete(); // delegate deletion
      }
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
      synchronized (IndexDeletionPolicyWrapper.this) {
        return delegate.isDeleted();
      }
    }

    @Override
    public Map getUserData() throws IOException {
      return delegate.getUserData();
    }    
  }

  /**
   * Returns the commit with the specified generation <em>if it is known</em>.
   * <p>
   * <b>NOTE:</b> This method makes no garuntee that the commit returned still exists as the 
   * moment this method completes.  Callers are encouraged to use {@link #getAndSaveLatestCommit} instead.
   * </p>
   * 
   * @param gen the generation of the commit point requested
   * @return a commit point corresponding to the given version if available, or null if not yet created or already deleted
   * @deprecated use {@link #getAndSaveCommitPoint} instead
   */
  @Deprecated
  public IndexCommit getCommitPoint(Long gen) {
    return knownCommits.get(gen);
  }

  /**
   * Returns a Map of all currently known commits, keyed by their generation.
   * <p>
   * <b>NOTE:</b> This map instance may change between commits and commit points may be deleted.
   * This API is intended for "informational purposes" only, to provide an "at the moment" view of 
   * the current list of known commits.  Callers that need to ensure commits exist for an extended period
   * must wrap this call and all subsequent usage of the results in a synchornization block.
   * </p>
   *
   * @return a Map of generation to commit points
   */
  public Map<Long, IndexCommit> getCommits() {
    return Collections.unmodifiableMap(knownCommits);
  }

  /**
   * Updates {@link #latestCommit}.
   * <p>
   * This is handled special, and not included in {@link #updateKnownCommitPoints}, because we need to 
   * ensure this happens <em>before</em> delegating calls to {@link #onInit} or {@link #onCommit} to our 
   * inner Policy.  Doing this ensures that we can always protect {@link #latestCommit} from being deleted.  
   * </p>
   * <p>
   * If we did not do this, and waited to update <code>latestCommit</code> in 
   * <code>updateKnownCommitPoints()</code> then we would need to wrap synchronization completley around 
   * the (delegated) <code>onInit()</code> and <code>onCommit()</code> calls, to ensure there was no 
   * window of time when {@link #getAndSaveLatestCommit} might return the "old" latest commit, after our 
   * delegate Policy had already deleted it.
   * </p>
   * <p>
   * (Since Saving/Reserving (other) commits is handled indirectly ("by reference") via the generation
   * callers can still safely (try) to reserve "old" commits using an explicit generation since 
   * {@link IndexCommitWrapper#delete} is synchornized on <code>this</code>)
   *
   * @see #latestCommit
   * @see #updateKnownCommitPoints
   */
  private synchronized void updateLatestCommit(final List<IndexCommitWrapper> list) {
    // NOTE: There's a hypothetical, not neccessarily possible/plausible, situation that
    // could lead to this combination of updateLatestCommit + updateKnownCommitPoints not
    // being as thread safe as completley synchornizing in onInit/onCommit...
    //  - knownCommits==(1, 2, 3, 4), latestCommit==4
    //  - onCommit(1, 2, 3, 4, 5, 6, 7) - we immediately update latestCommit=7
    //    - before knownCommits is updated, some client calls getAndSaveCommitPoint(6)
    //      - call fails "too old to be saved" even though it's in flight
    // (this assumes some future caller/use-case that doesn't currently exist)
    //
    // The upside of this current approach, and not completley synchornizing onInit/onCommit
    // is that we have no control over what delegate is used, or how long those calls might take.
    //
    // If the hypotehtical situation above ever becomes problematic, then an alternative approach might be
    // to *add* to the Set/Map of all known commits *before* delegating, then *remove* everything except
    // the new (non-deleted) commits *after* delegating.

    assert null != list;
    if (list.isEmpty()) {
      return;
    }
    final IndexCommitWrapper newLast = list.get(list.size() - 1);
    assert ! newLast.isDeleted()
      : "Code flaw: Last commit already deleted, call this method before delegating onCommit/onInit";

    latestCommit = newLast.delegate;
  }


  
  /**
   * Updates the state of all "current" commits.
   * <p>
   * This method is safe to call <em>after</em> delegating to ou inner <code>IndexDeletionPolicy</code>
   * (w/o synchornizing the delegate calls) because even if the delegate decides to 
   * {@link IndexCommit#delete} a commit that a concurrent thread may wish to reserve/save, 
   * that {@link IndexCommitWrapper} will ensure that call is synchronized.  
   * </p>
   * 
   * @see #updateLatestCommit
   */
  private synchronized void updateKnownCommitPoints(final List<IndexCommitWrapper> list) {
    assert null != list;
    assert (list.isEmpty() || null != latestCommit) : "Code flaw: How is latestCommit not set yet?";
    assert (null == latestCommit || ! latestCommit.isDeleted())
      : "Code flaw: How did the latestCommit get set but deleted?";
    assert (list.isEmpty() || latestCommit == list.get(list.size() - 1).delegate)
      : "Code flaw, updateLatestCommit() should have already been called";
    
    final Map<Long, IndexCommit> map = new ConcurrentHashMap<>();
    for (IndexCommitWrapper wrapper : list) {
      if (!wrapper.isDeleted()) {
        map.put(wrapper.delegate.getGeneration(), wrapper.delegate);
      }
    }
    knownCommits = map;
  }

  /**
   * Helper method for unpacking the timestamp infor from the user data 
   * @see SolrIndexWriter#COMMIT_TIME_MSEC_KEY
   * @see IndexCommit#getUserData
   */
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

