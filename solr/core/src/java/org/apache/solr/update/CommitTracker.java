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
package org.apache.solr.update;

import org.apache.solr.cloud.LeaderElector;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.lang.invoke.MethodHandles;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Helper class for tracking autoCommit state.
 * 
 * Note: This is purely an implementation detail of autoCommit and will
 * definitely change in the future, so the interface should not be relied-upon
 * 
 * Note: all access must be synchronized.
 * 
 * Public for tests.
 */
public final class CommitTracker implements Runnable, Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  // scheduler delay for maxDoc-triggered autocommits
  public static final int DOC_COMMIT_DELAY_MS = 1;
  // scheduler delay for maxSize-triggered autocommits
  public static final int SIZE_COMMIT_DELAY_MS = 1;
  
  // settings, not final so we can change them in testing
  private int docsUpperBound;
  private long timeUpperBound;
  private long tLogFileSizeUpperBound;

  private ReentrantLock lock = new ReentrantLock(true);

  // note: can't use ExecutorsUtil because it doesn't have a *scheduled* ExecutorService.
  //  Not a big deal but it means we must take care of MDC logging here.
  private final ScheduledThreadPoolExecutor scheduler = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(1,
      new SolrNamedThreadFactory("commitScheduler", true));
  @SuppressWarnings({"rawtypes"})
  private volatile ScheduledFuture pending;
  
  // state
  private AtomicLong docsSinceCommit = new AtomicLong(0);
  private AtomicInteger autoCommitCount = new AtomicInteger(0);

  private final SolrCore core;

  private final boolean softCommit;
  private boolean openSearcher;
  private static final boolean WAIT_SEARCHER = true;

  private String name;
  private volatile boolean closed;

  public CommitTracker(String name, SolrCore core, int docsUpperBound, int timeUpperBound, long tLogFileSizeUpperBound,
                       boolean openSearcher, boolean softCommit) {
    this.core = core;
    this.name = name;
    pending = null;
    
    this.docsUpperBound = docsUpperBound;
    this.timeUpperBound = timeUpperBound;
    this.tLogFileSizeUpperBound = tLogFileSizeUpperBound;
    
    this.softCommit = softCommit;
    this.openSearcher = openSearcher;

    scheduler.setRemoveOnCancelPolicy(true);
    scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
    log.info("{} AutoCommit: {}", name, this);
    assert ObjectReleaseTracker.track(this);
  }

  public boolean getOpenSearcher() {
    return openSearcher;
  }
  
  public void close() {

    try {
      lock.lockInterruptibly();
    } catch (InterruptedException e) {
      ParWork.propagateInterrupt(e);
      return;
    }
    try {
      this.closed = true;
      try {
        if (pending != null) {
          pending.cancel(false);
        }
      } catch (NullPointerException e) {
        // okay
      }
      pending = null;
      ParWork.close(scheduler);
    } finally {
      if (lock.isHeldByCurrentThread()) lock.unlock();
    }
    assert ObjectReleaseTracker.release(this);
  }
  
  /** schedule individual commits */
  public void scheduleCommitWithin(long commitMaxTime) {
    _scheduleCommitWithin(commitMaxTime);
  }

  public void cancelPendingCommit() {
    lock.lock();
    try {
      if (pending != null) {
        boolean canceled = pending.cancel(false);
        if (canceled) {
          pending = null;
        }
      }
    } finally {
      lock.unlock();
    }
  }
  
  private void _scheduleCommitWithinIfNeeded(long commitWithin) {
    long ctime = (commitWithin > 0) ? commitWithin : timeUpperBound;

    if (ctime > 0) {
      _scheduleCommitWithin(ctime);
    }
  }

  private void _scheduleCommitWithin(long commitMaxTime) {
    if (commitMaxTime <= 0) return;
    lock.lock();
    try {
      if (pending != null && pending.getDelay(TimeUnit.MILLISECONDS) <= commitMaxTime) {
        // There is already a pending commit that will happen first, so
        // nothing else to do here.
        // log.info("###returning since getDelay()=={} less than {}", pending.getDelay(TimeUnit.MILLISECONDS), commitMaxTime);

        return;
      }

      if (pending != null) {
        // we need to schedule a commit to happen sooner than the existing one,
        // so lets try to cancel the existing one first.
        boolean canceled = pending.cancel(false);
        if (!canceled) {
          // It looks like we can't cancel... it must have just started running!
          // this is possible due to thread scheduling delays and a low commitMaxTime.
          // Nothing else to do since we obviously can't schedule our commit *before*
          // the one that just started running (or has just completed).
          // log.info("###returning since cancel failed");
          return;
        }
      }

      // log.info("###scheduling for " + commitMaxTime);

      // schedule our new commit
      if (!closed) {
        pending = scheduler.schedule(this, commitMaxTime, TimeUnit.MILLISECONDS);
      }
    } finally {
      lock.unlock();
    }
  }
  
  /**
   * Indicate that documents have been added
   * @param commitWithin amount of time (in ms) within which a commit should be scheduled
   */
  public void addedDocument(int commitWithin) {
    addedDocument(commitWithin, -1);
  }

  /**
   * Indicate that documents have been added
   * @param commitWithin amount of time (in ms) within which a commit should be scheduled
   * @param currentTlogSize current tlog size (in bytes). Use -1 if we don't want to check for a max size triggered commit
   */
  public void addedDocument(int commitWithin, long currentTlogSize) {
    // maxDocs-triggered autoCommit
    _scheduleMaxDocsTriggeredCommitIfNeeded();

    // maxTime-triggered autoCommit
    _scheduleCommitWithinIfNeeded(commitWithin);

    // maxSize-triggered autoCommit
    _scheduleMaxSizeTriggeredCommitIfNeeded(currentTlogSize);
  }

  /**
   * If a doc size upper bound is set, and the current number of documents has exceeded it, then
   * schedule a commit and reset the counter
   */
  private void _scheduleMaxDocsTriggeredCommitIfNeeded() {
    // Use == instead of > so we only trigger once on the way up
    if (docsUpperBound > 0) {
      long docs = docsSinceCommit.incrementAndGet();
      if (docs == docsUpperBound + 1) {
        // reset the count here instead of run() so we don't miss other documents being added
        docsSinceCommit.set(0);
        _scheduleCommitWithin(DOC_COMMIT_DELAY_MS);
      }
    }
  }
  
  /** 
   * Indicate that documents have been deleted
   */
  public void deletedDocument( int commitWithin ) {
    _scheduleCommitWithinIfNeeded(commitWithin);
  }

  /**
   * If the given current tlog size is greater than the file size upper bound, then schedule a commit
   * @param currentTlogSize current tlog size (in bytes)
   */
  public void scheduleMaxSizeTriggeredCommitIfNeeded(long currentTlogSize) {
    _scheduleMaxSizeTriggeredCommitIfNeeded(currentTlogSize);
  }

  /**
   * If the given current tlog size is greater than the file size upper bound, then schedule a commit
   * @param currentTlogSize current tlog size (in bytes)
   */
  private void _scheduleMaxSizeTriggeredCommitIfNeeded(long currentTlogSize) {
    if (tLogFileSizeUpperBound > 0 && currentTlogSize > tLogFileSizeUpperBound) {
      docsSinceCommit.set(0);
      _scheduleCommitWithin(SIZE_COMMIT_DELAY_MS);
    }
  }

  
  /** Inform tracker that a commit has occurred */
  public void didCommit() {
  }
  
  /** Inform tracker that a rollback has occurred, cancel any pending commits */
  public void didRollback() {
    lock.lock();
    try {
      if (pending != null) {
        pending.cancel(false);
        pending = null; // let it start another one
      }
      docsSinceCommit.set(0);
    } finally {
      lock.unlock();
    }
  }
  
  /** This is the worker part for the ScheduledFuture **/
  @Override
  public void run() {
    lock.lock();
    try {
      // log.info("###start commit. pending=null");
      pending = null;  // allow a new commit to be scheduled
    } finally {
      lock.unlock();
    }

    MDCLoggingContext.setCoreName(core.getName());
    try (SolrQueryRequest req = new LocalSolrQueryRequest(core, new ModifiableSolrParams())) {
      CommitUpdateCommand command = new CommitUpdateCommand(req, false);
      command.openSearcher = openSearcher;
      command.waitSearcher = WAIT_SEARCHER;
      command.softCommit = softCommit;
      boolean isLeader = false;
      if (core.getCoreContainer().isZooKeeperAware()) {
        LeaderElector elector = core.getCoreContainer().getZkController().getLeaderElector(core.getName());
        if (elector != null && elector.isLeader()) {
          isLeader = true;
        }

        if (core.getCoreDescriptor().getCloudDescriptor() != null && isLeader) {
          command.version = core.getUpdateHandler().getUpdateLog().getVersionInfo().getNewClock();
        }
      }
      // no need for command.maxOptimizeSegments = 1; since it is not optimizing

      // we increment this *before* calling commit because it was causing a race
      // in the tests (the new searcher was registered and the test proceeded
      // to check the commit count before we had incremented it.)
      autoCommitCount.incrementAndGet();

      core.getUpdateHandler().commit(command);
    } catch (Exception e) {
      SolrException.log(log, "auto commit error...", e);
    } finally {
      MDCLoggingContext.clear();
    }
    // log.info("###done committing");
  }
  
  // to facilitate testing: blocks if called during commit
  public int getCommitCount() {
    return autoCommitCount.get();
  }
  
  @Override
  public String toString() {
    if (timeUpperBound > 0 || docsUpperBound > 0 || tLogFileSizeUpperBound > 0) {
      return (timeUpperBound > 0 ? ("if uncommitted for " + timeUpperBound + "ms; ")
          : "")
          + (docsUpperBound > 0 ? ("if " + docsUpperBound + " uncommitted docs; ")
              : "")
          + (tLogFileSizeUpperBound > 0 ? String.format(Locale.ROOT, "if tlog file size has exceeded %d bytes",
          tLogFileSizeUpperBound)
            : "");
    } else {
      return "disabled";
    }
  }

  public long getTimeUpperBound() {
    return timeUpperBound;
  }

  int getDocsUpperBound() {
    return docsUpperBound;
  }

  long getTLogFileSizeUpperBound() {
    return tLogFileSizeUpperBound;
  }

  void setDocsUpperBound(int docsUpperBound) {
    this.docsUpperBound = docsUpperBound;
  }

  // only for testing - not thread safe
  public void setTimeUpperBound(long timeUpperBound) {
    this.timeUpperBound = timeUpperBound;
  }

  // only for testing - not thread safe
  public void setTLogFileSizeUpperBound(int sizeUpperBound) {
    this.tLogFileSizeUpperBound = sizeUpperBound;
  }
  
  // only for testing - not thread safe
  public void setOpenSearcher(boolean openSearcher) {
    this.openSearcher = openSearcher;
  }

  // only for testing - not thread safe
  public boolean hasPending() {
    return (null != pending && !pending.isDone());
  }
}
