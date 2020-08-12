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

import java.lang.invoke.MethodHandles;

import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public final class CommitTracker implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  // scheduler delay for maxDoc-triggered autocommits
  public static final int DOC_COMMIT_DELAY_MS = 1;
  // scheduler delay for maxSize-triggered autocommits
  public static final int SIZE_COMMIT_DELAY_MS = 1;
  
  // settings, not final so we can change them in testing
  private int docsUpperBound;
  private long timeUpperBound;
  private long tLogFileSizeUpperBound;

  // note: can't use ExecutorsUtil because it doesn't have a *scheduled* ExecutorService.
  //  Not a big deal but it means we must take care of MDC logging here.
  private final ScheduledExecutorService scheduler =
      Executors.newScheduledThreadPool(1, new SolrNamedThreadFactory("commitScheduler"));
  @SuppressWarnings({"rawtypes"})
  private ScheduledFuture pending;
  
  // state
  private AtomicLong docsSinceCommit = new AtomicLong(0);
  private AtomicInteger autoCommitCount = new AtomicInteger(0);

  private final SolrCore core;

  private final boolean softCommit;
  private boolean openSearcher;
  private static final boolean WAIT_SEARCHER = true;

  private String name;
  
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

    log.info("{} AutoCommit: {}", name, this);
  }

  public boolean getOpenSearcher() {
    return openSearcher;
  }
  
  public synchronized void close() {
    if (pending != null) {
      pending.cancel(false);
      pending = null;
    }
    scheduler.shutdown();
  }
  
  /** schedule individual commits */
  public void scheduleCommitWithin(long commitMaxTime) {
    _scheduleCommitWithin(commitMaxTime);
  }

  public void cancelPendingCommit() {
    synchronized (this) {
      if (pending != null) {
        boolean canceled = pending.cancel(false);
        if (canceled) {
          pending = null;
        }
      }
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
    synchronized (this) {
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
      pending = scheduler.schedule(this, commitMaxTime, TimeUnit.MILLISECONDS);
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
    synchronized (this) {
      if (pending != null) {
        pending.cancel(false);
        pending = null; // let it start another one
      }
      docsSinceCommit.set(0);
    }
  }
  
  /** This is the worker part for the ScheduledFuture **/
  @Override
  public void run() {
    synchronized (this) {
      // log.info("###start commit. pending=null");
      pending = null;  // allow a new commit to be scheduled
    }

    MDCLoggingContext.setCore(core);
    try (SolrQueryRequest req = new LocalSolrQueryRequest(core, new ModifiableSolrParams())) {
      CommitUpdateCommand command = new CommitUpdateCommand(req, false);
      command.openSearcher = openSearcher;
      command.waitSearcher = WAIT_SEARCHER;
      command.softCommit = softCommit;
      if (core.getCoreDescriptor().getCloudDescriptor() != null
          && core.getCoreDescriptor().getCloudDescriptor().isLeader()
          && !softCommit) {
        command.version = core.getUpdateHandler().getUpdateLog().getVersionInfo().getNewClock();
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
