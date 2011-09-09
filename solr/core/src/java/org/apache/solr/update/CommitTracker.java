package org.apache.solr.update;

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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for tracking autoCommit state.
 * 
 * Note: This is purely an implementation detail of autoCommit and will
 * definitely change in the future, so the interface should not be relied-upon
 * 
 * Note: all access must be synchronized.
 */
final class CommitTracker implements Runnable {
  protected final static Logger log = LoggerFactory.getLogger(CommitTracker.class);
  
  // scheduler delay for maxDoc-triggered autocommits
  public final int DOC_COMMIT_DELAY_MS = 1;
  
  // settings, not final so we can change them in testing
  private int docsUpperBound;
  private long timeUpperBound;
  
  private final ScheduledExecutorService scheduler = Executors
      .newScheduledThreadPool(1);
  private ScheduledFuture pending;
  
  // state
  private AtomicLong docsSinceCommit = new AtomicLong(0);
  private AtomicInteger autoCommitCount = new AtomicInteger(0);

  private final SolrCore core;

  private final boolean softCommit;
  private final boolean waitSearcher;

  private String name;
  
  public CommitTracker(String name, SolrCore core, int docsUpperBound, int timeUpperBound, boolean waitSearcher, boolean softCommit) {
    this.core = core;
    this.name = name;
    pending = null;
    
    this.docsUpperBound = docsUpperBound;
    this.timeUpperBound = timeUpperBound;
    
    this.softCommit = softCommit;
    this.waitSearcher = waitSearcher;

    SolrCore.log.info(name + " AutoCommit: " + this);
  }
  
  public synchronized void close() {
    if (pending != null) {
      pending.cancel(true);
      pending = null;
    }
    scheduler.shutdownNow();
  }
  
  /** schedule individual commits */
  public void scheduleCommitWithin(long commitMaxTime) {
    _scheduleCommitWithin(commitMaxTime);
  }

  private void _scheduleCommitWithin(long commitMaxTime) {
    if (commitMaxTime <= 0) return;
    synchronized (this) {
      if (pending != null && pending.getDelay(TimeUnit.MILLISECONDS) <= commitMaxTime) {
        // There is already a pending commit that will happen first, so
        // nothing else to do here.
        // log.info("###returning since getDelay()==" + pending.getDelay(TimeUnit.MILLISECONDS) + " less than " + commitMaxTime);

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
   */
  public void addedDocument(int commitWithin) {
    // maxDocs-triggered autoCommit.  Use == instead of > so we only trigger once on the way up
    if (docsUpperBound > 0) {
      long docs = docsSinceCommit.incrementAndGet();
      if (docs == docsUpperBound + 1) {
        // reset the count here instead of run() so we don't miss other documents being added
        docsSinceCommit.set(0);
        _scheduleCommitWithin(DOC_COMMIT_DELAY_MS);
      }
    }
    
    // maxTime-triggered autoCommit
    long ctime = (commitWithin > 0) ? commitWithin : timeUpperBound;

    if (ctime > 0) {
      _scheduleCommitWithin(ctime);
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
  public void run() {
    synchronized (this) {
      // log.info("###start commit. pending=null");
      pending = null;  // allow a new commit to be scheduled
    }

    SolrQueryRequest req = new LocalSolrQueryRequest(core,
        new ModifiableSolrParams());
    try {
      CommitUpdateCommand command = new CommitUpdateCommand(req, false);
      command.waitSearcher = waitSearcher;
      command.softCommit = softCommit;
      // no need for command.maxOptimizeSegments = 1; since it is not optimizing

      // we increment this *before* calling commit because it was causing a race
      // in the tests (the new searcher was registered and the test proceeded
      // to check the commit count before we had incremented it.)
      autoCommitCount.incrementAndGet();

      core.getUpdateHandler().commit(command);
    } catch (Exception e) {
      SolrException.log(log, "auto commit error...", e);
    } finally {
      // log.info("###done committing");
      req.close();
    }
  }
  
  // to facilitate testing: blocks if called during commit
  public int getCommitCount() {
    return autoCommitCount.get();
  }
  
  @Override
  public String toString() {
    if (timeUpperBound > 0 || docsUpperBound > 0) {
      return (timeUpperBound > 0 ? ("if uncommited for " + timeUpperBound + "ms; ")
          : "")
          + (docsUpperBound > 0 ? ("if " + docsUpperBound + " uncommited docs ")
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

  void setDocsUpperBound(int docsUpperBound) {
    this.docsUpperBound = docsUpperBound;
  }

  void setTimeUpperBound(long timeUpperBound) {
    this.timeUpperBound = timeUpperBound;
  }
}
