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
  public final int DOC_COMMIT_DELAY_MS = 250;
  
  // settings, not final so we can change them in testing
  private int docsUpperBound;
  private long timeUpperBound;
  
  private final ScheduledExecutorService scheduler = Executors
      .newScheduledThreadPool(1);
  private ScheduledFuture pending;
  
  // state
  private AtomicLong docsSinceCommit = new AtomicLong(0);
  private AtomicInteger autoCommitCount = new AtomicInteger(0);
  private volatile long lastAddedTime = -1;
  
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
  public synchronized void scheduleCommitWithin(long commitMaxTime) {
    _scheduleCommitWithin(commitMaxTime);
  }
  
  private synchronized void _scheduleCommitWithin(long commitMaxTime) {
    // Check if there is a commit already scheduled for longer then this time
    if (pending != null
        && pending.getDelay(TimeUnit.MILLISECONDS) >= commitMaxTime) {
      pending.cancel(false);
      pending = null;
    }
    
    // schedule a new commit
    if (pending == null) {
      pending = scheduler.schedule(this, commitMaxTime, TimeUnit.MILLISECONDS);
    }
  }
  
  /**
   * Indicate that documents have been added
   */
  public boolean addedDocument(int commitWithin) {
    docsSinceCommit.incrementAndGet();
    lastAddedTime = System.currentTimeMillis();
    boolean triggered = false;
    // maxDocs-triggered autoCommit
    if (docsUpperBound > 0 && (docsSinceCommit.get() > docsUpperBound)) {
      _scheduleCommitWithin(DOC_COMMIT_DELAY_MS);
      triggered = true;
    }
    
    // maxTime-triggered autoCommit
    long ctime = (commitWithin > 0) ? commitWithin : timeUpperBound;

    if (ctime > 0) {
      _scheduleCommitWithin(ctime);
      triggered = true;
    }

    return triggered;
  }
  
  /** Inform tracker that a commit has occurred, cancel any pending commits */
  public void didCommit() {
    if (pending != null) {
      pending.cancel(false);
      pending = null; // let it start another one
    }
    docsSinceCommit.set(0);
  }
  
  /** Inform tracker that a rollback has occurred, cancel any pending commits */
  public void didRollback() {
    if (pending != null) {
      pending.cancel(false);
      pending = null; // let it start another one
    }
    docsSinceCommit.set(0);
  }
  
  /** This is the worker part for the ScheduledFuture **/
  public synchronized void run() {
    long started = System.currentTimeMillis();
    SolrQueryRequest req = new LocalSolrQueryRequest(core,
        new ModifiableSolrParams());
    try {
      CommitUpdateCommand command = new CommitUpdateCommand(req, false);
      command.waitSearcher = waitSearcher;
      command.softCommit = softCommit;
      // no need for command.maxOptimizeSegments = 1; since it is not optimizing
      core.getUpdateHandler().commit(command);
      autoCommitCount.incrementAndGet();
    } catch (Exception e) {
      log.error("auto commit error...");
      e.printStackTrace();
    } finally {
      pending = null;
      req.close();
    }
    
    // check if docs have been submitted since the commit started
    if (lastAddedTime > started) {
      if (docsUpperBound > 0 && docsSinceCommit.get() > docsUpperBound) {
        pending = scheduler.schedule(this, 100, TimeUnit.MILLISECONDS);
      } else if (timeUpperBound > 0) {
        pending = scheduler.schedule(this, timeUpperBound,
            TimeUnit.MILLISECONDS);
      }
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
