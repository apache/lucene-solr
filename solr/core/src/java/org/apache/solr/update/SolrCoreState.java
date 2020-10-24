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

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.Sort;
import org.apache.solr.cloud.ActionThrottle;
import org.apache.solr.cloud.RecoveryStrategy;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.util.RefCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;

/**
 * The state in this class can be easily shared between SolrCores across
 * SolrCore reloads.
 * 
 */
public abstract class SolrCoreState {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final int PAUSE_UPDATES_TIMEOUT_MILLIS = Integer.getInteger("solr.cloud.wait-for-updates-on-shutdown-millis", 2500);

  protected boolean closed = false;
  private final Object updateLock = new Object();
  private final Object reloadLock = new Object();

  /**
   * If true then all update requests will be refused
   */
  private final AtomicBoolean pauseUpdateRequests = new AtomicBoolean();

  /**
   * Phaser is used to track in flight update requests and can be used
   * to wait for all in-flight requests to finish. A Phaser terminates
   * automatically when the number of registered parties reach zero.
   * Since we track requests with this phaser, we disable the automatic
   * termination by overriding the onAdvance method to return false.
   *
   * @see #registerInFlightUpdate()
   * @see #deregisterInFlightUpdate()
   * @see #pauseUpdatesAndAwaitInflightRequests()
   */
  private final Phaser inflightUpdatesCounter = new Phaser()  {
    @Override
    protected boolean onAdvance(int phase, int registeredParties) {
      // disable termination of phaser
      return false;
    }
  };

  public Object getUpdateLock() {
    return updateLock;
  }
  
  public Object getReloadLock() {
    return reloadLock;
  }
  
  
  private int solrCoreStateRefCnt = 1;

  public void increfSolrCoreState() {
    synchronized (this) {
      if (solrCoreStateRefCnt == 0) {
        throw new CoreIsClosedException("IndexWriter has been closed");
      }
      solrCoreStateRefCnt++;
    }
  }
  
  public boolean decrefSolrCoreState(IndexWriterCloser closer) {
    boolean close = false;
    synchronized (this) {
      solrCoreStateRefCnt--;
      assert solrCoreStateRefCnt >= 0;
      if (solrCoreStateRefCnt == 0) {
        closed = true;
        close = true;
      }
    }
    
    if (close) {
      try {
        log.debug("Closing SolrCoreState");
        close(closer);
      } catch (Exception e) {
        log.error("Error closing SolrCoreState", e);
      }
    }
    return close;
  }

  /**
   * Pauses all update requests to this core and waits (indefinitely) for all in-flight
   * update requests to finish
   */
  public void pauseUpdatesAndAwaitInflightRequests() throws TimeoutException, InterruptedException {
    if (pauseUpdateRequests.compareAndSet(false, true)) {
      int arrivalNumber = inflightUpdatesCounter.register();
      assert arrivalNumber >= 0 : "Registration of in-flight request should have succeeded but got arrival phase number < 0";
      inflightUpdatesCounter.awaitAdvanceInterruptibly(inflightUpdatesCounter.arrive(), PAUSE_UPDATES_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Registers in-flight update requests to this core.
   *
   * @return true if request was registered, false if update requests are paused
   */
  public boolean registerInFlightUpdate() {
    if (pauseUpdateRequests.get()) {
      return false;
    }
    inflightUpdatesCounter.register();
    return true;
  }

  /**
   * De-registers in-flight update requests to this core (marks them as completed)
   */
  public void deregisterInFlightUpdate() {
    int arrivalPhaseNumber = inflightUpdatesCounter.arriveAndDeregister();
    assert arrivalPhaseNumber >= 0 : "inflightUpdatesCounter should not have been terminated";
  }

  public abstract Lock getCommitLock();
  
  /**
   * Force the creation of a new IndexWriter using the settings from the given
   * SolrCore.
   * 
   * @param rollback close IndexWriter if false, else rollback
   * @throws IOException If there is a low-level I/O error.
   */
  public abstract void newIndexWriter(SolrCore core, boolean rollback) throws IOException;
  
  
  /**
   * Expert method that closes the IndexWriter - you must call {@link #openIndexWriter(SolrCore)}
   * in a finally block after calling this method.
   * 
   * @param core that the IW belongs to
   * @param rollback true if IW should rollback rather than close
   * @throws IOException If there is a low-level I/O error.
   */
  public abstract void closeIndexWriter(SolrCore core, boolean rollback) throws IOException;
  
  /**
   * Expert method that opens the IndexWriter - you must call {@link #closeIndexWriter(SolrCore, boolean)}
   * first, and then call this method in a finally block.
   * 
   * @param core that the IW belongs to
   * @throws IOException If there is a low-level I/O error.
   */
  public abstract void openIndexWriter(SolrCore core) throws IOException;
  
  /**
   * Get the current IndexWriter. If a new IndexWriter must be created, use the
   * settings from the given {@link SolrCore}.
   * 
   * @throws IOException If there is a low-level I/O error.
   */
  public abstract RefCounted<IndexWriter> getIndexWriter(SolrCore core) throws IOException;
  
  /**
   * Rollback the current IndexWriter. When creating the new IndexWriter use the
   * settings from the given {@link SolrCore}.
   * 
   * @throws IOException If there is a low-level I/O error.
   */
  public abstract void rollbackIndexWriter(SolrCore core) throws IOException;
  
  /**
   * Get the current Sort of the current IndexWriter's MergePolicy..
   *
   * @throws IOException If there is a low-level I/O error.
   */
  public abstract Sort getMergePolicySort() throws IOException;

  /**
   * @return the {@link DirectoryFactory} that should be used.
   */
  public abstract DirectoryFactory getDirectoryFactory();

  /**
   * @return the {@link org.apache.solr.cloud.RecoveryStrategy.Builder} that should be used.
   */
  public abstract RecoveryStrategy.Builder getRecoveryStrategyBuilder();


  public interface IndexWriterCloser {
    void closeWriter(IndexWriter writer) throws IOException;
  }

  public abstract void doRecovery(CoreContainer cc, CoreDescriptor cd);
  
  public abstract void cancelRecovery();

  public abstract void close(IndexWriterCloser closer);

  /**
   * @return throttle to limit how fast a core attempts to become leader
   */
  public abstract ActionThrottle getLeaderThrottle();

  public abstract boolean getLastReplicateIndexSuccess();

  public abstract void setLastReplicateIndexSuccess(boolean success);

  public static class CoreIsClosedException extends AlreadyClosedException {
    
    public CoreIsClosedException() {
      super();
    }
    
    public CoreIsClosedException(String s) {
      super(s);
    }
  }

  public abstract Lock getRecoveryLock();

  // These are needed to properly synchronize the bootstrapping when the
  // in the target DC require a full sync.
  public abstract boolean getCdcrBootstrapRunning();

  public abstract void setCdcrBootstrapRunning(boolean cdcrRunning);

  public abstract Future<Boolean> getCdcrBootstrapFuture();

  public abstract void setCdcrBootstrapFuture(Future<Boolean> cdcrBootstrapFuture);

  @SuppressWarnings("rawtypes")
  public abstract Callable getCdcrBootstrapCallable();

  public abstract void setCdcrBootstrapCallable(@SuppressWarnings("rawtypes") Callable cdcrBootstrapCallable);

  public Throwable getTragicException() throws IOException {
    RefCounted<IndexWriter> ref = getIndexWriter(null);
    if (ref == null) return null;
    try {
      return ref.get().getTragicException();
    } finally {
      ref.decref();
    }
  }
}
