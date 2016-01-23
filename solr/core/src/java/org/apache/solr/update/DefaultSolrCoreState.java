package org.apache.solr.update;

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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.lucene.index.IndexWriter;
import org.apache.solr.cloud.ActionThrottle;
import org.apache.solr.cloud.RecoveryStrategy;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.util.RefCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DefaultSolrCoreState extends SolrCoreState implements RecoveryStrategy.RecoveryListener {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  private final boolean SKIP_AUTO_RECOVERY = Boolean.getBoolean("solrcloud.skip.autorecovery");

  private final ReentrantLock recoveryLock = new ReentrantLock();
  
  private final ActionThrottle recoveryThrottle = new ActionThrottle("recovery", 10000);
  
  private final ActionThrottle leaderThrottle = new ActionThrottle("leader", 5000);
  
  private final AtomicInteger recoveryWaiting = new AtomicInteger();

  // Use the readLock to retrieve the current IndexWriter (may be lazily opened)
  // Use the writeLock for changing index writers
  private final ReentrantReadWriteLock iwLock = new ReentrantReadWriteLock();

  private SolrIndexWriter indexWriter = null;
  private DirectoryFactory directoryFactory;

  private volatile RecoveryStrategy recoveryStrat;

  private volatile boolean lastReplicationSuccess = true;

  // will we attempt recovery as if we just started up (i.e. use starting versions rather than recent versions for peersync
  // so we aren't looking at update versions that have started buffering since we came up.
  private volatile boolean recoveringAfterStartup = true;

  private RefCounted<IndexWriter> refCntWriter;
  
  protected final ReentrantLock commitLock = new ReentrantLock();

  public DefaultSolrCoreState(DirectoryFactory directoryFactory) {
    this.directoryFactory = directoryFactory;
  }
  
  private void closeIndexWriter(IndexWriterCloser closer) {
    try {
      log.info("SolrCoreState ref count has reached 0 - closing IndexWriter");
      if (closer != null) {
        log.info("closing IndexWriter with IndexWriterCloser");
        closer.closeWriter(indexWriter);
      } else if (indexWriter != null) {
        log.info("closing IndexWriter...");
        indexWriter.close();
      }
      indexWriter = null;
    } catch (Exception e) {
      log.error("Error during close of writer.", e);
    } 
  }
  
  @Override
  public RefCounted<IndexWriter> getIndexWriter(SolrCore core)
      throws IOException {

    boolean succeeded = false;
    lock(iwLock.readLock());
    try {
      // Multiple readers may be executing this, but we only want one to open the writer on demand.
      synchronized (this) {
        if (core == null) {
          // core == null is a signal to just return the current writer, or null if none.
          initRefCntWriter();
          if (refCntWriter == null) return null;
        } else {
          if (indexWriter == null) {
            indexWriter = createMainIndexWriter(core, "DirectUpdateHandler2");
          }
          initRefCntWriter();
        }

        refCntWriter.incref();
        succeeded = true;  // the returned RefCounted<IndexWriter> will release the readLock on a decref()
        return refCntWriter;
      }

    } finally {
      // if we failed to return the IW for some other reason, we should unlock.
      if (!succeeded) {
        iwLock.readLock().unlock();
      }
    }

  }

  private void initRefCntWriter() {
    // TODO: since we moved to a read-write lock, and don't rely on the count to close the writer, we don't really
    // need this class any more.  It could also be a singleton created at the same time as SolrCoreState
    // or we could change the API of SolrCoreState to just return the writer and then add a releaseWriter() call.
    if (refCntWriter == null && indexWriter != null) {
      refCntWriter = new RefCounted<IndexWriter>(indexWriter) {

        @Override
        public void decref() {
          iwLock.readLock().unlock();
          super.decref();  // This is now redundant (since we switched to read-write locks), we don't really need to maintain our own reference count.
        }

        @Override
        public void close() {
          //  We rely on other code to actually close the IndexWriter, and there's nothing special to do when the ref count hits 0
        }
      };
    }
  }

  // acquires the lock or throws an exception if the CoreState has been closed.
  private void lock(Lock lock) {
    boolean acquired = false;
    do {
      try {
        acquired = lock.tryLock(100, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        log.warn("WARNING - Dangerous interrupt", e);
      }

      // even if we failed to acquire, check if we are closed
      if (closed) {
        if (acquired) {
          lock.unlock();
        }
        throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE, "SolrCoreState already closed.");
      }
    } while (!acquired);
  }

  // closes and opens index writers without any locking
  private void changeWriter(SolrCore core, boolean rollback, boolean openNewWriter) throws IOException {
    String coreName = core.getName();

    // We need to null this so it picks up the new writer next get call.
    // We do this before anything else in case we hit an exception.
    refCntWriter = null;
    IndexWriter iw = indexWriter; // temp reference just for closing
    indexWriter = null; // null this out now in case we fail, so we won't use the writer again

    if (iw != null) {
      if (!rollback) {
        try {
          log.info("Closing old IndexWriter... core=" + coreName);
          iw.close();
        } catch (Exception e) {
          SolrException.log(log, "Error closing old IndexWriter. core=" + coreName, e);
        }
      } else {
        try {
          log.info("Rollback old IndexWriter... core=" + coreName);
          iw.rollback();
        } catch (Exception e) {
          SolrException.log(log, "Error rolling back old IndexWriter. core=" + coreName, e);
        }
      }
    }

    if (openNewWriter) {
      indexWriter = createMainIndexWriter(core, "DirectUpdateHandler2");
      log.info("New IndexWriter is ready to be used.");
    }
  }

  @Override
  public void newIndexWriter(SolrCore core, boolean rollback) throws IOException {
    lock(iwLock.writeLock());
    try {
      changeWriter(core, rollback, true);
    } finally {
      iwLock.writeLock().unlock();
    }
  }

  @Override
  public void closeIndexWriter(SolrCore core, boolean rollback) throws IOException {
    lock(iwLock.writeLock());
    changeWriter(core, rollback, false);
    // Do not unlock the writeLock in this method.  It will be unlocked by the openIndexWriter call (see base class javadoc)
  }

  @Override
  public void openIndexWriter(SolrCore core) throws IOException {
    try {
      changeWriter(core, false, true);
    } finally {
      iwLock.writeLock().unlock();  //unlock even if we failed
    }
  }

  @Override
  public void rollbackIndexWriter(SolrCore core) throws IOException {
    changeWriter(core, true, true);
  }
  
  protected SolrIndexWriter createMainIndexWriter(SolrCore core, String name) throws IOException {
    return SolrIndexWriter.create(core, name, core.getNewIndexDir(),
        core.getDirectoryFactory(), false, core.getLatestSchema(),
        core.getSolrConfig().indexConfig, core.getDeletionPolicy(), core.getCodec());
  }

  @Override
  public DirectoryFactory getDirectoryFactory() {
    return directoryFactory;
  }

  @Override
  public void doRecovery(CoreContainer cc, CoreDescriptor cd) {
    
    Thread thread = new Thread() {
      @Override
      public void run() {
        MDCLoggingContext.setCoreDescriptor(cd);
        try {
          if (SKIP_AUTO_RECOVERY) {
            log.warn("Skipping recovery according to sys prop solrcloud.skip.autorecovery");
            return;
          }
          
          // check before we grab the lock
          if (cc.isShutDown()) {
            log.warn("Skipping recovery because Solr is shutdown");
            return;
          }
          
          // if we can't get the lock, another recovery is running
          // we check to see if there is already one waiting to go
          // after the current one, and if there is, bail
          boolean locked = recoveryLock.tryLock();
          try {
            if (!locked) {
              if (recoveryWaiting.get() > 0) {
                return;
              }
              recoveryWaiting.incrementAndGet();
            } else {
              recoveryWaiting.incrementAndGet();
              cancelRecovery();
            }
            
            recoveryLock.lock();
            try {
              recoveryWaiting.decrementAndGet();
              
              // to be air tight we must also check after lock
              if (cc.isShutDown()) {
                log.warn("Skipping recovery because Solr is shutdown");
                return;
              }
              log.info("Running recovery");
              
              recoveryThrottle.minimumWaitBetweenActions();
              recoveryThrottle.markAttemptingAction();
              
              recoveryStrat = new RecoveryStrategy(cc, cd, DefaultSolrCoreState.this);
              recoveryStrat.setRecoveringAfterStartup(recoveringAfterStartup);
              Future<?> future = cc.getUpdateShardHandler().getRecoveryExecutor().submit(recoveryStrat);
              try {
                future.get();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SolrException(ErrorCode.SERVER_ERROR, e);
              } catch (ExecutionException e) {
                throw new SolrException(ErrorCode.SERVER_ERROR, e);
              }
            } finally {
              recoveryLock.unlock();
            }
          } finally {
            if (locked) recoveryLock.unlock();
          }
        } finally {
          MDCLoggingContext.clear();
        }
      }
    };
    try {
      // we make recovery requests async - that async request may
      // have to 'wait in line' a bit or bail if a recovery is 
      // already queued up - the recovery execution itself is run
      // in another thread on another 'recovery' executor.
      // The update executor is interrupted on shutdown and should 
      // not do disk IO.
      // The recovery executor is not interrupted on shutdown.
      //
      // avoid deadlock: we can't use the recovery executor here
      cc.getUpdateShardHandler().getUpdateExecutor().submit(thread);
    } catch (RejectedExecutionException e) {
      // fine, we are shutting down
    }
  }
  
  @Override
  public void cancelRecovery() {
    if (recoveryStrat != null) {
      try {
        recoveryStrat.close();
      } catch (NullPointerException e) {
        // okay
      }
    }
  }

  /** called from recoveryStrat on a successful recovery */
  @Override
  public void recovered() {
    recoveringAfterStartup = false;  // once we have successfully recovered, we no longer need to act as if we are recovering after startup
  }

  /** called from recoveryStrat on a failed recovery */
  @Override
  public void failed() {}

  @Override
  public synchronized void close(IndexWriterCloser closer) {
    closed = true;
    cancelRecovery();
    closeIndexWriter(closer);
  }
  
  @Override
  public Lock getCommitLock() {
    return commitLock;
  }
  
  @Override
  public ActionThrottle getLeaderThrottle() {
    return leaderThrottle;
  }
  
  @Override
  public boolean getLastReplicateIndexSuccess() {
    return lastReplicationSuccess;
  }

  @Override
  public void setLastReplicateIndexSuccess(boolean success) {
    this.lastReplicationSuccess = success;
  }
  
}
