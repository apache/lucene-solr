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
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.search.Sort;
import org.apache.solr.cloud.ActionThrottle;
import org.apache.solr.cloud.RecoveryStrategy;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.index.SortingMergePolicy;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.util.RefCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class DefaultSolrCoreState extends SolrCoreState implements RecoveryStrategy.RecoveryListener {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final boolean SKIP_AUTO_RECOVERY = Boolean.getBoolean("solrcloud.skip.autorecovery");

  private final ReentrantLock recoveryLock = new ReentrantLock(false);

  private final ActionThrottle recoveryThrottle = new ActionThrottle("recovery", Integer.getInteger("solr.recoveryThrottle", 0));

  private final ActionThrottle leaderThrottle = new ActionThrottle("leader", Integer.getInteger("solr.leaderThrottle", 0));

  private final AtomicInteger recoveryWaiting = new AtomicInteger();

  // Use the readLock to retrieve the current IndexWriter (may be lazily opened)
  // Use the writeLock for changing index writers
  private final ReentrantReadWriteLock iwLock = new ReentrantReadWriteLock(true);

  private volatile SolrIndexWriter indexWriter = null;
  private final DirectoryFactory directoryFactory;
  private final RecoveryStrategy.Builder recoveryStrategyBuilder;

  private volatile RecoveryStrategy recoveryStrat;
  private volatile Future recoveryFuture;

  private volatile boolean lastReplicationSuccess = true;

  // will we attempt recovery as if we just started up (i.e. use starting versions rather than recent versions for peersync
  // so we aren't looking at update versions that have started buffering since we came up.
  private volatile boolean recoveringAfterStartup = true;

  private volatile RefCounted<IndexWriter> refCntWriter;

  protected final ReentrantLock commitLock = new ReentrantLock();

  private final AtomicBoolean cdcrRunning = new AtomicBoolean();

  private volatile Future<Boolean> cdcrBootstrapFuture;

  private volatile Callable cdcrBootstrapCallable;

  private volatile boolean prepForClose;
  private volatile boolean recoverying;

  @Deprecated
  public DefaultSolrCoreState(DirectoryFactory directoryFactory) {
    this(directoryFactory, new RecoveryStrategy.Builder());
  }

  public DefaultSolrCoreState(DirectoryFactory directoryFactory,
                              RecoveryStrategy.Builder recoveryStrategyBuilder) {
    this.directoryFactory = directoryFactory;
    this.recoveryStrategyBuilder = recoveryStrategyBuilder;
  }

  private void closeIndexWriter(IndexWriterCloser closer) {
    try {
      if (log.isDebugEnabled()) log.debug("SolrCoreState ref count has reached 0 - closing IndexWriter");
      if (closer != null) {
        if (log.isDebugEnabled()) log.debug("closing IndexWriter with IndexWriterCloser");

        // indexWriter may be null if there was a failure in opening the search during core init,
        // such as from an index corruption issue (see TestCoreAdmin#testReloadCoreAfterFailure)
        if (indexWriter != null) {
          closer.closeWriter(indexWriter);
        }
      } else if (indexWriter != null) {
        log.debug("closing IndexWriter...");
        indexWriter.close();
      }
      indexWriter = null;
    } catch (Exception e) {
      ParWork.propagateInterrupt("Error during close of writer.", e);
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }
  }

  @Override
  public RefCounted<IndexWriter> getIndexWriter(SolrCore core)
          throws IOException {
    if (core != null && (!core.indexEnabled || core.readOnly)) {
      throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE,
              "Indexing is temporarily disabled");
    }

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
        acquired = lock.tryLock() || lock.tryLock(100, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        log.warn("WARNING - Dangerous interrupt", e);
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
          log.debug("Closing old IndexWriter... core=" + coreName);
          iw.commit();
          iw.close();
        } catch (Exception e) {
          ParWork.propagateInterrupt("Error closing old IndexWriter. core=" + coreName, e);
        }
      } else {
        try {
          log.debug("Rollback old IndexWriter... core=" + coreName);
          iw.rollback();
        } catch (Exception e) {
          ParWork.propagateInterrupt("Error rolling back old IndexWriter. core=" + coreName, e);
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
    iwLock.writeLock().lock();
    try {
      changeWriter(core, true, true);
    } finally {
      iwLock.writeLock().unlock();
    }
  }

  protected SolrIndexWriter createMainIndexWriter(SolrCore core, String name) throws IOException {
    SolrIndexWriter iw;
    try {
      iw = SolrIndexWriter.buildIndexWriter(core, name, core.getNewIndexDir(), core.getDirectoryFactory(), false, core.getLatestSchema(),
              core.getSolrConfig().indexConfig, core.getDeletionPolicy(), core.getCodec(), false);
    } catch (Exception e) {
      ParWork.propagateInterrupt(e);
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }

    return iw;
  }

  public Sort getMergePolicySort() throws IOException {
    lock(iwLock.readLock());
    try {
      if (indexWriter != null) {
        final MergePolicy mergePolicy = indexWriter.getConfig().getMergePolicy();
        if (mergePolicy instanceof SortingMergePolicy) {
          return ((SortingMergePolicy)mergePolicy).getSort();
        }
      }
    } finally {
      iwLock.readLock().unlock();
    }
    return null;
  }

  @Override
  public DirectoryFactory getDirectoryFactory() {
    return directoryFactory;
  }

  @Override
  public RecoveryStrategy.Builder getRecoveryStrategyBuilder() {
    return recoveryStrategyBuilder;
  }

  @Override
  public void doRecovery(CoreContainer cc, CoreDescriptor cd) {
    try (SolrCore core = cc.getCore(cd.getName())) {
      doRecovery(core);
    }
  }

  @Override
  public void doRecovery(SolrCore core) {

    log.info("Do recovery for core {}", core.getName());
    CoreContainer corecontainer = core.getCoreContainer();
    if (prepForClose || closed || corecontainer.isShutDown()) {
      log.warn("Skipping recovery because Solr is shutdown");
      return;
    }
    Runnable recoveryTask = () -> {
      CoreDescriptor coreDescriptor = core.getCoreDescriptor();
      MDCLoggingContext.setCoreName(core.getName());
      MDCLoggingContext.setNode(corecontainer.getZkController().getNodeName());
      try {
        if (SKIP_AUTO_RECOVERY) {
          log.warn("Skipping recovery according to sys prop solrcloud.skip.autorecovery");
          return;
        }

        if (log.isDebugEnabled()) log.debug("Going to create and run RecoveryStrategy");

//        try {
//          Replica leader = core.getCoreContainer().getZkController().getZkStateReader().getLeaderRetry(core.getCoreDescriptor().getCollectionName(), core.getCoreDescriptor().getCloudDescriptor().getShardId(), 1000);
//          if (leader != null && leader.getName().equals(core.getName())) {
//            return;
//          }
//        } catch (InterruptedException e) {
//          throw new SolrException(ErrorCode.BAD_REQUEST, e);
//        } catch (TimeoutException e) {
//          throw new SolrException(ErrorCode.SERVER_ERROR, e);
//        }

        // check before we grab the lock
        if (prepForClose || closed || corecontainer.isShutDown()) {
          log.warn("Skipping recovery because Solr is shutdown");
          return;
        }

        // if we can't get the lock, another recovery is running
        // we check to see if there is already one waiting to go
        // after the current one, and if there is, bail
        boolean locked = recoveryLock.tryLock();

//        if (closed || prepForClose) {
//          return;
//        }
        if (!locked) {
          recoveryWaiting.incrementAndGet();
          if (log.isDebugEnabled()) log.debug("Wait for recovery lock");
          //cancelRecovery(true, false);
          while (!(recoveryLock.tryLock() || recoveryLock.tryLock(500, TimeUnit.MILLISECONDS))) {
            if (closed || prepForClose) {
              log.warn("Skipping recovery because we are closed");
              recoveryWaiting.decrementAndGet();
              return;
            }
          }
          // don't use recoveryLock.getQueueLength() for this
          if (recoveryWaiting.decrementAndGet() > 0) {
            // another recovery waiting behind us, let it run now instead of after we finish
            log.info("Skipping recovery because there is another in line behind");
            return;
          }
        }

        recoverying = true;

        // to be air tight we must also check after lock
        if (prepForClose || closed || corecontainer.isShutDown()) {
          log.info("Skipping recovery due to being closed");
          return;
        }

        recoveryThrottle.minimumWaitBetweenActions();
        recoveryThrottle.markAttemptingAction();

        recoveryStrat = recoveryStrategyBuilder.create(corecontainer, coreDescriptor, DefaultSolrCoreState.this);
        recoveryStrat.setRecoveringAfterStartup(recoveringAfterStartup);

        if (log.isDebugEnabled()) log.debug("Running recovery");

        recoveryStrat.run();

      } catch (AlreadyClosedException e) {
        log.warn("Skipping recovery because we are closed");
      } catch (Exception e) {
        log.error("Exception starting recovery", e);
      } finally {
        recoverying = false;
        if (recoveryLock.isHeldByCurrentThread()) {
          recoveryLock.unlock();
        }
        MDCLoggingContext.clear();
      }
    };
    boolean success = false;
    try {
      // we make recovery requests async - that async request may
      // have to 'wait in line' a bit or bail if a recovery is
      // already queued up - the recovery execution itself is run
      // in another thread on another 'recovery' executor.
      //
      if (log.isDebugEnabled()) log.debug("Submit recovery for {}", core.getName());
      recoveryFuture = core.getCoreContainer().getUpdateShardHandler().getRecoveryExecutor().submit(recoveryTask);
      success = true;
    } catch (RejectedExecutionException e) {
      // fine, we are shutting down
      log.warn("Skipping recovery because we are closed");
    } finally {
      if (!success) {
        recoverying = false;
      }

    }
  }

  @Override
  public void cancelRecovery() {
    cancelRecovery(false, false);
  }

  @Override
  public void cancelRecovery(boolean wait, boolean prepForClose) {
    if (log.isDebugEnabled()) log.debug("Cancel recovery");
    recoverying = false;
    
    if (prepForClose) {
      this.prepForClose = true;
    }

    if (recoveryStrat != null) {
      try {
        recoveryStrat.close();
      } catch (NullPointerException e) {
        // okay
      }
    }

    if (recoveryFuture != null) {
      try {
        recoveryFuture.cancel(false);
      } catch (NullPointerException e) {
        // okay
      }
    }

    recoveryFuture = null;
    recoveryStrat = null;
  }

  /** called from recoveryStrat on a successful recovery */
  @Override
  public void recovered() {
    recoveringAfterStartup = false;  // once we have successfully recovered, we no longer need to act as if we are recovering after startup
  }

  public boolean isRecoverying() {
    return recoverying;
  }


  /** called from recoveryStrat on a failed recovery */
  @Override
  public void failed() {

  }

  @Override
  public void close(IndexWriterCloser closer) {

    // we can't lock here without
    // a blocking race, we should not need to
    // though
    // iwLock.writeLock().lock();
    if (recoverying) {
      cancelRecovery(false, true);
    }
    try {
      closeIndexWriter(closer);
    } finally {
      // iwLock.writeLock().unlock();
    }

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

  @Override
  public Lock getRecoveryLock() {
    return recoveryLock;
  }
}
