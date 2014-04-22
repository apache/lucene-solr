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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.solr.cloud.RecoveryStrategy;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.util.IOUtils;
import org.apache.solr.util.RefCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DefaultSolrCoreState extends SolrCoreState implements RecoveryStrategy.RecoveryListener {
  public static Logger log = LoggerFactory.getLogger(DefaultSolrCoreState.class);
  
  private final boolean SKIP_AUTO_RECOVERY = Boolean.getBoolean("solrcloud.skip.autorecovery");
  
  private final Object recoveryLock = new Object();
  
  // protects pauseWriter and writerFree
  private final Object writerPauseLock = new Object();
  
  private SolrIndexWriter indexWriter = null;
  private DirectoryFactory directoryFactory;

  private volatile boolean recoveryRunning;
  private RecoveryStrategy recoveryStrat;

  private RefCounted<IndexWriter> refCntWriter;

  private boolean pauseWriter;
  private boolean writerFree = true;
  
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
      }
      indexWriter = null;
    } catch (Exception e) {
      log.error("Error during shutdown of writer.", e);
    } 
  }
  
  @Override
  public RefCounted<IndexWriter> getIndexWriter(SolrCore core)
      throws IOException {
    synchronized (writerPauseLock) {
      if (closed) {
        throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE, "SolrCoreState already closed");
      }
      
      while (pauseWriter) {
        try {
          writerPauseLock.wait(100);
        } catch (InterruptedException e) {}
        
        if (closed) {
          throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE, "Already closed");
        }
      }
      
      if (core == null) {
        // core == null is a signal to just return the current writer, or null
        // if none.
        initRefCntWriter();
        if (refCntWriter == null) return null;
        writerFree = false;
        writerPauseLock.notifyAll();
        if (refCntWriter != null) refCntWriter.incref();
        
        return refCntWriter;
      }
      
      if (indexWriter == null) {
        indexWriter = createMainIndexWriter(core, "DirectUpdateHandler2");
      }
      initRefCntWriter();
      writerFree = false;
      writerPauseLock.notifyAll();
      refCntWriter.incref();
      return refCntWriter;
    }
  }

  private void initRefCntWriter() {
    if (refCntWriter == null && indexWriter != null) {
      refCntWriter = new RefCounted<IndexWriter>(indexWriter) {
        @Override
        public void close() {
          synchronized (writerPauseLock) {
            writerFree = true;
            writerPauseLock.notifyAll();
          }
        }
      };
    }
  }

  @Override
  public synchronized void newIndexWriter(SolrCore core, boolean rollback) throws IOException {
    log.info("Creating new IndexWriter...");
    String coreName = core.getName();
    synchronized (writerPauseLock) {
      if (closed) {
        throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE, "Already closed");
      }
      
      // we need to wait for the Writer to fall out of use
      // first lets stop it from being lent out
      pauseWriter = true;
      // then lets wait until its out of use
      log.info("Waiting until IndexWriter is unused... core=" + coreName);
      
      while (!writerFree) {
        try {
          writerPauseLock.wait(100);
        } catch (InterruptedException e) {}
        
        if (closed) {
          throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE, "SolrCoreState already closed");
        }
      }

      try {
        if (indexWriter != null) {
          if (!rollback) {
            closeIndexWriter(coreName);
          } else {
            rollbackIndexWriter(coreName);
          }
        }
        indexWriter = createMainIndexWriter(core, "DirectUpdateHandler2");
        log.info("New IndexWriter is ready to be used.");
        // we need to null this so it picks up the new writer next get call
        refCntWriter = null;
      } finally {
        
        pauseWriter = false;
        writerPauseLock.notifyAll();
      }
    }
  }

  private void closeIndexWriter(String coreName) {
    try {
      log.info("Closing old IndexWriter... core=" + coreName);
      Directory dir = indexWriter.getDirectory();
      try {
        IOUtils.closeQuietly(indexWriter);
      } finally {
        if (IndexWriter.isLocked(dir)) {
          IndexWriter.unlock(dir);
        }
      }
    } catch (Exception e) {
      SolrException.log(log, "Error closing old IndexWriter. core="
          + coreName, e);
    }
  }

  private void rollbackIndexWriter(String coreName) {
    try {
      log.info("Rollback old IndexWriter... core=" + coreName);
      Directory dir = indexWriter.getDirectory();
      try {
        
        indexWriter.rollback();
      } finally {
        if (IndexWriter.isLocked(dir)) {
          IndexWriter.unlock(dir);
        }
      }
    } catch (Exception e) {
      SolrException.log(log, "Error rolling back old IndexWriter. core="
          + coreName, e);
    }
  }
  
  @Override
  public synchronized void closeIndexWriter(SolrCore core, boolean rollback)
      throws IOException {
    log.info("Closing IndexWriter...");
    String coreName = core.getName();
    synchronized (writerPauseLock) {
      if (closed) {
        throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE, "Already closed");
      }
      
      // we need to wait for the Writer to fall out of use
      // first lets stop it from being lent out
      pauseWriter = true;
      // then lets wait until its out of use
      log.info("Waiting until IndexWriter is unused... core=" + coreName);
      
      while (!writerFree) {
        try {
          writerPauseLock.wait(100);
        } catch (InterruptedException e) {}
        
        if (closed) {
          throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE,
              "SolrCoreState already closed");
        }
      }
      
      if (indexWriter != null) {
        if (!rollback) {
          closeIndexWriter(coreName);
        } else {
          rollbackIndexWriter(coreName);
        }
      }
      
    }
  }
  
  @Override
  public synchronized void openIndexWriter(SolrCore core) throws IOException {
    log.info("Creating new IndexWriter...");
    synchronized (writerPauseLock) {
      if (closed) {
        throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE, "Already closed");
      }
      
      try {
        indexWriter = createMainIndexWriter(core, "DirectUpdateHandler2");
        log.info("New IndexWriter is ready to be used.");
        // we need to null this so it picks up the new writer next get call
        refCntWriter = null;
      } finally {
        pauseWriter = false;
        writerPauseLock.notifyAll();
      }
    }
  }

  @Override
  public synchronized void rollbackIndexWriter(SolrCore core) throws IOException {
    newIndexWriter(core, true);
  }
  
  protected SolrIndexWriter createMainIndexWriter(SolrCore core, String name) throws IOException {
    return SolrIndexWriter.create(name, core.getNewIndexDir(),
        core.getDirectoryFactory(), false, core.getLatestSchema(),
        core.getSolrConfig().indexConfig, core.getDeletionPolicy(), core.getCodec());
  }

  @Override
  public DirectoryFactory getDirectoryFactory() {
    return directoryFactory;
  }

  @Override
  public void doRecovery(CoreContainer cc, CoreDescriptor cd) {
    if (SKIP_AUTO_RECOVERY) {
      log.warn("Skipping recovery according to sys prop solrcloud.skip.autorecovery");
      return;
    }
    
    // check before we grab the lock
    if (cc.isShutDown()) {
      log.warn("Skipping recovery because Solr is shutdown");
      return;
    }
    
    synchronized (recoveryLock) {
      // to be air tight we must also check after lock
      if (cc.isShutDown()) {
        log.warn("Skipping recovery because Solr is shutdown");
        return;
      }
      log.info("Running recovery - first canceling any ongoing recovery");
      cancelRecovery();
      
      while (recoveryRunning) {
        try {
          recoveryLock.wait(1000);
        } catch (InterruptedException e) {

        }
        // check again for those that were waiting
        if (cc.isShutDown()) {
          log.warn("Skipping recovery because Solr is shutdown");
          return;
        }
        if (closed) return;
      }

      // if true, we are recovering after startup and shouldn't have (or be receiving) additional updates (except for local tlog recovery)
      boolean recoveringAfterStartup = recoveryStrat == null;

      recoveryStrat = new RecoveryStrategy(cc, cd, this);
      recoveryStrat.setRecoveringAfterStartup(recoveringAfterStartup);
      recoveryStrat.start();
      recoveryRunning = true;
    }
    
  }
  
  @Override
  public void cancelRecovery() {
    synchronized (recoveryLock) {
      if (recoveryStrat != null && recoveryRunning) {
        recoveryStrat.close();
        while (true) {
          try {
            recoveryStrat.join();
          } catch (InterruptedException e) {
            // not interruptible - keep waiting
            continue;
          }
          break;
        }
        
        recoveryRunning = false;
        recoveryLock.notifyAll();
      }
    }
  }

  @Override
  public void recovered() {
    recoveryRunning = false;
  }

  @Override
  public void failed() {
    recoveryRunning = false;
  }

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
  
}
