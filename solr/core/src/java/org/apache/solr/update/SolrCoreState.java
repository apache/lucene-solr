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

import org.apache.lucene.index.IndexWriter;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.util.RefCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The state in this class can be easily shared between SolrCores across
 * SolrCore reloads.
 * 
 */
public abstract class SolrCoreState {
  public static Logger log = LoggerFactory.getLogger(SolrCoreState.class);
  
  protected boolean closed = false;
  private final Object deleteLock = new Object();
  
  public Object getUpdateLock() {
    return deleteLock;
  }
  
  private int solrCoreStateRefCnt = 1;

  public void increfSolrCoreState() {
    synchronized (this) {
      if (solrCoreStateRefCnt == 0) {
        throw new IllegalStateException("IndexWriter has been closed");
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
        log.info("Closing SolrCoreState");
        close(closer);
      } catch (Throwable t) {
        log.error("Error closing SolrCoreState", t);
      }
    }
    return close;
  }
  
  public abstract Lock getCommitLock();
  
  /**
   * Force the creation of a new IndexWriter using the settings from the given
   * SolrCore.
   * 
   * @param rollback close IndexWriter if false, else rollback
   * @throws IOException If there is a low-level I/O error.
   */
  public abstract void newIndexWriter(SolrCore core, boolean rollback, boolean forceNewDir) throws IOException;
  
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
   * @return the {@link DirectoryFactory} that should be used.
   */
  public abstract DirectoryFactory getDirectoryFactory();


  public interface IndexWriterCloser {
    public void closeWriter(IndexWriter writer) throws IOException;
  }

  public abstract void doRecovery(CoreContainer cc, CoreDescriptor cd);
  
  public abstract void cancelRecovery();

  public abstract void close(IndexWriterCloser closer);

}
