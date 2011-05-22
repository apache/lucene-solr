package org.apache.lucene.index;
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.Iterator;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.FieldInfos.FieldNumberBiMap;
import org.apache.lucene.index.SegmentCodecs.SegmentCodecsBuilder;
import org.apache.lucene.index.codecs.CodecProvider;
import org.apache.lucene.util.SetOnce;

/**
 * {@link DocumentsWriterPerThreadPool} controls {@link ThreadState} instances
 * and their thread assignments during indexing. Each {@link ThreadState} holds
 * a reference to a {@link DocumentsWriterPerThread} that is once a
 * {@link ThreadState} is obtained from the pool exclusively used for indexing a
 * single document by the obtaining thread. Each indexing thread must obtain
 * such a {@link ThreadState} to make progress. Depending on the
 * {@link DocumentsWriterPerThreadPool} implementation {@link ThreadState}
 * assignments might differ from document to document.
 * <p>
 * Once a {@link DocumentsWriterPerThread} is selected for flush the thread pool
 * is reusing the flushing {@link DocumentsWriterPerThread}s ThreadState with a
 * new {@link DocumentsWriterPerThread} instance.
 * </p>
 */
public abstract class DocumentsWriterPerThreadPool {
  /** The maximum number of simultaneous threads that may be
   *  indexing documents at once in IndexWriter; if more
   *  than this many threads arrive they will wait for
   *  others to finish. */
  public final static int DEFAULT_MAX_THREAD_STATES = 8;
  
  /**
   * {@link ThreadState} references and guards a
   * {@link DocumentsWriterPerThread} instance that is used during indexing to
   * build a in-memory index segment. {@link ThreadState} also holds all flush
   * related per-thread data controlled by {@link DocumentsWriterFlushControl}.
   * <p>
   * A {@link ThreadState}, its methods and members should only accessed by one
   * thread a time. Users must acquire the lock via {@link ThreadState#lock()}
   * and release the lock in a finally block via {@link ThreadState#unlock()}
   * before accessing the state.
   */
  @SuppressWarnings("serial")
  public final static class ThreadState extends ReentrantLock {
    // package private for FlushPolicy
    DocumentsWriterPerThread perThread;
    // write access guarded by DocumentsWriterFlushControl
    volatile boolean flushPending = false;
    // write access guarded by DocumentsWriterFlushControl
    long bytesUsed = 0;
    // guarded by Reentrant lock
    private boolean isActive = true;

    ThreadState(DocumentsWriterPerThread perThread) {
      this.perThread = perThread;
    }
    
    /**
     * Resets the internal {@link DocumentsWriterPerThread} with the given one. 
     * if the given DWPT is <code>null</code> this ThreadState is marked as inactive and should not be used
     * for indexing anymore.
     * @see #isActive()  
     */
    void resetWriter(DocumentsWriterPerThread perThread) {
      assert this.isHeldByCurrentThread();
      if (perThread == null) {
        isActive = false;
      }
      this.perThread = perThread;
      this.bytesUsed = 0;
      this.flushPending = false;
    }
    
    /**
     * Returns <code>true</code> if this ThreadState is still open. This will
     * only return <code>false</code> iff the DW has been closed and this
     * ThreadState is already checked out for flush.
     */
    boolean isActive() {
      assert this.isHeldByCurrentThread();
      return isActive;
    }
    
    /**
     * Returns the number of currently active bytes in this ThreadState's
     * {@link DocumentsWriterPerThread}
     */
    public long getBytesUsedPerThread() {
      assert this.isHeldByCurrentThread();
      // public for FlushPolicy
      return bytesUsed;
    }
    
    /**
     * Returns this {@link ThreadState}s {@link DocumentsWriterPerThread}
     */
    public DocumentsWriterPerThread getDocumentsWriterPerThread() {
      assert this.isHeldByCurrentThread();
      // public for FlushPolicy
      return perThread;
    }
    
    /**
     * Returns <code>true</code> iff this {@link ThreadState} is marked as flush
     * pending otherwise <code>false</code>
     */
    public boolean isFlushPending() {
      return flushPending;
    }
  }

  private final ThreadState[] perThreads;
  private volatile int numThreadStatesActive;
  private CodecProvider codecProvider;
  private FieldNumberBiMap globalFieldMap;
  private final SetOnce<DocumentsWriter> documentsWriter = new SetOnce<DocumentsWriter>();
  
  /**
   * Creates a new {@link DocumentsWriterPerThreadPool} with max.
   * {@link #DEFAULT_MAX_THREAD_STATES} thread states.
   */
  public DocumentsWriterPerThreadPool() {
    this(DEFAULT_MAX_THREAD_STATES);
  }

  public DocumentsWriterPerThreadPool(int maxNumPerThreads) {
    maxNumPerThreads = (maxNumPerThreads < 1) ? DEFAULT_MAX_THREAD_STATES : maxNumPerThreads;
    perThreads = new ThreadState[maxNumPerThreads];
    numThreadStatesActive = 0;
  }

  public void initialize(DocumentsWriter documentsWriter, FieldNumberBiMap globalFieldMap, IndexWriterConfig config) {
    this.documentsWriter.set(documentsWriter); // thread pool is bound to DW
    final CodecProvider codecs = config.getCodecProvider();
    this.codecProvider = codecs;
    this.globalFieldMap = globalFieldMap;
    for (int i = 0; i < perThreads.length; i++) {
      final FieldInfos infos = globalFieldMap.newFieldInfos(SegmentCodecsBuilder.create(codecs));
      perThreads[i] = new ThreadState(new DocumentsWriterPerThread(documentsWriter.directory, documentsWriter, infos, documentsWriter.chain));
    }
  }

  /**
   * Returns the max number of {@link ThreadState} instances available in this
   * {@link DocumentsWriterPerThreadPool}
   */
  public int getMaxThreadStates() {
    return perThreads.length;
  }
  
  /**
   * Returns the active number of {@link ThreadState} instances.
   */
  public int getActiveThreadState() {
    return numThreadStatesActive;
  }

  /**
   * Returns a new {@link ThreadState} iff any new state is available otherwise
   * <code>null</code>.
   * <p>
   * NOTE: the returned {@link ThreadState} is already locked iff non-
   * <code>null</code>.
   * 
   * @return a new {@link ThreadState} iff any new state is available otherwise
   *         <code>null</code>
   */
  public synchronized ThreadState newThreadState() {
    if (numThreadStatesActive < perThreads.length) {
      final ThreadState threadState = perThreads[numThreadStatesActive];
      threadState.lock(); // lock so nobody else will get this ThreadState
      numThreadStatesActive++; // increment will publish the ThreadState
      threadState.perThread.initialize();
      return threadState;
    }
    return null;
  }
  
  protected DocumentsWriterPerThread replaceForFlush(ThreadState threadState, boolean closed) {
    assert threadState.isHeldByCurrentThread();
    final DocumentsWriterPerThread dwpt = threadState.perThread;
    if (!closed) {
      final FieldInfos infos = globalFieldMap.newFieldInfos(SegmentCodecsBuilder.create(codecProvider));
      final DocumentsWriterPerThread newDwpt = new DocumentsWriterPerThread(dwpt, infos);
      newDwpt.initialize();
      threadState.resetWriter(newDwpt);
    } else {
      threadState.resetWriter(null);
    }
    return dwpt;
  }
  
  public void recycle(DocumentsWriterPerThread dwpt) {
    // don't recycle DWPT by default
  }
  
  public abstract ThreadState getAndLock(Thread requestingThread, DocumentsWriter documentsWriter, Document doc);

  /**
   * Returns an iterator providing access to all {@link ThreadState}
   * instances. 
   */
  // TODO: new Iterator per indexed doc is overkill...?
  public Iterator<ThreadState> getAllPerThreadsIterator() {
    return getPerThreadsIterator(this.perThreads.length);
  }

  /**
   * Returns an iterator providing access to all active {@link ThreadState}
   * instances. 
   * <p>
   * Note: The returned iterator will only iterator
   * {@link ThreadState}s that are active at the point in time when this method
   * has been called.
   * 
   */
  // TODO: new Iterator per indexed doc is overkill...?
  public Iterator<ThreadState> getActivePerThreadsIterator() {
    return getPerThreadsIterator(numThreadStatesActive);
  }

  private Iterator<ThreadState> getPerThreadsIterator(final int upto) {
    return new Iterator<ThreadState>() {
      int i = 0;

      public boolean hasNext() {
        return i < upto;
      }

      public ThreadState next() {
        return perThreads[i++];
      }

      public void remove() {
        throw new UnsupportedOperationException("remove() not supported.");
      }
    };
  }

  /**
   * Returns the ThreadState with the minimum estimated number of threads
   * waiting to acquire its lock or <code>null</code> if no {@link ThreadState}
   * is yet visible to the calling thread.
   */
  protected ThreadState minContendedThreadState() {
    ThreadState minThreadState = null;
    // TODO: new Iterator per indexed doc is overkill...?
    final Iterator<ThreadState> it = getActivePerThreadsIterator();
    while (it.hasNext()) {
      final ThreadState state = it.next();
      if (minThreadState == null || state.getQueueLength() < minThreadState.getQueueLength()) {
        minThreadState = state;
      }
    }
    return minThreadState;
  }
}
