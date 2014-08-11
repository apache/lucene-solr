package org.apache.lucene.index;
/*
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

import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.ThreadInterruptedException;

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
final class DocumentsWriterPerThreadPool {
  
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
  final static class ThreadState extends ReentrantLock {
    DocumentsWriterPerThread dwpt;
    // TODO this should really be part of DocumentsWriterFlushControl
    // write access guarded by DocumentsWriterFlushControl
    volatile boolean flushPending = false;
    // TODO this should really be part of DocumentsWriterFlushControl
    // write access guarded by DocumentsWriterFlushControl
    long bytesUsed = 0;
    // guarded by Reentrant lock
    private boolean isActive = true;

    ThreadState(DocumentsWriterPerThread dpwt) {
      this.dwpt = dpwt;
    }
    
    /**
     * Resets the internal {@link DocumentsWriterPerThread} with the given one. 
     * if the given DWPT is <code>null</code> this ThreadState is marked as inactive and should not be used
     * for indexing anymore.
     * @see #isActive()  
     */
  
    private void deactivate() {
      assert this.isHeldByCurrentThread();
      isActive = false;
      reset();
    }
    
    private void reset() {
      assert this.isHeldByCurrentThread();
      this.dwpt = null;
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
    
    boolean isInitialized() {
      assert this.isHeldByCurrentThread();
      return isActive() && dwpt != null;
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
      return dwpt;
    }
    
    /**
     * Returns <code>true</code> iff this {@link ThreadState} is marked as flush
     * pending otherwise <code>false</code>
     */
    public boolean isFlushPending() {
      return flushPending;
    }
  }

  private final ThreadState[] threadStates;
  private volatile int numThreadStatesActive;

  private final ThreadState[] freeList;
  private int freeCount;

  /**
   * Creates a new {@link DocumentsWriterPerThreadPool} with a given maximum of {@link ThreadState}s.
   */
  DocumentsWriterPerThreadPool(int maxNumThreadStates) {
    if (maxNumThreadStates < 1) {
      throw new IllegalArgumentException("maxNumThreadStates must be >= 1 but was: " + maxNumThreadStates);
    }
    threadStates = new ThreadState[maxNumThreadStates];
    numThreadStatesActive = 0;
    for (int i = 0; i < threadStates.length; i++) {
      threadStates[i] = new ThreadState(null);
    }
    freeList = new ThreadState[maxNumThreadStates];
  }

  /**
   * Returns the max number of {@link ThreadState} instances available in this
   * {@link DocumentsWriterPerThreadPool}
   */
  int getMaxThreadStates() {
    return threadStates.length;
  }
  
  /**
   * Returns the active number of {@link ThreadState} instances.
   */
  int getActiveThreadState() {
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
  private ThreadState newThreadState() {
    assert numThreadStatesActive < threadStates.length;
    final ThreadState threadState = threadStates[numThreadStatesActive];
    threadState.lock(); // lock so nobody else will get this ThreadState
    boolean unlock = true;
    try {
      if (threadState.isActive()) {
        // unreleased thread states are deactivated during DW#close()
        numThreadStatesActive++; // increment will publish the ThreadState
        //System.out.println("activeCount=" + numThreadStatesActive);
        assert threadState.dwpt == null;
        unlock = false;
        return threadState;
      }
      // we are closed: unlock since the threadstate is not active anymore
      assert assertUnreleasedThreadStatesInactive();
      throw new AlreadyClosedException("this IndexWriter is closed");
    } finally {
      if (unlock) {
        // in any case make sure we unlock if we fail 
        threadState.unlock();
      }
    }
  }
  
  private synchronized boolean assertUnreleasedThreadStatesInactive() {
    for (int i = numThreadStatesActive; i < threadStates.length; i++) {
      assert threadStates[i].tryLock() : "unreleased threadstate should not be locked";
      try {
        assert !threadStates[i].isInitialized() : "expected unreleased thread state to be inactive";
      } finally {
        threadStates[i].unlock();
      }
    }
    return true;
  }
  
  /**
   * Deactivate all unreleased threadstates 
   */
  synchronized void deactivateUnreleasedStates() {
    for (int i = numThreadStatesActive; i < threadStates.length; i++) {
      final ThreadState threadState = threadStates[i];
      threadState.lock();
      try {
        threadState.deactivate();
      } finally {
        threadState.unlock();
      }
    }
    
    // In case any threads are waiting for indexing:
    notifyAll();
  }
  
  DocumentsWriterPerThread reset(ThreadState threadState, boolean closed) {
    assert threadState.isHeldByCurrentThread();
    final DocumentsWriterPerThread dwpt = threadState.dwpt;
    if (!closed) {
      threadState.reset();
    } else {
      threadState.deactivate();
    }
    return dwpt;
  }
  
  void recycle(DocumentsWriterPerThread dwpt) {
    // don't recycle DWPT by default
  }

  /** This method is used by DocumentsWriter/FlushControl to obtain a ThreadState to do an indexing operation (add/updateDocument). */
  ThreadState getAndLock(Thread requestingThread, DocumentsWriter documentsWriter) {
    ThreadState threadState = null;
    synchronized (this) {
      while (true) {
        if (freeCount > 0) {
          // Important that we are LIFO here! This way if number of concurrent indexing threads was once high, but has now reduced, we only use a
          // limited number of thread states:
          threadState = freeList[freeCount-1];

          if (threadState.dwpt == null) {
            // This thread-state is not initialized, e.g. it
            // was just flushed. See if we can instead find
            // another free thread state that already has docs
            // indexed. This way if incoming thread concurrency
            // has decreased, we don't leave docs
            // indefinitely buffered, tying up RAM.  This
            // will instead get those thread states flushed,
            // freeing up RAM for larger segment flushes:
            for(int i=0;i<freeCount;i++) {
              if (freeList[i].dwpt != null) {
                // Use this one instead, and swap it with
                // the un-initialized one:
                ThreadState ts = freeList[i];
                freeList[i] = threadState;
                threadState = ts;
                break;
              }
            }
          }
          freeCount--;
          break;
        } else if (numThreadStatesActive < threadStates.length) {
          // ThreadState is already locked before return by this method:
          return newThreadState();
        } else {
          // Wait until a thread state frees up:
          try {
            wait();
          } catch (InterruptedException ie) {
            throw new ThreadInterruptedException(ie);
          }
        }
      }
    }

    // This could take time, e.g. if the threadState is [briefly] checked for flushing:
    threadState.lock();

    return threadState;
  }

  void release(ThreadState state) {
    state.unlock();
    synchronized (this) {
      assert freeCount < freeList.length;
      freeList[freeCount++] = state;
      // In case any thread is waiting, wake one of them up since we just released a thread state; notify() should be sufficient but we do
      // notifyAll defensively:
      notifyAll();
    }
  }
  
  /**
   * Returns the <i>i</i>th active {@link ThreadState} where <i>i</i> is the
   * given ord.
   * 
   * @param ord
   *          the ordinal of the {@link ThreadState}
   * @return the <i>i</i>th active {@link ThreadState} where <i>i</i> is the
   *         given ord.
   */
  ThreadState getThreadState(int ord) {
    return threadStates[ord];
  }

  /**
   * Returns the ThreadState with the minimum estimated number of threads
   * waiting to acquire its lock or <code>null</code> if no {@link ThreadState}
   * is yet visible to the calling thread.
   */
  ThreadState minContendedThreadState() {
    ThreadState minThreadState = null;
    final int limit = numThreadStatesActive;
    for (int i = 0; i < limit; i++) {
      final ThreadState state = threadStates[i];
      if (minThreadState == null || state.getQueueLength() < minThreadState.getQueueLength()) {
        minThreadState = state;
      }
    }
    return minThreadState;
  }
  
  /**
   * Returns the number of currently deactivated {@link ThreadState} instances.
   * A deactivated {@link ThreadState} should not be used for indexing anymore.
   * 
   * @return the number of currently deactivated {@link ThreadState} instances.
   */
  int numDeactivatedThreadStates() {
    int count = 0;
    for (int i = 0; i < threadStates.length; i++) {
      final ThreadState threadState = threadStates[i];
      threadState.lock();
      try {
        if (!threadState.isActive) {
          count++;
        }
      } finally {
        threadState.unlock();
      }
    }
    return count;
  }

  /**
   * Deactivates an active {@link ThreadState}. Inactive {@link ThreadState} can
   * not be used for indexing anymore once they are deactivated. This method should only be used
   * if the parent {@link DocumentsWriter} is closed or aborted.
   * 
   * @param threadState the state to deactivate
   */
  void deactivateThreadState(ThreadState threadState) {
    assert threadState.isActive();
    threadState.deactivate();
  }
}
