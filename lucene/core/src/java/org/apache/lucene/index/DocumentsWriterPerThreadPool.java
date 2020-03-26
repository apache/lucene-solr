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
package org.apache.lucene.index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.locks.ReentrantLock;

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
final class DocumentsWriterPerThreadPool implements Iterable<DocumentsWriterPerThreadPool.ThreadState> {

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

    // set by DocumentsWriter after each indexing op finishes
    volatile long lastSeqNo;

    ThreadState(DocumentsWriterPerThread dpwt) {
      this.dwpt = dpwt;
    }
    
    private void reset() {
      assert this.isHeldByCurrentThread();
      this.dwpt = null;
      this.bytesUsed = 0;
      this.flushPending = false;
    }
    
    boolean isInitialized() {
      assert this.isHeldByCurrentThread();
      return dwpt != null;
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
    boolean isFlushPending() {
      return flushPending;
    }
  }

  private final List<ThreadState> threadStates = new ArrayList<>();

  private final PriorityQueue<ThreadState> freeList = new PriorityQueue<>(8,
      Collections.reverseOrder(Comparator.comparingLong(c -> c.bytesUsed))); // biggest first

  private int takenThreadStatePermits = 0;

  /**
   * Returns the active number of {@link ThreadState} instances.
   */
  synchronized int getActiveThreadStateCount() {
    return threadStates.size();
  }

  synchronized void lockNewThreadStates() {
    // this is similar to a semaphore - we need to acquire all permits ie. takenThreadStatePermits must be == 0
    // any call to lockNewThreadStates() must be followed by unlockNewThreadStates() otherwise we will deadlock at some
    // point
    assert takenThreadStatePermits >= 0;
    takenThreadStatePermits++;
  }

  synchronized void unlockNewThreadStates() {
    assert takenThreadStatePermits > 0;
    takenThreadStatePermits--;
    if (takenThreadStatePermits == 0) {
      notifyAll();
    }
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
  private synchronized ThreadState newThreadState() {
    assert takenThreadStatePermits >= 0;
    while (takenThreadStatePermits > 0) {
      // we can't create new thread-states while not all permits are available
      try {
        wait();
      } catch (InterruptedException ie) {
        throw new ThreadInterruptedException(ie);
      }
    }
    ThreadState threadState = new ThreadState(null);
    threadState.lock(); // lock so nobody else will get this ThreadState
    threadStates.add(threadState);
    return threadState;
}

  DocumentsWriterPerThread reset(ThreadState threadState) {
    assert threadState.isHeldByCurrentThread();
    final DocumentsWriterPerThread dwpt = threadState.dwpt;
    threadState.reset();
    return dwpt;
  }
  
  // TODO: maybe we should try to do load leveling here: we want roughly even numbers
  // of items (docs, deletes, DV updates) to most take advantage of concurrency while flushing

  /** This method is used by DocumentsWriter/FlushControl to obtain a ThreadState to do an indexing operation (add/updateDocument). */
  ThreadState getAndLock() {
    ThreadState threadState;
    synchronized (this) {
      if (freeList.size() == 0) {
        // ThreadState is already locked before return by this method:
        return newThreadState();
      } else {
        // We poll the biggest DWPT first to ensure that we fill up DWPTs in order to produces larger segments
        // and reduce the amount of merging that needs to be done after flush. If we have many indexing
        // threads we will distribute load across multiple DWPTs and once the number threads drops we will
        // make sure we fill up on after another until they get flushed. We don't re-use thread-states anymore
        // if they are released without a DWPT that way we will reduce the number of threadstates automatically.
        threadState = freeList.poll();
      }
    }
    // This could take time, e.g. if the threadState is [briefly] checked for flushing:
    threadState.lock();
    return threadState;
  }

  void release(ThreadState state) {
    state.unlock();
    synchronized (this) {
      if (state.dwpt != null) { // we don't need to add back flushed thread-states
        freeList.add(state);
      } else {
        threadStates.remove(state); // remove it from the thread states list
      }
    }
  }

  @Override
  public synchronized Iterator<ThreadState> iterator() {
    return List.copyOf(threadStates).iterator(); // copy on read - this is a quick op since num states is low
  }
}
