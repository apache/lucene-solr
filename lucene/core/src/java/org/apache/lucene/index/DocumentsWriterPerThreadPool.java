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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.lucene.util.IOSupplier;
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
final class DocumentsWriterPerThreadPool implements Iterable<DocumentsWriterPerThread> {

  private final Set<DocumentsWriterPerThread> dwpts = Collections.newSetFromMap(new IdentityHashMap<>());

  private final List<DocumentsWriterPerThread> freeList = new ArrayList<>();
  private final IOSupplier<DocumentsWriterPerThread> factory;

  private int takenWriterPermits = 0;


  public DocumentsWriterPerThreadPool(IOSupplier<DocumentsWriterPerThread> factory) {
    this.factory = factory;
  }

  /**
   * Returns the active number of {@link DocumentsWriterPerThread} instances.
   */
  synchronized int getActiveThreadStateCount() {
    return dwpts.size();
  }

  synchronized void lockNewWriters() {
    // this is similar to a semaphore - we need to acquire all permits ie. takenWriterPermits must be == 0
    // any call to lockNewThreadStates() must be followed by unlockNewThreadStates() otherwise we will deadlock at some
    // point
    assert takenWriterPermits >= 0;
    takenWriterPermits++;
  }

  synchronized void unlockNewWriters() {
    assert takenWriterPermits > 0;
    takenWriterPermits--;
    if (takenWriterPermits == 0) {
      notifyAll();
    }
  }
  /**
   * Returns a new {@link DocumentsWriterPerThread} iff any new state is available otherwise
   * <code>null</code>.
   * <p>
   * NOTE: the returned {@link DocumentsWriterPerThread} is already locked iff non-
   * <code>null</code>.
   * 
   * @return a new {@link DocumentsWriterPerThread} iff any new state is available otherwise
   *         <code>null</code>
   */
  private synchronized DocumentsWriterPerThread newThreadState() throws IOException {
    assert takenWriterPermits >= 0;
    while (takenWriterPermits > 0) {
      // we can't create new thread-states while not all permits are available
      try {
        wait();
      } catch (InterruptedException ie) {
        throw new ThreadInterruptedException(ie);
      }
    }
    DocumentsWriterPerThread threadState = factory.get();
    threadState.lock(); // lock so nobody else will get this ThreadState
    dwpts.add(threadState);
    return threadState;
}

  // TODO: maybe we should try to do load leveling here: we want roughly even numbers
  // of items (docs, deletes, DV updates) to most take advantage of concurrency while flushing

  /** This method is used by DocumentsWriter/FlushControl to obtain a ThreadState to do an indexing operation (add/updateDocument). */
  DocumentsWriterPerThread getAndLock() throws IOException {
    synchronized (this) {
      // Important that we are LIFO here! This way if number of concurrent indexing threads was once high, but has now reduced, we only use a
      // limited number of thread states:
      for (int i = freeList.size()-1; i >= 0; i--) {
        DocumentsWriterPerThread perThread = freeList.get(i);
        if (perThread.tryLock()) {
          freeList.remove(i);
          return perThread;
        }
      }
      // ThreadState is already locked before return by this method:
      return newThreadState();
    }
  }

  void release(DocumentsWriterPerThread state) {
    synchronized (this) {
      if (dwpts.contains(state)) {
        freeList.add(state);
      }
    }
    state.unlock();

  }

  @Override
  public synchronized Iterator<DocumentsWriterPerThread> iterator() {
    return List.copyOf(dwpts).iterator(); // copy on read - this is a quick op since num states is low
  }

  public List<DocumentsWriterPerThread> drain(Predicate<DocumentsWriterPerThread> predicate) {
    List<DocumentsWriterPerThread> list = new ArrayList<>();
    for (DocumentsWriterPerThread perThread : this) {
      if (predicate.test(perThread)) {
        perThread.lock();
        if (checkout(perThread)) {
          list.add(perThread);
        } else {
          perThread.unlock();
        }
      }
    }
    return Collections.unmodifiableList(list);
  }

  synchronized boolean checkout(DocumentsWriterPerThread perThread) {
   assert perThread.isHeldByCurrentThread();
    synchronized (this) {
      if (dwpts.remove(perThread)) {
        freeList.remove(perThread);
      } else {
        assert freeList.contains(perThread) == false;
        return false;
      }
    }
    return true;
  }

  synchronized boolean isRegistered(DocumentsWriterPerThread perThread) {
    return dwpts.contains(perThread);
  }
}
