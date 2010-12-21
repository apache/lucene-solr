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

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.document.Document;
import org.apache.lucene.util.ThreadInterruptedException;

abstract class DocumentsWriterThreadPool {
  public static abstract class Task<T> {
    private boolean clearThreadBindings = false;

    protected void clearThreadBindings() {
      this.clearThreadBindings = true;
    }

    boolean doClearThreadBindings() {
      return clearThreadBindings;
    }
  }

  public static abstract class PerThreadTask<T> extends Task<T> {
    abstract T process(final DocumentsWriterPerThread perThread) throws IOException;
  }

  public static abstract class AllThreadsTask<T> extends Task<T> {
    abstract T process(final Iterator<DocumentsWriterPerThread> threadsIterator) throws IOException;
  }

  protected abstract static class ThreadState {
    private DocumentsWriterPerThread perThread;
    private boolean isIdle = true;

    void start() {/* extension hook */}
    void finish() {/* extension hook */}
  }

  private int pauseThreads = 0;

  protected final int maxNumThreadStates;
  protected ThreadState[] allThreadStates = new ThreadState[0];

  private final Lock lock = new ReentrantLock();
  private final Condition threadStateAvailable = lock.newCondition();
  private boolean globalLock;
  private boolean aborting;

  DocumentsWriterThreadPool(int maxNumThreadStates) {
    this.maxNumThreadStates = (maxNumThreadStates < 1) ? IndexWriterConfig.DEFAULT_MAX_THREAD_STATES : maxNumThreadStates;
  }

  public final int getMaxThreadStates() {
    return this.maxNumThreadStates;
  }

  void pauseAllThreads() {
    lock.lock();
    try {
      pauseThreads++;
      while(!allThreadsIdle()) {
        try {
          threadStateAvailable.await();
        } catch (InterruptedException ie) {
          throw new ThreadInterruptedException(ie);
        }
      }
    } finally {
      lock.unlock();
    }
  }

  void resumeAllThreads() {
    lock.lock();
    try {
      pauseThreads--;
      assert pauseThreads >= 0;
      if (0 == pauseThreads) {
        threadStateAvailable.signalAll();
      }
    } finally {
      lock.unlock();
    }
  }

  private boolean allThreadsIdle() {
    for (ThreadState state : allThreadStates) {
      if (!state.isIdle) {
        return false;
      }
    }

    return true;
  }

  void abort() throws IOException {
    lock.lock();
    try {
      if (!aborting) {
        aborting = true;
        pauseAllThreads();
        for (ThreadState state : allThreadStates) {
          state.perThread.aborting = true;
        }

        try {
          for (ThreadState state : allThreadStates) {
            state.perThread.abort();
          }

        } finally {
          aborting = false;
          resumeAllThreads();
        }
      }
    } finally {
      lock.unlock();
    }
  }

  void finishAbort() {
    aborting = false;
    resumeAllThreads();
  }

  public <T> T executeAllThreads(DocumentsWriter documentsWriter, AllThreadsTask<T> task) throws IOException {
    T result = null;

    lock.lock();
    try {
      try {
        while (globalLock) {
          threadStateAvailable.await();
        }
      } catch (InterruptedException ie) {
        throw new ThreadInterruptedException(ie);
      }

      assert !globalLock;
      globalLock = true;

      pauseAllThreads();

    } finally {
      lock.unlock();
    }

    final ThreadState[] localAllThreads = allThreadStates;

    // all threads are idle now
    boolean success = false;
    try {
      result = task.process(getPerThreadIterator(localAllThreads));
      success = true;
      return result;
    } finally {
      boolean abort = false;
      if (!success) {
        for (ThreadState state : localAllThreads) {
          if (state.perThread.aborting) {
            abort = true;
          }
        }
      }

      lock.lock();
      try {
        try {
          if (task.doClearThreadBindings()) {
            clearAllThreadBindings();
          }
        } finally {
          globalLock = false;
          resumeAllThreads();
          threadStateAvailable.signalAll();
        }
      } finally {
        lock.unlock();
      }

      if (!aborting && abort) {
        documentsWriter.abort();
      }

    }
  }


  public final <T> T executePerThread(DocumentsWriter documentsWriter, Document doc, PerThreadTask<T> task) throws IOException {
    ThreadState state = acquireThreadState(documentsWriter, doc);
    boolean success = false;
    try {
      T result = task.process(state.perThread);
      success = true;
      return result;
    } finally {
      boolean abort = false;
      if (!success && state.perThread.aborting) {
        abort = true;
      }

      returnDocumentsWriterPerThread(state, task.doClearThreadBindings());

      if (!aborting && abort) {
        documentsWriter.abort();
      }
    }
  }

  final Iterator<DocumentsWriterPerThread> getPerThreadIterator() {
    return getPerThreadIterator(allThreadStates);
  }

  private static final Iterator<DocumentsWriterPerThread> getPerThreadIterator(final ThreadState[] localAllThreads) {
    return new Iterator<DocumentsWriterPerThread>() {
      int i = 0;

      @Override
      public boolean hasNext() {
        return i < localAllThreads.length;
      }

      @Override
      public DocumentsWriterPerThread next() {
        return localAllThreads[i++].perThread;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("remove() not supported.");
      }
    };
  }

  protected final <T extends ThreadState> T addNewThreadState(DocumentsWriter documentsWriter, T threadState) {
    // Just create a new "private" thread state
    ThreadState[] newArray = new ThreadState[1+allThreadStates.length];
    if (allThreadStates.length > 0)
      System.arraycopy(allThreadStates, 0, newArray, 0, allThreadStates.length);
    threadState.perThread = documentsWriter.newDocumentsWriterPerThread();
    newArray[allThreadStates.length] = threadState;

    allThreadStates = newArray;
    return threadState;
  }

  protected abstract ThreadState selectThreadState(Thread requestingThread, DocumentsWriter documentsWriter, Document doc);
  protected void clearThreadBindings(ThreadState flushedThread) {
    // subclasses can optionally override this to cleanup after a thread flushed
  }

  protected void clearAllThreadBindings() {
    // subclasses can optionally override this to cleanup after a thread flushed
  }


  private final ThreadState acquireThreadState(DocumentsWriter documentsWriter, Document doc) {
    lock.lock();
    try {
      ThreadState threadState = selectThreadState(Thread.currentThread(), documentsWriter, doc);

      try {
        while (!threadState.isIdle || globalLock || aborting || threadState.perThread.aborting) {
          threadStateAvailable.await();
        }
      } catch (InterruptedException ie) {
        throw new ThreadInterruptedException(ie);
      }

      assert threadState.isIdle;

      threadState.isIdle = false;
      threadState.start();

      return threadState;

    } finally {
      lock.unlock();
    }
  }

  private final void returnDocumentsWriterPerThread(ThreadState state, boolean clearThreadBindings) {
    lock.lock();
    try {
      state.finish();
      if (clearThreadBindings) {
        clearThreadBindings(state);
      }
      state.isIdle = true;
      threadStateAvailable.signalAll();
    } finally {
      lock.unlock();
    }
  }
}
