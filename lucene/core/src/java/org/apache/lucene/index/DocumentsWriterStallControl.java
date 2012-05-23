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
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

import org.apache.lucene.index.DocumentsWriterPerThreadPool.ThreadState;
import org.apache.lucene.util.ThreadInterruptedException;

/**
 * Controls the health status of a {@link DocumentsWriter} sessions. This class
 * used to block incoming indexing threads if flushing significantly slower than
 * indexing to ensure the {@link DocumentsWriter}s healthiness. If flushing is
 * significantly slower than indexing the net memory used within an
 * {@link IndexWriter} session can increase very quickly and easily exceed the
 * JVM's available memory.
 * <p>
 * To prevent OOM Errors and ensure IndexWriter's stability this class blocks
 * incoming threads from indexing once 2 x number of available
 * {@link ThreadState}s in {@link DocumentsWriterPerThreadPool} is exceeded.
 * Once flushing catches up and the number of flushing DWPT is equal or lower
 * than the number of active {@link ThreadState}s threads are released and can
 * continue indexing.
 */
final class DocumentsWriterStallControl {
  @SuppressWarnings("serial")
  private static final class Sync extends AbstractQueuedSynchronizer {
    volatile boolean hasBlockedThreads = false; // only with assert

    Sync() {
      setState(0);
    }

    boolean isHealthy() {
      return getState() == 0;
    }

    boolean trySetStalled() {
      int state = getState();
      return compareAndSetState(state, state + 1);
    }

    boolean tryReset() {
      final int oldState = getState();
      if (oldState == 0) {
        return true;
      }
      if (compareAndSetState(oldState, 0)) {
        return releaseShared(0);
      }
      return false;
    }

    @Override
    public int tryAcquireShared(int acquires) {
      assert maybeSetHasBlocked(getState());
      return getState() == 0 ? 1 : -1;
    }

    // only used for testing
    private boolean maybeSetHasBlocked(int state) {
      hasBlockedThreads |= getState() != 0;
      return true;
    }

    @Override
    public boolean tryReleaseShared(int newState) {
      return (getState() == 0);
    }
  }

  private final Sync sync = new Sync();
  volatile boolean wasStalled = false; // only with asserts

  boolean anyStalledThreads() {
    return !sync.isHealthy();
  }

  /**
   * Update the stalled flag status. This method will set the stalled flag to
   * <code>true</code> iff the number of flushing
   * {@link DocumentsWriterPerThread} is greater than the number of active
   * {@link DocumentsWriterPerThread}. Otherwise it will reset the
   * {@link DocumentsWriterStallControl} to healthy and release all threads waiting on
   * {@link #waitIfStalled()}
   */
  void updateStalled(MemoryController controller) {
    do {
      final long netBytes = controller.netBytes();
      final long flushBytes = controller.flushBytes();
      final long limit = controller.stallLimitBytes();
      assert netBytes >= flushBytes;
      assert limit > 0;
      /*
       * we block indexing threads if net byte grows due to slow flushes
       * yet, for small ram buffers and large documents we can easily
       * reach the limit without any ongoing flushes. we need to ensure
       * that we don't stall/block if an ongoing or pending flush can 
       * not free up enough memory to release the stall lock.
       */
      while (netBytes > limit && (netBytes - flushBytes) < limit) {
        if (sync.trySetStalled()) {
          assert wasStalled = true;
          return;
        }
      }
    } while (!sync.tryReset());
  }

  void waitIfStalled() {
    try {
      sync.acquireSharedInterruptibly(0);
    } catch (InterruptedException e) {
      throw new ThreadInterruptedException(e);
    }
  }
  
  boolean hasBlocked() { // for tests
    return sync.hasBlockedThreads;
  }
  
  static interface MemoryController {
    long netBytes();
    long flushBytes();
    long stallLimitBytes();
  }
}
