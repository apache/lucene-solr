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
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

import org.apache.lucene.index.DocumentsWriterPerThreadPool.ThreadState;

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
      if (oldState == 0)
        return true;
      if (compareAndSetState(oldState, 0)) {
        releaseShared(0);
        return true;
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
  void updateStalled(DocumentsWriterFlushControl flushControl) {
    do {
      // if we have more flushing / blocked DWPT than numActiveDWPT we stall!
      // don't stall if we have queued flushes - threads should be hijacked instead
      while (flushControl.netBytes() > flushControl.stallLimitBytes()) {
        if (sync.trySetStalled()) {
          assert wasStalled = true;
          return;
        }
      }
    } while (!sync.tryReset());
  }

  void waitIfStalled() {
    sync.acquireShared(0);
  }
  
  boolean hasBlocked() { // for tests
    return sync.hasBlockedThreads;
  }
}