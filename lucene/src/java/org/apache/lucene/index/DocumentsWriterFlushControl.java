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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.index.DocumentsWriterPerThreadPool.ThreadState;
import org.apache.lucene.util.ThreadInterruptedException;

/**
 * This class controls {@link DocumentsWriterPerThread} flushing during
 * indexing. It tracks the memory consumption per
 * {@link DocumentsWriterPerThread} and uses a configured {@link FlushPolicy} to
 * decide if a {@link DocumentsWriterPerThread} must flush.
 * <p>
 * In addition to the {@link FlushPolicy} the flush control might set certain
 * {@link DocumentsWriterPerThread} as flush pending iff a
 * {@link DocumentsWriterPerThread} exceeds the
 * {@link IndexWriterConfig#getRAMPerThreadHardLimitMB()} to prevent address
 * space exhaustion.
 */
public final class DocumentsWriterFlushControl {

  private final long hardMaxBytesPerDWPT;
  private long activeBytes = 0;
  private long flushBytes = 0;
  private volatile int numPending = 0;
  private volatile int numFlushing = 0;
  final AtomicBoolean flushDeletes = new AtomicBoolean(false);
  private boolean fullFlush = false;
  private Queue<DocumentsWriterPerThread> flushQueue = new LinkedList<DocumentsWriterPerThread>();
  // only for safety reasons if a DWPT is close to the RAM limit
  private Queue<DocumentsWriterPerThread> blockedFlushes = new LinkedList<DocumentsWriterPerThread>();


  long peakActiveBytes = 0;// only with assert
  long peakFlushBytes = 0;// only with assert
  long peakNetBytes = 0;// only with assert
  private final Healthiness healthiness;
  private final DocumentsWriterPerThreadPool perThreadPool;
  private final FlushPolicy flushPolicy;
  private boolean closed = false;
  private final HashMap<DocumentsWriterPerThread, Long> flushingWriters = new HashMap<DocumentsWriterPerThread, Long>();
  private final DocumentsWriter documentsWriter;

  DocumentsWriterFlushControl(DocumentsWriter documentsWriter,
      Healthiness healthiness, long hardMaxBytesPerDWPT) {
    this.healthiness = healthiness;
    this.perThreadPool = documentsWriter.perThreadPool;
    this.flushPolicy = documentsWriter.flushPolicy;
    this.hardMaxBytesPerDWPT = hardMaxBytesPerDWPT;
    this.documentsWriter = documentsWriter;
  }

  public synchronized long activeBytes() {
    return activeBytes;
  }

  public synchronized long flushBytes() {
    return flushBytes;
  }

  public synchronized long netBytes() {
    return flushBytes + activeBytes;
  }

  private void commitPerThreadBytes(ThreadState perThread) {
    final long delta = perThread.perThread.bytesUsed()
        - perThread.bytesUsed;
    perThread.bytesUsed += delta;
    /*
     * We need to differentiate here if we are pending since setFlushPending
     * moves the perThread memory to the flushBytes and we could be set to
     * pending during a delete
     */
    if (perThread.flushPending) {
      flushBytes += delta;
    } else {
      activeBytes += delta;
    }
    assert updatePeaks(delta);
  }

  // only for asserts
  private boolean updatePeaks(long delta) {
    peakActiveBytes = Math.max(peakActiveBytes, activeBytes);
    peakFlushBytes = Math.max(peakFlushBytes, flushBytes);
    peakNetBytes = Math.max(peakNetBytes, netBytes());
    return true;
  }

  synchronized DocumentsWriterPerThread doAfterDocument(ThreadState perThread,
      boolean isUpdate) {
    commitPerThreadBytes(perThread);
    if (!perThread.flushPending) {
      if (isUpdate) {
        flushPolicy.onUpdate(this, perThread);
      } else {
        flushPolicy.onInsert(this, perThread);
      }
      if (!perThread.flushPending && perThread.bytesUsed > hardMaxBytesPerDWPT) {
        // Safety check to prevent a single DWPT exceeding its RAM limit. This
        // is super important since we can not address more than 2048 MB per DWPT
        setFlushPending(perThread);
        if (fullFlush) {
          DocumentsWriterPerThread toBlock = internalTryCheckOutForFlush(perThread);
          assert toBlock != null;
          blockedFlushes.add(toBlock);
        }
      }
    }
    final DocumentsWriterPerThread flushingDWPT = tryCheckoutForFlush(perThread);
    healthiness.updateStalled(this);
    return flushingDWPT;
  }

  synchronized void doAfterFlush(DocumentsWriterPerThread dwpt) {
    assert flushingWriters.containsKey(dwpt);
    try {
      numFlushing--;
      Long bytes = flushingWriters.remove(dwpt);
      flushBytes -= bytes.longValue();
      perThreadPool.recycle(dwpt);
      healthiness.updateStalled(this);
    } finally {
      notifyAll();
    }
  }
  
  public synchronized boolean anyFlushing() {
    return numFlushing != 0;
  }
  
  public synchronized void waitForFlush() {
    if (numFlushing != 0) {
      try {
        this.wait();
      } catch (InterruptedException e) {
        throw new ThreadInterruptedException(e);
      }
    }
  }

  /**
   * Sets flush pending state on the given {@link ThreadState}. The
   * {@link ThreadState} must have indexed at least on Document and must not be
   * already pending.
   */
  public synchronized void setFlushPending(ThreadState perThread) {
    assert !perThread.flushPending;
    if (perThread.perThread.getNumDocsInRAM() > 0) {
      perThread.flushPending = true; // write access synced
      final long bytes = perThread.bytesUsed;
      flushBytes += bytes;
      activeBytes -= bytes;
      numPending++; // write access synced
    } // don't assert on numDocs since we could hit an abort excp. while selecting that dwpt for flushing
    
  }

  synchronized void doOnAbort(ThreadState state) {
    if (state.flushPending) {
      flushBytes -= state.bytesUsed;
    } else {
      activeBytes -= state.bytesUsed;
    }
    // Take it out of the loop this DWPT is stale
    perThreadPool.replaceForFlush(state, closed);
    healthiness.updateStalled(this);
  }

  synchronized DocumentsWriterPerThread tryCheckoutForFlush(
      ThreadState perThread) {
    if (fullFlush) {
      return null;
    }
    return internalTryCheckOutForFlush(perThread);
  }

  private DocumentsWriterPerThread internalTryCheckOutForFlush(
      ThreadState perThread) {
    if (perThread.flushPending) {
      // We are pending so all memory is already moved to flushBytes
      if (perThread.tryLock()) {
        try {
          if (perThread.isActive()) {
            assert perThread.isHeldByCurrentThread();
            final DocumentsWriterPerThread dwpt;
            final long bytes = perThread.bytesUsed; // do that before
                                                         // replace!
            dwpt = perThreadPool.replaceForFlush(perThread, closed);
            assert !flushingWriters.containsKey(dwpt) : "DWPT is already flushing";
            // Record the flushing DWPT to reduce flushBytes in doAfterFlush
            flushingWriters.put(dwpt, Long.valueOf(bytes));
            numPending--; // write access synced
            numFlushing++;
            return dwpt;
          }
        } finally {
          perThread.unlock();
        }
      }
    }
    return null;
  }

  @Override
  public String toString() {
    return "DocumentsWriterFlushControl [activeBytes=" + activeBytes
        + ", flushBytes=" + flushBytes + "]";
  }

  DocumentsWriterPerThread nextPendingFlush() {
    synchronized (this) {
      DocumentsWriterPerThread poll = flushQueue.poll();
      if (poll != null) {
        return poll;
      }  
    }
    if (numPending > 0) {
      final Iterator<ThreadState> allActiveThreads = perThreadPool
          .getActivePerThreadsIterator();
      while (allActiveThreads.hasNext() && numPending > 0) {
        ThreadState next = allActiveThreads.next();
        if (next.flushPending) {
          final DocumentsWriterPerThread dwpt = tryCheckoutForFlush(next);
          if (dwpt != null) {
            return dwpt;
          }
        }
      }
    }
    return null;
  }

  synchronized void setClosed() {
    // set by DW to signal that we should not release new DWPT after close
    this.closed = true;
  }

  /**
   * Returns an iterator that provides access to all currently active {@link ThreadState}s 
   */
  public Iterator<ThreadState> allActiveThreads() {
    return perThreadPool.getActivePerThreadsIterator();
  }

  synchronized void doOnDelete() {
    // pass null this is a global delete no update
    flushPolicy.onDelete(this, null);
  }

  /**
   * Returns the number of delete terms in the global pool
   */
  public int getNumGlobalTermDeletes() {
    return documentsWriter.deleteQueue.numGlobalTermDeletes();
  }

  int numFlushingDWPT() {
    return numFlushing;
  }
  
  public boolean doApplyAllDeletes() {	
    return flushDeletes.getAndSet(false);
  }

  public void setApplyAllDeletes() {	
    flushDeletes.set(true);
  }
  
  int numActiveDWPT() {
    return this.perThreadPool.getMaxThreadStates();
  }
  
  void markForFullFlush() {
    final DocumentsWriterDeleteQueue flushingQueue;
    synchronized (this) {
      assert !fullFlush;
      fullFlush = true;
      flushingQueue = documentsWriter.deleteQueue;
      // Set a new delete queue - all subsequent DWPT will use this queue until
      // we do another full flush
      DocumentsWriterDeleteQueue newQueue = new DocumentsWriterDeleteQueue(flushingQueue.generation+1);
      documentsWriter.deleteQueue = newQueue;
    }
    final Iterator<ThreadState> allActiveThreads = perThreadPool
    .getActivePerThreadsIterator();
    final ArrayList<DocumentsWriterPerThread> toFlush = new ArrayList<DocumentsWriterPerThread>();
    while (allActiveThreads.hasNext()) {
      final ThreadState next = allActiveThreads.next();
      next.lock();
      try {
        if (!next.isActive()) {
          continue; 
        }
        assert next.perThread.deleteQueue == flushingQueue
            || next.perThread.deleteQueue == documentsWriter.deleteQueue : " flushingQueue: "
            + flushingQueue
            + " currentqueue: "
            + documentsWriter.deleteQueue
            + " perThread queue: "
            + next.perThread.deleteQueue
            + " numDocsInRam: " + next.perThread.getNumDocsInRAM();
        if (next.perThread.deleteQueue != flushingQueue) {
          // this one is already a new DWPT
          continue;
        }
        if (next.perThread.getNumDocsInRAM() > 0 ) {
          final DocumentsWriterPerThread dwpt = next.perThread; // just for assert
          synchronized (this) {
            if (!next.flushPending) {
              setFlushPending(next);
            }
          }
          final DocumentsWriterPerThread flushingDWPT = internalTryCheckOutForFlush(next);
          assert flushingDWPT != null : "DWPT must never be null here since we hold the lock and it holds documents";
          assert dwpt == flushingDWPT : "flushControl returned different DWPT";
          toFlush.add(flushingDWPT);
        } else {
          // get the new delete queue from DW
          next.perThread.initialize();
        }
      } finally {
        next.unlock();
      }
    }
    synchronized (this) {
      assert assertBlockedFlushes(flushingQueue);
      flushQueue.addAll(blockedFlushes);
      blockedFlushes.clear();
      flushQueue.addAll(toFlush);
    }
  }
  
  synchronized void finishFullFlush() {
    assert fullFlush;
    assert flushQueue.isEmpty();
    try {
      if (!blockedFlushes.isEmpty()) {
        assert assertBlockedFlushes(documentsWriter.deleteQueue);
        flushQueue.addAll(blockedFlushes);
        blockedFlushes.clear();
      }
    } finally {
      fullFlush = false;
    }
  }
  
  boolean assertBlockedFlushes(DocumentsWriterDeleteQueue flushingQueue) {
    Queue<DocumentsWriterPerThread> flushes = this.blockedFlushes;
    for (DocumentsWriterPerThread documentsWriterPerThread : flushes) {
      assert documentsWriterPerThread.deleteQueue == flushingQueue;
    }
    return true;
  }

  synchronized void abortFullFlushes() {
    try {
      for (DocumentsWriterPerThread dwpt : flushQueue) {
        doAfterFlush(dwpt);
      }
      for (DocumentsWriterPerThread dwpt : blockedFlushes) {
        doAfterFlush(dwpt);
      }
      
    } finally {
      fullFlush = false;
      flushQueue.clear();
      blockedFlushes.clear();
    }
  }
  
  synchronized boolean isFullFlush() {
    return fullFlush;
  }
}