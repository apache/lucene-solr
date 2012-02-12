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
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
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
  final AtomicBoolean flushDeletes = new AtomicBoolean(false);
  private boolean fullFlush = false;
  private final Queue<DocumentsWriterPerThread> flushQueue = new LinkedList<DocumentsWriterPerThread>();
  // only for safety reasons if a DWPT is close to the RAM limit
  private final Queue<BlockedFlush> blockedFlushes = new LinkedList<BlockedFlush>();
  private final IdentityHashMap<DocumentsWriterPerThread, Long> flushingWriters = new IdentityHashMap<DocumentsWriterPerThread, Long>();


  double maxConfiguredRamBuffer = 0;
  long peakActiveBytes = 0;// only with assert
  long peakFlushBytes = 0;// only with assert
  long peakNetBytes = 0;// only with assert
  long peakDelta = 0; // only with assert
  final DocumentsWriterStallControl stallControl;
  private final DocumentsWriterPerThreadPool perThreadPool;
  private final FlushPolicy flushPolicy;
  private boolean closed = false;
  private final DocumentsWriter documentsWriter;
  private final IndexWriterConfig config;

  DocumentsWriterFlushControl(DocumentsWriter documentsWriter,
      IndexWriterConfig config) {
    this.stallControl = new DocumentsWriterStallControl();
    this.perThreadPool = documentsWriter.perThreadPool;
    this.flushPolicy = documentsWriter.flushPolicy;
    this.hardMaxBytesPerDWPT = config.getRAMPerThreadHardLimitMB() * 1024 * 1024;
    this.config = config;
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
  
  long stallLimitBytes() {
    final double maxRamMB = config.getRAMBufferSizeMB();
    return maxRamMB != IndexWriterConfig.DISABLE_AUTO_FLUSH ? (long)(2 * (maxRamMB * 1024 * 1024)) : Long.MAX_VALUE;
  }
  
  private boolean assertMemory() {
    final double maxRamMB = config.getRAMBufferSizeMB();
    if (maxRamMB != IndexWriterConfig.DISABLE_AUTO_FLUSH) {
      // for this assert we must be tolerant to ram buffer changes!
      maxConfiguredRamBuffer = Math.max(maxRamMB, maxConfiguredRamBuffer);
      final long ram = flushBytes + activeBytes;
      final long ramBufferBytes = (long) (maxConfiguredRamBuffer * 1024 * 1024);
      // take peakDelta into account - worst case is that all flushing, pending and blocked DWPT had maxMem and the last doc had the peakDelta 
      final long expected = (2 * (ramBufferBytes)) + ((numPending + numFlushingDWPT() + numBlockedFlushes()) * peakDelta);
      if (peakDelta < (ramBufferBytes >> 1)) {
        /*
         * if we are indexing with very low maxRamBuffer like 0.1MB memory can
         * easily overflow if we check out some DWPT based on docCount and have
         * several DWPT in flight indexing large documents (compared to the ram
         * buffer). This means that those DWPT and their threads will not hit
         * the stall control before asserting the memory which would in turn
         * fail. To prevent this we only assert if the the largest document seen
         * is smaller than the 1/2 of the maxRamBufferMB
         */
        assert ram <= expected : "ram was " + ram + " expected: " + expected
            + " flush mem: " + flushBytes + " activeMem: " + activeBytes
            + " pendingMem: " + numPending + " flushingMem: "
            + numFlushingDWPT() + " blockedMem: " + numBlockedFlushes()
            + " peakDeltaMem: " + peakDelta;
      }
    }
    return true;
  }

  private void commitPerThreadBytes(ThreadState perThread) {
    final long delta = perThread.dwpt.bytesUsed()
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
    peakDelta = Math.max(peakDelta, delta);
    
    return true;
  }

  synchronized DocumentsWriterPerThread doAfterDocument(ThreadState perThread,
      boolean isUpdate) {
    try {
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
        }
      }
      final DocumentsWriterPerThread flushingDWPT;
      if (fullFlush) {
        if (perThread.flushPending) {
          checkoutAndBlock(perThread);
          flushingDWPT = nextPendingFlush();
        } else {
          flushingDWPT = null;
        }
      } else {
       flushingDWPT = tryCheckoutForFlush(perThread);
      }
      return flushingDWPT;
    } finally {
      stallControl.updateStalled(this);
      assert assertMemory();
    }
  }

  synchronized void doAfterFlush(DocumentsWriterPerThread dwpt) {
    assert flushingWriters.containsKey(dwpt);
    try {
      Long bytes = flushingWriters.remove(dwpt);
      flushBytes -= bytes.longValue();
      perThreadPool.recycle(dwpt);
      stallControl.updateStalled(this);
      assert assertMemory();
    } finally {
      notifyAll();
    }
  }
  
  public synchronized void waitForFlush() {
    while (flushingWriters.size() != 0) {
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
    if (perThread.dwpt.getNumDocsInRAM() > 0) {
      perThread.flushPending = true; // write access synced
      final long bytes = perThread.bytesUsed;
      flushBytes += bytes;
      activeBytes -= bytes;
      numPending++; // write access synced
      assert assertMemory();
    } // don't assert on numDocs since we could hit an abort excp. while selecting that dwpt for flushing
    
  }
  
  synchronized void doOnAbort(ThreadState state) {
    try {
      if (state.flushPending) {
        flushBytes -= state.bytesUsed;
      } else {
        activeBytes -= state.bytesUsed;
      }
      assert assertMemory();
      // Take it out of the loop this DWPT is stale
      perThreadPool.replaceForFlush(state, closed);
    } finally {
      stallControl.updateStalled(this);
    }
  }

  synchronized DocumentsWriterPerThread tryCheckoutForFlush(
      ThreadState perThread) {
   return perThread.flushPending ? internalTryCheckOutForFlush(perThread) : null;
  }
  
  private void checkoutAndBlock(ThreadState perThread) {
    perThread.lock();
    try {
      assert perThread.flushPending : "can not block non-pending threadstate";
      assert fullFlush : "can not block if fullFlush == false";
      final DocumentsWriterPerThread dwpt;
      final long bytes = perThread.bytesUsed;
      dwpt = perThreadPool.replaceForFlush(perThread, closed);
      numPending--;
      blockedFlushes.add(new BlockedFlush(dwpt, bytes));
    }finally {
      perThread.unlock();
    }
  }

  private DocumentsWriterPerThread internalTryCheckOutForFlush(
      ThreadState perThread) {
    assert Thread.holdsLock(this);
    assert perThread.flushPending;
    try {
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
            return dwpt;
          }
        } finally {
          perThread.unlock();
        }
      }
      return null;
    } finally {
      stallControl.updateStalled(this);
    }
  }

  @Override
  public String toString() {
    return "DocumentsWriterFlushControl [activeBytes=" + activeBytes
        + ", flushBytes=" + flushBytes + "]";
  }

  DocumentsWriterPerThread nextPendingFlush() {
    int numPending;
    boolean fullFlush;
    synchronized (this) {
      final DocumentsWriterPerThread poll;
      if ((poll = flushQueue.poll()) != null) {
        stallControl.updateStalled(this);
        return poll;
      }
      fullFlush = this.fullFlush;
      numPending = this.numPending;
    }
    if (numPending > 0 && !fullFlush) { // don't check if we are doing a full flush
      final int limit = perThreadPool.getActiveThreadState();
      for (int i = 0; i < limit && numPending > 0; i++) {
        final ThreadState next = perThreadPool.getThreadState(i);
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
    perThreadPool.deactivateUnreleasedStates();
  }

  /**
   * Returns an iterator that provides access to all currently active {@link ThreadState}s 
   */
  public Iterator<ThreadState> allActiveThreadStates() {
    return getPerThreadsIterator(perThreadPool.getActiveThreadState());
  }
  
  private Iterator<ThreadState> getPerThreadsIterator(final int upto) {
    return new Iterator<ThreadState>() {
      int i = 0;

      public boolean hasNext() {
        return i < upto;
      }

      public ThreadState next() {
        return perThreadPool.getThreadState(i++);
      }

      public void remove() {
        throw new UnsupportedOperationException("remove() not supported.");
      }
    };
  }

  

  synchronized void doOnDelete() {
    // pass null this is a global delete no update
    flushPolicy.onDelete(this, null);
  }

  /**
   * Returns the number of delete terms in the global pool
   */
  public int getNumGlobalTermDeletes() {
    return documentsWriter.deleteQueue.numGlobalTermDeletes() + documentsWriter.indexWriter.bufferedDeletesStream.numTerms();
  }
  
  public long getDeleteBytesUsed() {
    return documentsWriter.deleteQueue.bytesUsed() + documentsWriter.indexWriter.bufferedDeletesStream.bytesUsed();
  }

  synchronized int numFlushingDWPT() {
    return flushingWriters.size();
  }
  
  public boolean doApplyAllDeletes() {	
    return flushDeletes.getAndSet(false);
  }

  public void setApplyAllDeletes() {	
    flushDeletes.set(true);
  }
  
  int numActiveDWPT() {
    return this.perThreadPool.getActiveThreadState();
  }
  
  ThreadState obtainAndLock() {
    final ThreadState perThread = perThreadPool.getAndLock(Thread
        .currentThread(), documentsWriter);
    boolean success = false;
    try {
      if (perThread.isActive()
          && perThread.dwpt.deleteQueue != documentsWriter.deleteQueue) {
        // There is a flush-all in process and this DWPT is
        // now stale -- enroll it for flush and try for
        // another DWPT:
        addFlushableState(perThread);
      }
      success = true;
      // simply return the ThreadState even in a flush all case sine we already hold the lock
      return perThread;
    } finally {
      if (!success) { // make sure we unlock if this fails
        perThread.unlock();
      }
    }
  }
  
  void markForFullFlush() {
    final DocumentsWriterDeleteQueue flushingQueue;
    synchronized (this) {
      assert !fullFlush : "called DWFC#markForFullFlush() while full flush is still running";
      assert fullFlushBuffer.isEmpty() : "full flush buffer should be empty: "+ fullFlushBuffer;
      fullFlush = true;
      flushingQueue = documentsWriter.deleteQueue;
      // Set a new delete queue - all subsequent DWPT will use this queue until
      // we do another full flush
      DocumentsWriterDeleteQueue newQueue = new DocumentsWriterDeleteQueue(flushingQueue.generation+1);
      documentsWriter.deleteQueue = newQueue;
    }
    final int limit = perThreadPool.getActiveThreadState();
    for (int i = 0; i < limit; i++) {
      final ThreadState next = perThreadPool.getThreadState(i);
      next.lock();
      try {
        if (!next.isActive()) {
          continue; 
        }
        assert next.dwpt.deleteQueue == flushingQueue
            || next.dwpt.deleteQueue == documentsWriter.deleteQueue : " flushingQueue: "
            + flushingQueue
            + " currentqueue: "
            + documentsWriter.deleteQueue
            + " perThread queue: "
            + next.dwpt.deleteQueue
            + " numDocsInRam: " + next.dwpt.getNumDocsInRAM();
        if (next.dwpt.deleteQueue != flushingQueue) {
          // this one is already a new DWPT
          continue;
        }
        addFlushableState(next);
      } finally {
        next.unlock();
      }
    }
    synchronized (this) {
      /* make sure we move all DWPT that are where concurrently marked as
       * pending and moved to blocked are moved over to the flushQueue. There is
       * a chance that this happens since we marking DWPT for full flush without
       * blocking indexing.*/
      pruneBlockedQueue(flushingQueue);   
      assert assertBlockedFlushes(documentsWriter.deleteQueue);
      flushQueue.addAll(fullFlushBuffer);
      fullFlushBuffer.clear();
      stallControl.updateStalled(this);
    }
    assert assertActiveDeleteQueue(documentsWriter.deleteQueue);
  }
  
  private boolean assertActiveDeleteQueue(DocumentsWriterDeleteQueue queue) {
    final int limit = perThreadPool.getActiveThreadState();
    for (int i = 0; i < limit; i++) {
      final ThreadState next = perThreadPool.getThreadState(i);
      next.lock();
      try {
        assert !next.isActive() || next.dwpt.deleteQueue == queue;
      } finally {
        next.unlock();
      }
    }
    return true;
  }

  private final List<DocumentsWriterPerThread> fullFlushBuffer = new ArrayList<DocumentsWriterPerThread>();

  void addFlushableState(ThreadState perThread) {
    if (documentsWriter.infoStream.isEnabled("DWFC")) {
      documentsWriter.infoStream.message("DWFC", Thread.currentThread().getName() + ": addFlushableState " + perThread.dwpt);
    }
    final DocumentsWriterPerThread dwpt = perThread.dwpt;
    assert perThread.isHeldByCurrentThread();
    assert perThread.isActive();
    assert fullFlush;
    assert dwpt.deleteQueue != documentsWriter.deleteQueue;
    if (dwpt.getNumDocsInRAM() > 0) {
      synchronized(this) {
        if (!perThread.flushPending) {
          setFlushPending(perThread);
        }
        final DocumentsWriterPerThread flushingDWPT = internalTryCheckOutForFlush(perThread);
        assert flushingDWPT != null : "DWPT must never be null here since we hold the lock and it holds documents";
        assert dwpt == flushingDWPT : "flushControl returned different DWPT";
        fullFlushBuffer.add(flushingDWPT);
      }
    } else {
      if (closed) {
        perThreadPool.deactivateThreadState(perThread); // make this state inactive
      } else {
        perThreadPool.reinitThreadState(perThread);
      }
    }
  }
  
  /**
   * Prunes the blockedQueue by removing all DWPT that are associated with the given flush queue. 
   */
  private void pruneBlockedQueue(final DocumentsWriterDeleteQueue flushingQueue) {
    Iterator<BlockedFlush> iterator = blockedFlushes.iterator();
    while (iterator.hasNext()) {
      BlockedFlush blockedFlush = iterator.next();
      if (blockedFlush.dwpt.deleteQueue == flushingQueue) {
        iterator.remove();
        assert !flushingWriters.containsKey(blockedFlush.dwpt) : "DWPT is already flushing";
        // Record the flushing DWPT to reduce flushBytes in doAfterFlush
        flushingWriters.put(blockedFlush.dwpt, Long.valueOf(blockedFlush.bytes));
        // don't decr pending here - its already done when DWPT is blocked
        flushQueue.add(blockedFlush.dwpt);
      }
    }
  }
  
  synchronized void finishFullFlush() {
    assert fullFlush;
    assert flushQueue.isEmpty();
    assert flushingWriters.isEmpty();
    try {
      if (!blockedFlushes.isEmpty()) {
        assert assertBlockedFlushes(documentsWriter.deleteQueue);
        pruneBlockedQueue(documentsWriter.deleteQueue);
        assert blockedFlushes.isEmpty();
      }
    } finally {
      fullFlush = false;
      stallControl.updateStalled(this);
    }
  }
  
  boolean assertBlockedFlushes(DocumentsWriterDeleteQueue flushingQueue) {
    for (BlockedFlush blockedFlush : blockedFlushes) {
      assert blockedFlush.dwpt.deleteQueue == flushingQueue;
    }
    return true;
  }

  synchronized void abortFullFlushes() {
    try {
      for (DocumentsWriterPerThread dwpt : flushQueue) {
        doAfterFlush(dwpt);
        try {
          dwpt.abort();
        } catch (IOException ex) {
          // continue
        }
      }
      for (BlockedFlush blockedFlush : blockedFlushes) {
        flushingWriters
            .put(blockedFlush.dwpt, Long.valueOf(blockedFlush.bytes));
        doAfterFlush(blockedFlush.dwpt);
        try {
          blockedFlush.dwpt.abort();
        } catch (IOException ex) {
          // continue
        }
      }
    } finally {
      fullFlush = false;
      flushQueue.clear();
      blockedFlushes.clear();
      stallControl.updateStalled(this);
    }
  }
  
  /**
   * Returns <code>true</code> if a full flush is currently running
   */
  synchronized boolean isFullFlush() {
    return fullFlush;
  }

  /**
   * Returns the number of flushes that are already checked out but not yet
   * actively flushing
   */
  synchronized int numQueuedFlushes() {
    return flushQueue.size();
  }

  /**
   * Returns the number of flushes that are checked out but not yet available
   * for flushing. This only applies during a full flush if a DWPT needs
   * flushing but must not be flushed until the full flush has finished.
   */
  synchronized int numBlockedFlushes() {
    return blockedFlushes.size();
  }
  
  private static class BlockedFlush {
    final DocumentsWriterPerThread dwpt;
    final long bytes;
    BlockedFlush(DocumentsWriterPerThread dwpt, long bytes) {
      super();
      this.dwpt = dwpt;
      this.bytes = bytes;
    }
  }

  /**
   * This method will block if too many DWPT are currently flushing and no
   * checked out DWPT are available
   */
  void waitIfStalled() {
      stallControl.waitIfStalled();
  }

  /**
   * Returns <code>true</code> iff stalled
   */
  boolean anyStalledThreads() {
    return stallControl.anyStalledThreads();
  }
  
  
}
