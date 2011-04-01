package org.apache.lucene.index;

import java.util.Iterator;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.FieldInfos.FieldNumberBiMap;
import org.apache.lucene.index.SegmentCodecs.SegmentCodecsBuilder;
import org.apache.lucene.index.codecs.CodecProvider;

public abstract class DocumentsWriterPerThreadPool {
  
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
    // public for FlushPolicy
    DocumentsWriterPerThread perThread;
    // write access guarded by DocumentsWriterFlushControl
    volatile boolean flushPending = false;
    // write access guarded by DocumentsWriterFlushControl
    long perThreadBytes = 0;
    
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
      this.perThreadBytes = 0;
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
      return perThreadBytes;
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

  public DocumentsWriterPerThreadPool(int maxNumPerThreads) {
    maxNumPerThreads = (maxNumPerThreads < 1) ? IndexWriterConfig.DEFAULT_MAX_THREAD_STATES : maxNumPerThreads;
    this.perThreads = new ThreadState[maxNumPerThreads];

    numThreadStatesActive = 0;
  }

  public void initialize(DocumentsWriter documentsWriter, FieldNumberBiMap globalFieldMap, IndexWriterConfig config) {
    codecProvider = config.getCodecProvider();
    this.globalFieldMap = globalFieldMap;
    for (int i = 0; i < perThreads.length; i++) {
      final FieldInfos infos = globalFieldMap.newFieldInfos(SegmentCodecsBuilder.create(codecProvider));
      perThreads[i] = new ThreadState(new DocumentsWriterPerThread(documentsWriter.directory, documentsWriter, infos, documentsWriter.chain));
    }
  }

  public int getMaxThreadStates() {
    return perThreads.length;
  }

  public synchronized ThreadState newThreadState() {
    if (numThreadStatesActive < perThreads.length) {
      return perThreads[numThreadStatesActive++];
    }
    return null;
  }
  
  protected DocumentsWriterPerThread replaceForFlush(ThreadState threadState, boolean closed) {
    assert threadState.isHeldByCurrentThread();
    final DocumentsWriterPerThread dwpt = threadState.perThread;
    if (!closed) {
      final FieldInfos infos = globalFieldMap.newFieldInfos(SegmentCodecsBuilder.create(codecProvider));
      threadState.resetWriter(new DocumentsWriterPerThread(dwpt, infos));
    } else {
      threadState.resetWriter(null);
    }
    return dwpt;
  }
  
  public void recycle(DocumentsWriterPerThread dwpt) {
    // don't recycle DWPT by default
  }
  
  public abstract ThreadState getAndLock(Thread requestingThread, DocumentsWriter documentsWriter, Document doc);

  public abstract void clearThreadBindings(ThreadState perThread);

  public abstract void clearAllThreadBindings();

  /**
   * Returns an iterator providing access to all {@link ThreadState}
   * instances. 
   */
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
