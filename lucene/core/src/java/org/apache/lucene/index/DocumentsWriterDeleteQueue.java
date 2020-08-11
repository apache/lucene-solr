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

import java.io.Closeable;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

import org.apache.lucene.index.DocValuesUpdate.BinaryDocValuesUpdate;
import org.apache.lucene.index.DocValuesUpdate.NumericDocValuesUpdate;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.InfoStream;

/**
 * {@link DocumentsWriterDeleteQueue} is a non-blocking linked pending deletes
 * queue. In contrast to other queue implementation we only maintain the
 * tail of the queue. A delete queue is always used in a context of a set of
 * DWPTs and a global delete pool. Each of the DWPT and the global pool need to
 * maintain their 'own' head of the queue (as a DeleteSlice instance per
 * {@link DocumentsWriterPerThread}).
 * The difference between the DWPT and the global pool is that the DWPT starts
 * maintaining a head once it has added its first document since for its segments
 * private deletes only the deletes after that document are relevant. The global
 * pool instead starts maintaining the head once this instance is created by
 * taking the sentinel instance as its initial head.
 * <p>
 * Since each {@link DeleteSlice} maintains its own head and the list is only
 * single linked the garbage collector takes care of pruning the list for us.
 * All nodes in the list that are still relevant should be either directly or
 * indirectly referenced by one of the DWPT's private {@link DeleteSlice} or by
 * the global {@link BufferedUpdates} slice.
 * <p>
 * Each DWPT as well as the global delete pool maintain their private
 * DeleteSlice instance. In the DWPT case updating a slice is equivalent to
 * atomically finishing the document. The slice update guarantees a "happens
 * before" relationship to all other updates in the same indexing session. When a
 * DWPT updates a document it:
 * 
 * <ol>
 * <li>consumes a document and finishes its processing</li>
 * <li>updates its private {@link DeleteSlice} either by calling
 * {@link #updateSlice(DeleteSlice)} or {@link #add(Node, DeleteSlice)} (if the
 * document has a delTerm)</li>
 * <li>applies all deletes in the slice to its private {@link BufferedUpdates}
 * and resets it</li>
 * <li>increments its internal document id</li>
 * </ol>
 * 
 * The DWPT also doesn't apply its current documents delete term until it has
 * updated its delete slice which ensures the consistency of the update. If the
 * update fails before the DeleteSlice could have been updated the deleteTerm
 * will also not be added to its private deletes neither to the global deletes.
 * 
 */
final class DocumentsWriterDeleteQueue implements Accountable, Closeable {

  // the current end (latest delete operation) in the delete queue:
  private volatile Node<?> tail;

  private volatile boolean closed = false;

  /** Used to record deletes against all prior (already written to disk) segments.  Whenever any segment flushes, we bundle up this set of
   *  deletes and insert into the buffered updates stream before the newly flushed segment(s). */
  private final DeleteSlice globalSlice;
  private final BufferedUpdates globalBufferedUpdates;
  
  // only acquired to update the global deletes, pkg-private for access by tests:
  final ReentrantLock globalBufferLock = new ReentrantLock();

  final long generation;

  /** Generates the sequence number that IW returns to callers changing the index, showing the effective serialization of all operations. */
  private final AtomicLong nextSeqNo;

  private final InfoStream infoStream;

  private volatile long maxSeqNo = Long.MAX_VALUE;

  private final long startSeqNo;
  private final LongSupplier previousMaxSeqId;
  private boolean advanced;
  
  DocumentsWriterDeleteQueue(InfoStream infoStream) {
    // seqNo must start at 1 because some APIs negate this to also return a boolean
    this(infoStream, 0, 1, () -> 0);
  }

  private DocumentsWriterDeleteQueue(InfoStream infoStream, long generation, long startSeqNo, LongSupplier previousMaxSeqId) {
    this.infoStream = infoStream;
    this.globalBufferedUpdates = new BufferedUpdates("global");
    this.generation = generation;
    this.nextSeqNo = new AtomicLong(startSeqNo);
    this.startSeqNo = startSeqNo;
    this.previousMaxSeqId = previousMaxSeqId;
    long value = previousMaxSeqId.getAsLong();
    assert value <= startSeqNo : "illegal max sequence ID: " + value + " start was: " + startSeqNo;
    /*
     * we use a sentinel instance as our initial tail. No slice will ever try to
     * apply this tail since the head is always omitted.
     */
    tail = new Node<>(null); // sentinel
    globalSlice = new DeleteSlice(tail);
  }

  long addDelete(Query... queries) {
    long seqNo = add(new QueryArrayNode(queries));
    tryApplyGlobalSlice();
    return seqNo;
  }

  long addDelete(Term... terms) {
    long seqNo = add(new TermArrayNode(terms));
    tryApplyGlobalSlice();
    return seqNo;
  }

  long addDocValuesUpdates(DocValuesUpdate... updates) {
    long seqNo = add(new DocValuesUpdatesNode(updates));
    tryApplyGlobalSlice();
    return seqNo;
  }

  static Node<Term> newNode(Term term) {
    return new TermNode(term);
  }

  static Node<DocValuesUpdate[]> newNode(DocValuesUpdate... updates) {
    return new DocValuesUpdatesNode(updates);
  }

  /**
   * invariant for document update
   */
  long add(Node<?> deleteNode, DeleteSlice slice) {
    long seqNo = add(deleteNode);
    /*
     * this is an update request where the term is the updated documents
     * delTerm. in that case we need to guarantee that this insert is atomic
     * with regards to the given delete slice. This means if two threads try to
     * update the same document with in turn the same delTerm one of them must
     * win. By taking the node we have created for our del term as the new tail
     * it is guaranteed that if another thread adds the same right after us we
     * will apply this delete next time we update our slice and one of the two
     * competing updates wins!
     */
    slice.sliceTail = deleteNode;
    assert slice.sliceHead != slice.sliceTail : "slice head and tail must differ after add";
    tryApplyGlobalSlice(); // TODO doing this each time is not necessary maybe
    // we can do it just every n times or so?

    return seqNo;
  }

  synchronized long add(Node<?> newNode) {
    ensureOpen();
    tail.next = newNode;
    this.tail = newNode;
    return getNextSequenceNumber();
  }

  boolean anyChanges() {
    globalBufferLock.lock();
    try {
      /*
       * check if all items in the global slice were applied 
       * and if the global slice is up-to-date
       * and if globalBufferedUpdates has changes
       */
      return globalBufferedUpdates.any() || !globalSlice.isEmpty() || globalSlice.sliceTail != tail || tail.next != null;
    } finally {
      globalBufferLock.unlock();
    }
  }

  void tryApplyGlobalSlice() {
    if (globalBufferLock.tryLock()) {
      ensureOpen();
      /*
       * The global buffer must be locked but we don't need to update them if
       * there is an update going on right now. It is sufficient to apply the
       * deletes that have been added after the current in-flight global slices
       * tail the next time we can get the lock!
       */
      try {
        if (updateSliceNoSeqNo(globalSlice)) {
          globalSlice.apply(globalBufferedUpdates, BufferedUpdates.MAX_INT);
        }
      } finally {
        globalBufferLock.unlock();
      }
    }
  }


  FrozenBufferedUpdates freezeGlobalBuffer(DeleteSlice callerSlice) {
    globalBufferLock.lock();
    try {
      ensureOpen();
      /*
       * Here we freeze the global buffer so we need to lock it, apply all
       * deletes in the queue and reset the global slice to let the GC prune the
       * queue.
       */
      final Node<?> currentTail = tail; // take the current tail make this local any
      // Changes after this call are applied later
      // and not relevant here
      if (callerSlice != null) {
        // Update the callers slices so we are on the same page
        callerSlice.sliceTail = currentTail;
      }
      return freezeGlobalBufferInternal(currentTail);
    } finally {
      globalBufferLock.unlock();
    }
  }

  /**
   * This may freeze the global buffer unless the delete queue has already been closed.
   * If the queue has been closed this method will return <code>null</code>
   */
  FrozenBufferedUpdates maybeFreezeGlobalBuffer() {
    globalBufferLock.lock();
    try {
      if (closed == false) {
        /*
         * Here we freeze the global buffer so we need to lock it, apply all
         * deletes in the queue and reset the global slice to let the GC prune the
         * queue.
         */
        return freezeGlobalBufferInternal(tail); // take the current tail make this local any
      } else {
        assert anyChanges() == false : "we are closed but have changes";
        return null;
      }
    } finally {
      globalBufferLock.unlock();
    }
  }

  private FrozenBufferedUpdates freezeGlobalBufferInternal(final Node<?> currentTail ) {
    assert globalBufferLock.isHeldByCurrentThread();
    if (globalSlice.sliceTail != currentTail) {
      globalSlice.sliceTail = currentTail;
      globalSlice.apply(globalBufferedUpdates, BufferedUpdates.MAX_INT);
    }

    if (globalBufferedUpdates.any()) {
      final FrozenBufferedUpdates packet = new FrozenBufferedUpdates(infoStream, globalBufferedUpdates, null);
      globalBufferedUpdates.clear();
      return packet;
    } else {
      return null;
    }
  }

  DeleteSlice newSlice() {
    return new DeleteSlice(tail);
  }

  /** Negative result means there were new deletes since we last applied */
  synchronized long updateSlice(DeleteSlice slice) {
    ensureOpen();
    long seqNo = getNextSequenceNumber();
    if (slice.sliceTail != tail) {
      // new deletes arrived since we last checked
      slice.sliceTail = tail;
      seqNo = -seqNo;
    }
    return seqNo;
  }

  /** Just like updateSlice, but does not assign a sequence number */
  boolean updateSliceNoSeqNo(DeleteSlice slice) {
    if (slice.sliceTail != tail) {
      // new deletes arrived since we last checked
      slice.sliceTail = tail;
      return true;
    }
    return false;
  }

  private void ensureOpen() {
    if (closed) {
      throw new AlreadyClosedException("This " + DocumentsWriterDeleteQueue.class.getSimpleName() + " is already closed");
    }
  }

  public boolean isOpen() {
    return closed == false;
  }

  @Override
  public synchronized void close() {
    globalBufferLock.lock();
    try {
      if (anyChanges()) {
        throw new IllegalStateException("Can't close queue unless all changes are applied");
      }
      this.closed = true;
      long seqNo = nextSeqNo.get();
      assert seqNo <= maxSeqNo : "maxSeqNo must be greater or equal to " + seqNo + " but was " + maxSeqNo;
      nextSeqNo.set(maxSeqNo+1);
    } finally {
      globalBufferLock.unlock();
    }
  }

  static class DeleteSlice {
    // No need to be volatile, slices are thread captive (only accessed by one thread)!
    Node<?> sliceHead; // we don't apply this one
    Node<?> sliceTail;

    DeleteSlice(Node<?> currentTail) {
      assert currentTail != null;
      /*
       * Initially this is a 0 length slice pointing to the 'current' tail of
       * the queue. Once we update the slice we only need to assign the tail and
       * have a new slice
       */
      sliceHead = sliceTail = currentTail;
    }

    void apply(BufferedUpdates del, int docIDUpto) {
      if (sliceHead == sliceTail) {
        // 0 length slice
        return;
      }
      /*
       * When we apply a slice we take the head and get its next as our first
       * item to apply and continue until we applied the tail. If the head and
       * tail in this slice are not equal then there will be at least one more
       * non-null node in the slice!
       */
      Node<?> current = sliceHead;
      do {
        current = current.next;
        assert current != null : "slice property violated between the head on the tail must not be a null node";
        current.apply(del, docIDUpto);
      } while (current != sliceTail);
      reset();
    }

    void reset() {
      // Reset to a 0 length slice
      sliceHead = sliceTail;
    }

    /**
     * Returns <code>true</code> iff the given node is identical to the slices tail,
     * otherwise <code>false</code>.
     */
    boolean isTail(Node<?> node) {
      return sliceTail == node;
    }

    /**
     * Returns <code>true</code> iff the given item is identical to the item
     * hold by the slices tail, otherwise <code>false</code>.
     */
    boolean isTailItem(Object object) {
      return sliceTail.item == object;
    }

    boolean isEmpty() {
      return sliceHead == sliceTail;
    }
  }

  public int numGlobalTermDeletes() {
    return globalBufferedUpdates.numTermDeletes.get();
  }

  void clear() {
    globalBufferLock.lock();
    try {
      final Node<?> currentTail = tail;
      globalSlice.sliceHead = globalSlice.sliceTail = currentTail;
      globalBufferedUpdates.clear();
    } finally {
      globalBufferLock.unlock();
    }
  }

  static class Node<T> {
    volatile Node<?> next;
    final T item;

    Node(T item) {
      this.item = item;
    }

    void apply(BufferedUpdates bufferedDeletes, int docIDUpto) {
      throw new IllegalStateException("sentinel item must never be applied");
    }

    boolean isDelete() {
      return true;
    }
  }

  private static final class TermNode extends Node<Term> {

    TermNode(Term term) {
      super(term);
    }

    @Override
    void apply(BufferedUpdates bufferedDeletes, int docIDUpto) {
      bufferedDeletes.addTerm(item, docIDUpto);
    }

    @Override
    public String toString() {
      return "del=" + item;
    }

  }

  private static final class QueryArrayNode extends Node<Query[]> {
    QueryArrayNode(Query[] query) {
      super(query);
    }

    @Override
    void apply(BufferedUpdates bufferedUpdates, int docIDUpto) {
      for (Query query : item) {
        bufferedUpdates.addQuery(query, docIDUpto);  
      }
    }
  }
  
  private static final class TermArrayNode extends Node<Term[]> {
    TermArrayNode(Term[] term) {
      super(term);
    }

    @Override
    void apply(BufferedUpdates bufferedUpdates, int docIDUpto) {
      for (Term term : item) {
        bufferedUpdates.addTerm(term, docIDUpto);  
      }
    }

    @Override
    public String toString() {
      return "dels=" + Arrays.toString(item);
    }

  }

  private static final class DocValuesUpdatesNode extends Node<DocValuesUpdate[]> {

    DocValuesUpdatesNode(DocValuesUpdate... updates) {
      super(updates);
    }

    @Override
    void apply(BufferedUpdates bufferedUpdates, int docIDUpto) {
      for (DocValuesUpdate update : item) {
        switch (update.type) {
          case NUMERIC:
            bufferedUpdates.addNumericUpdate((NumericDocValuesUpdate) update, docIDUpto);
            break;
          case BINARY:
            bufferedUpdates.addBinaryUpdate((BinaryDocValuesUpdate) update, docIDUpto);
            break;
          default:
            throw new IllegalArgumentException(update.type + " DocValues updates not supported yet!");
        }
      }
    }


    @Override
    boolean isDelete() {
      return false;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("docValuesUpdates: ");
      if (item.length > 0) {
        sb.append("term=").append(item[0].term).append("; updates: [");
        for (DocValuesUpdate update : item) {
          sb.append(update.field).append(':').append(update.valueToString()).append(',');
        }
        sb.setCharAt(sb.length()-1, ']');
      }
      return sb.toString();
    }
  }
  
  private boolean forceApplyGlobalSlice() {
    globalBufferLock.lock();
    final Node<?> currentTail = tail;
    try {
      if (globalSlice.sliceTail != currentTail) {
        globalSlice.sliceTail = currentTail;
        globalSlice.apply(globalBufferedUpdates, BufferedUpdates.MAX_INT);
      }
      return globalBufferedUpdates.any();
    } finally {
      globalBufferLock.unlock();
    }
  }

  public int getBufferedUpdatesTermsSize() {
    globalBufferLock.lock();
    try {
      forceApplyGlobalSlice();
      return globalBufferedUpdates.deleteTerms.size();
    } finally {
      globalBufferLock.unlock();
    }
  }

  @Override
  public long ramBytesUsed() {
    return globalBufferedUpdates.ramBytesUsed();
  }

  @Override
  public String toString() {
    return "DWDQ: [ generation: " + generation + " ]";
  }

  public long getNextSequenceNumber() {
    long seqNo = nextSeqNo.getAndIncrement();
    assert seqNo <= maxSeqNo: "seqNo=" + seqNo + " vs maxSeqNo=" + maxSeqNo;
    return seqNo;
  }  

  long getLastSequenceNumber() {
    return nextSeqNo.get()-1;
  }  

  /** Inserts a gap in the sequence numbers.  This is used by IW during flush or commit to ensure any in-flight threads get sequence numbers
   *  inside the gap */
  void skipSequenceNumbers(long jump) {
    nextSeqNo.addAndGet(jump);
  }

  /**
   * Returns the maximum completed seq no for this queue.
   */
  long getMaxCompletedSeqNo() {
    if (startSeqNo < nextSeqNo.get()) {
      return getLastSequenceNumber();
    } else {
      // if we haven't advanced the seqNo make sure we fall back to the previous queue
      long value = previousMaxSeqId.getAsLong();
      assert value < startSeqNo : "illegal max sequence ID: " + value + " start was: " + startSeqNo;
      return value;
    }
  }

  /**
   * Advances the queue to the next queue on flush. This carries over the the generation to the next queue and
   * set the {@link #getMaxSeqNo()} based on the given maxNumPendingOps. This method can only be called once, subsequently
   * the returned queue should be used.
   * @param maxNumPendingOps the max number of possible concurrent operations that will execute on this queue after
   *                         it was advanced. This corresponds the the number of DWPTs that own the current queue at the
   *                         moment when this queue is advanced since each these DWPTs can increment the seqId after we
   *                         advanced it.
   * @return a new queue as a successor of this queue.
   */
  synchronized DocumentsWriterDeleteQueue advanceQueue(int maxNumPendingOps) {
    if (advanced) {
      throw new IllegalStateException("queue was already advanced");
    }
    advanced = true;
    long seqNo = getLastSequenceNumber() + maxNumPendingOps + 1;
    maxSeqNo = seqNo;
    return new DocumentsWriterDeleteQueue(infoStream, generation + 1, seqNo + 1,
        // don't pass ::getMaxCompletedSeqNo here b/c otherwise we keep an reference to this queue
        // and this will be a memory leak since the queues can't be GCed
        () -> nextSeqNo.get() - 1);

  }

  /**
   * Returns the maximum sequence number for this queue. This value will change once this queue is advanced.
   */
  long getMaxSeqNo() {
    return maxSeqNo;
  }

  /**
   * Returns <code>true</code> if it was advanced.
   */
  boolean isAdvanced() {
    return advanced;
  }
}
