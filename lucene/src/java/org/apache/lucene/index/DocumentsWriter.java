package org.apache.lucene.index;

/**
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

import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DocumentsWriterPerThread.FlushedSegment;
import org.apache.lucene.index.DocumentsWriterPerThread.IndexingChain;
import org.apache.lucene.index.DocumentsWriterPerThreadPool.ThreadState;
import org.apache.lucene.index.FieldInfos.FieldNumberBiMap;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SimilarityProvider;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;

/**
 * This class accepts multiple added documents and directly
 * writes a single segment file.  It does this more
 * efficiently than creating a single segment per document
 * (with DocumentWriter) and doing standard merges on those
 * segments.
 *
 * Each added document is passed to the {@link DocConsumer},
 * which in turn processes the document and interacts with
 * other consumers in the indexing chain.  Certain
 * consumers, like {@link StoredFieldsWriter} and {@link
 * TermVectorsTermsWriter}, digest a document and
 * immediately write bytes to the "doc store" files (ie,
 * they do not consume RAM per document, except while they
 * are processing the document).
 *
 * Other consumers, eg {@link FreqProxTermsWriter} and
 * {@link NormsWriter}, buffer bytes in RAM and flush only
 * when a new segment is produced.

 * Once we have used our allowed RAM buffer, or the number
 * of added docs is large enough (in the case we are
 * flushing by doc count instead of RAM usage), we create a
 * real segment and flush it to the Directory.
 *
 * Threads:
 *
 * Multiple threads are allowed into addDocument at once.
 * There is an initial synchronized call to getThreadState
 * which allocates a ThreadState for this thread.  The same
 * thread will get the same ThreadState over time (thread
 * affinity) so that if there are consistent patterns (for
 * example each thread is indexing a different content
 * source) then we make better use of RAM.  Then
 * processDocument is called on that ThreadState without
 * synchronization (most of the "heavy lifting" is in this
 * call).  Finally the synchronized "finishDocument" is
 * called to flush changes to the directory.
 *
 * When flush is called by IndexWriter we forcefully idle
 * all threads and flush only once they are all idle.  This
 * means you can call flush with a given thread even while
 * other threads are actively adding/deleting documents.
 *
 *
 * Exceptions:
 *
 * Because this class directly updates in-memory posting
 * lists, and flushes stored fields and term vectors
 * directly to files in the directory, there are certain
 * limited times when an exception can corrupt this state.
 * For example, a disk full while flushing stored fields
 * leaves this file in a corrupt state.  Or, an OOM
 * exception while appending to the in-memory posting lists
 * can corrupt that posting list.  We call such exceptions
 * "aborting exceptions".  In these cases we must call
 * abort() to discard all docs added since the last flush.
 *
 * All other exceptions ("non-aborting exceptions") can
 * still partially update the index structures.  These
 * updates are consistent, but, they represent only a part
 * of the document seen up until the exception was hit.
 * When this happens, we immediately mark the document as
 * deleted so that the document is always atomically ("all
 * or none") added to the index.
 */

final class DocumentsWriter {
  Directory directory;

  private volatile boolean closed;

  PrintStream infoStream;
  SimilarityProvider similarityProvider;

  List<String> newFiles;

  final IndexWriter indexWriter;

  private AtomicInteger numDocsInRAM = new AtomicInteger(0);

  final BufferedDeletesStream bufferedDeletesStream;
  // TODO: cutover to BytesRefHash
  private final BufferedDeletes pendingDeletes = new BufferedDeletes(false);
  private Collection<String> abortedFiles;               // List of files that were written before last abort()

  final IndexingChain chain;

  final DocumentsWriterPerThreadPool perThreadPool;
  final FlushPolicy flushPolicy;
  final DocumentsWriterFlushControl flushControl;
  final Healthiness healthiness;
  DocumentsWriter(IndexWriterConfig config, Directory directory, IndexWriter writer, FieldNumberBiMap globalFieldNumbers,
      BufferedDeletesStream bufferedDeletesStream) throws IOException {
    this.directory = directory;
    this.indexWriter = writer;
    this.similarityProvider = config.getSimilarityProvider();
    this.bufferedDeletesStream = bufferedDeletesStream;
    this.perThreadPool = config.getIndexerThreadPool();
    this.chain = config.getIndexingChain();
    this.perThreadPool.initialize(this, globalFieldNumbers, config);
    final FlushPolicy configuredPolicy = config.getFlushPolicy();
    if (configuredPolicy == null) {
      flushPolicy = new FlushByRamOrCountsPolicy();
    } else {
      flushPolicy = configuredPolicy;
    }
    flushPolicy.init(this);
    
    healthiness = new Healthiness();
    final long maxRamPerDWPT = config.getRAMPerThreadHardLimitMB() * 1024 * 1024;
    flushControl = new DocumentsWriterFlushControl(flushPolicy, perThreadPool, healthiness, pendingDeletes, maxRamPerDWPT);
  }

  boolean deleteQueries(final Query... queries) throws IOException {
    synchronized(this) {
      for (Query query : queries) {
        pendingDeletes.addQuery(query, BufferedDeletes.MAX_INT);
      }
    }

    final Iterator<ThreadState> threadsIterator = perThreadPool.getActivePerThreadsIterator();

    while (threadsIterator.hasNext()) {
      ThreadState state = threadsIterator.next();
      state.lock();
      try {
        if (state.isActive()) {
          state.perThread.deleteQueries(queries); 
        }
      } finally {
        state.unlock();
      }
    }

    return false;
  }

  boolean deleteQuery(final Query query) throws IOException {
    return deleteQueries(query);
  }

  boolean deleteTerms(final Term... terms) throws IOException {
    synchronized(this) {
      for (Term term : terms) {
        pendingDeletes.addTerm(term, BufferedDeletes.MAX_INT);
      }
    }

    Iterator<ThreadState> threadsIterator = perThreadPool.getActivePerThreadsIterator();

    while (threadsIterator.hasNext()) {
      ThreadState state = threadsIterator.next();
      state.lock();
      try {
        if (state.isActive()) {
          state.perThread.deleteTerms(terms);
          flushControl.doOnDelete(state);
        }
      } finally {
        state.unlock();
      }
    }
    if (flushControl.flushDeletes.getAndSet(false)) {
      flushDeletes();
    }
    return false;
  }

  // TODO: we could check w/ FreqProxTermsWriter: if the
  // term doesn't exist, don't bother buffering into the
  // per-DWPT map (but still must go into the global map)
  boolean deleteTerm(final Term term) throws IOException {
    return deleteTerms(term);
  }

  void deleteTerm(final Term term, ThreadState exclude) throws IOException {
    synchronized(this) {
      pendingDeletes.addTerm(term, BufferedDeletes.MAX_INT);
    }

    final Iterator<ThreadState> threadsIterator = perThreadPool.getActivePerThreadsIterator();

    while (threadsIterator.hasNext()) {
      ThreadState state = threadsIterator.next();
      if (state != exclude) {
        state.lock();
        try {
          state.perThread.deleteTerms(term);
          flushControl.doOnDelete(state);
        } finally {
          state.unlock();
        }
      }
    }
    if (flushControl.flushDeletes.getAndSet(false)) {
      flushDeletes();
    }
  }

  private void flushDeletes() throws IOException {
    maybePushPendingDeletes();
    indexWriter.applyAllDeletes();
    indexWriter.flushCount.incrementAndGet();
  }

  synchronized void setInfoStream(PrintStream infoStream) {
    this.infoStream = infoStream;
    pushConfigChange();
  }

  private final void pushConfigChange() {
    final Iterator<ThreadState> it = perThreadPool.getAllPerThreadsIterator();
    while (it.hasNext()) {
      DocumentsWriterPerThread perThread = it.next().perThread;
      perThread.docState.infoStream = this.infoStream;
    }
  }

  /** Returns how many docs are currently buffered in RAM. */
  int getNumDocs() {
    return numDocsInRAM.get();
  }

  Collection<String> abortedFiles() {
    return abortedFiles;
  }

  // returns boolean for asserts
  boolean message(String message) {
    if (infoStream != null)
      indexWriter.message("DW: " + message);
    return true;
  }

  private void ensureOpen() throws AlreadyClosedException {
    if (closed) {
      throw new AlreadyClosedException("this IndexWriter is closed");
    }
  }

  /** Called if we hit an exception at a bad time (when
   *  updating the index files) and must discard all
   *  currently buffered docs.  This resets our state,
   *  discarding any docs added since last flush. */
  synchronized void abort() throws IOException {
    boolean success = false;

    synchronized (this) {
      pendingDeletes.clear();
    }

    try {
      if (infoStream != null) {
        message("docWriter: abort");
      }

      final Iterator<ThreadState> threadsIterator = perThreadPool.getActivePerThreadsIterator();

      while (threadsIterator.hasNext()) {
        ThreadState perThread = threadsIterator.next();
        perThread.lock();
        try {
          if (perThread.isActive()) { // we might be closed
            perThread.perThread.abort();
            perThread.perThread.checkAndResetHasAborted();
          } else {
            assert closed;
          }
        } finally {
          perThread.unlock();
        }
      }

      success = true;
    } finally {
      if (infoStream != null) {
        message("docWriter: done abort; abortedFiles=" + abortedFiles + " success=" + success);
      }
    }
  }

  boolean anyChanges() {
    return numDocsInRAM.get() != 0 || anyDeletions();
  }

  public int getBufferedDeleteTermsSize() {
    return pendingDeletes.terms.size();
  }

  //for testing
  public int getNumBufferedDeleteTerms() {
    return pendingDeletes.numTermDeletes.get();
  }

  public boolean anyDeletions() {
    return pendingDeletes.any();
    }

  void close() {
    closed = true;
    flushControl.setClosed();
  }

  boolean updateDocument(final Document doc, final Analyzer analyzer,
      final Term delTerm) throws CorruptIndexException, IOException {
    ensureOpen();
    boolean maybeMerge = false;
    final boolean isUpdate = delTerm != null;
    if (healthiness.isStalled()) {
      /*
       * if we are allowed to hijack threads for flushing we try to flush out 
       * as many pending DWPT to release memory and get back healthy status.
       */
      if (infoStream != null) {
        message("WARNING DocumentsWriter is stalled try to hijack thread to flush pending segment");
      }
      // try pick up pending threads here if possile
      final DocumentsWriterPerThread flushingDWPT;
      flushingDWPT = flushControl.getFlushIfPending(null);
       // don't push the delete here since the update could fail!
      maybeMerge = doFlush(flushingDWPT);
      if (infoStream != null && healthiness.isStalled()) {
        message("WARNING DocumentsWriter is stalled might block thread until DocumentsWriter is not stalled anymore");
      }
      healthiness.waitIfStalled(); // block if stalled
    }
    ThreadState perThread = perThreadPool.getAndLock(Thread.currentThread(),
        this, doc);
    DocumentsWriterPerThread flushingDWPT = null;
    try {
      if (!perThread.isActive()) {
        ensureOpen();
        assert false: "perThread is not active but we are still open";
      }
      final DocumentsWriterPerThread dwpt = perThread.perThread;
      try {
        dwpt.updateDocument(doc, analyzer, delTerm);
      } finally {
        if(dwpt.checkAndResetHasAborted()) {
            flushControl.doOnAbort(perThread);
        }
      }
      flushingDWPT = flushControl.doAfterDocument(perThread, isUpdate);
      numDocsInRAM.incrementAndGet();
    } finally {
      perThread.unlock();
    }
    // delete term from other DWPTs later, so that this thread
    // doesn't have to lock multiple DWPTs at the same time
    if (isUpdate) {
      deleteTerm(delTerm, perThread);
    }
    maybeMerge |= doFlush(flushingDWPT);
    return maybeMerge;
  }
  
 

  private boolean doFlush(DocumentsWriterPerThread flushingDWPT) throws IOException {
    boolean maybeMerge = false;
    while (flushingDWPT != null) {
      maybeMerge = true;
      try {
        // flush concurrently without locking
        final FlushedSegment newSegment = flushingDWPT.flush();
        finishFlushedSegment(newSegment);
      } finally {
          flushControl.doAfterFlush(flushingDWPT);
          flushingDWPT.checkAndResetHasAborted();
          indexWriter.flushCount.incrementAndGet();
      }
        flushingDWPT =  flushControl.nextPendingFlush() ;
    }
    return maybeMerge;
  }
  

  private void finishFlushedSegment(FlushedSegment newSegment)
      throws IOException {
    pushDeletes(newSegment);
    if (newSegment != null) {
      indexWriter.addFlushedSegment(newSegment);
    }
  }

  final void subtractFlushedNumDocs(int numFlushed) {
    int oldValue = numDocsInRAM.get();
    while (!numDocsInRAM.compareAndSet(oldValue, oldValue - numFlushed)) {
      oldValue = numDocsInRAM.get();
    }
  }

  private synchronized void pushDeletes(FlushedSegment flushedSegment) {
    maybePushPendingDeletes();
    if (flushedSegment != null) {
      BufferedDeletes deletes = flushedSegment.segmentDeletes;
      final long delGen = bufferedDeletesStream.getNextGen();
      // Lock order: DW -> BD
      if (deletes != null && deletes.any()) {
        final FrozenBufferedDeletes packet = new FrozenBufferedDeletes(deletes,
            delGen);
        if (infoStream != null) {
          message("flush: push buffered deletes");
        }
        bufferedDeletesStream.push(packet);
        if (infoStream != null) {
          message("flush: delGen=" + packet.gen);
        }
      }
      flushedSegment.segmentInfo.setBufferedDeletesGen(delGen);
    }
  }

  private synchronized final void maybePushPendingDeletes() {
    final long delGen = bufferedDeletesStream.getNextGen();
    if (pendingDeletes.any()) {
      indexWriter.bufferedDeletesStream.push(new FrozenBufferedDeletes(
          pendingDeletes, delGen));
      pendingDeletes.clear();
    }
  }

  final boolean flushAllThreads(final boolean flushDeletes)
    throws IOException {

    final Iterator<ThreadState> threadsIterator = perThreadPool.getActivePerThreadsIterator();
    boolean anythingFlushed = false;

    while (threadsIterator.hasNext()) {
      final ThreadState perThread = threadsIterator.next();
      final DocumentsWriterPerThread flushingDWPT;
      /*
       * TODO: maybe we can leverage incoming / indexing threads here if we mark
       * all active threads pending so that we don't need to block until we got
       * the handle. Yet, we need to figure out how to identify that a certain
       * DWPT has been flushed since they are simply replaced once checked out
       * for flushing. This would give us another level of concurrency during
       * commit.
       * 
       * Maybe we simply iterate them and store the ThreadStates and mark
       * all as flushPending and at the same time record the DWPT instance as a
       * key for the pending ThreadState. This way we can easily iterate until
       * all DWPT have changed.
       */
      perThread.lock(); 
      try {
        if (!perThread.isActive()) {
          assert closed;
          continue; //this perThread is already done maybe by a concurrently indexing thread
        }
        final DocumentsWriterPerThread dwpt = perThread.perThread; 
        // Always flush docs if there are any
        final boolean flushDocs =  dwpt.getNumDocsInRAM() > 0;
        final String segment = dwpt.getSegment();
        // If we are flushing docs, segment must not be null:
        assert segment != null || !flushDocs;
        if (flushDocs) {
          // check out and set pending if not already set
          flushingDWPT = flushControl.tryCheckoutForFlush(perThread, true);
          assert flushingDWPT != null : "DWPT must never be null here since we hold the lock and it holds documents";
          assert dwpt == flushingDWPT : "flushControl returned different DWPT";
          try {
            final FlushedSegment newSegment = dwpt.flush();
            anythingFlushed = true;
            finishFlushedSegment(newSegment);
          } finally {
            flushControl.doAfterFlush(flushingDWPT);
          }
        }
      } finally {
        perThread.unlock();
      }
    }

    if (!anythingFlushed && flushDeletes) {
      maybePushPendingDeletes();
    }


    return anythingFlushed;
  }
  
  
  
 

//  /* We have three pools of RAM: Postings, byte blocks
//   * (holds freq/prox posting data) and per-doc buffers
//   * (stored fields/term vectors).  Different docs require
//   * varying amount of storage from these classes.  For
//   * example, docs with many unique single-occurrence short
//   * terms will use up the Postings RAM and hardly any of
//   * the other two.  Whereas docs with very large terms will
//   * use alot of byte blocks RAM.  This method just frees
//   * allocations from the pools once we are over-budget,
//   * which balances the pools to match the current docs. */
//  void balanceRAM() {
//
//    final boolean doBalance;
//    final long deletesRAMUsed;
//
//    deletesRAMUsed = bufferedDeletes.bytesUsed();
//
//    synchronized(this) {
//      if (ramBufferSize == IndexWriterConfig.DISABLE_AUTO_FLUSH || bufferIsFull) {
//        return;
//      }
//
//      doBalance = bytesUsed() + deletesRAMUsed >= ramBufferSize;
//    }
//
//    if (doBalance) {
//
//      if (infoStream != null)
//        message("  RAM: balance allocations: usedMB=" + toMB(bytesUsed()) +
//                " vs trigger=" + toMB(ramBufferSize) +
//                " deletesMB=" + toMB(deletesRAMUsed) +
//                " byteBlockFree=" + toMB(byteBlockAllocator.bytesUsed()) +
//                " perDocFree=" + toMB(perDocAllocator.bytesUsed()));
//
//      final long startBytesUsed = bytesUsed() + deletesRAMUsed;
//
//      int iter = 0;
//
//      // We free equally from each pool in 32 KB
//      // chunks until we are below our threshold
//      // (freeLevel)
//
//      boolean any = true;
//
//      while(bytesUsed()+deletesRAMUsed > freeLevel) {
//
//        synchronized(this) {
//          if (0 == perDocAllocator.numBufferedBlocks() &&
//              0 == byteBlockAllocator.numBufferedBlocks() &&
//              0 == freeIntBlocks.size() && !any) {
//            // Nothing else to free -- must flush now.
//            bufferIsFull = bytesUsed()+deletesRAMUsed > ramBufferSize;
//            if (infoStream != null) {
//              if (bytesUsed()+deletesRAMUsed > ramBufferSize)
//                message("    nothing to free; set bufferIsFull");
//              else
//                message("    nothing to free");
//            }
//            break;
//          }
//
//          if ((0 == iter % 4) && byteBlockAllocator.numBufferedBlocks() > 0) {
//            byteBlockAllocator.freeBlocks(1);
//          }
//          if ((1 == iter % 4) && freeIntBlocks.size() > 0) {
//            freeIntBlocks.remove(freeIntBlocks.size()-1);
//            bytesUsed.addAndGet(-INT_BLOCK_SIZE * RamUsageEstimator.NUM_BYTES_INT);
//          }
//          if ((2 == iter % 4) && perDocAllocator.numBufferedBlocks() > 0) {
//            perDocAllocator.freeBlocks(32); // Remove upwards of 32 blocks (each block is 1K)
//          }
//        }
//
//        if ((3 == iter % 4) && any)
//          // Ask consumer to free any recycled state
//          any = consumer.freeRAM();
//
//        iter++;
//      }
//
//      if (infoStream != null)
//        message("    after free: freedMB=" + nf.format((startBytesUsed-bytesUsed()-deletesRAMUsed)/1024./1024.) + " usedMB=" + nf.format((bytesUsed()+deletesRAMUsed)/1024./1024.));
//    }
//  }
}
