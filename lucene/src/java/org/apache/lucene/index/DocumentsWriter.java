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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DocumentsWriterPerThread.IndexingChain;
import org.apache.lucene.index.DocumentsWriterPerThreadPool.ThreadState;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Similarity;
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
  final AtomicLong bytesUsed = new AtomicLong(0);
  Directory directory;

  boolean bufferIsFull;                   // True when it's time to write segment
  private volatile boolean closed;

  PrintStream infoStream;
  int maxFieldLength = IndexWriterConfig.UNLIMITED_FIELD_LENGTH;
  Similarity similarity;

  List<String> newFiles;

  final IndexWriter indexWriter;

  private AtomicInteger numDocsInRAM = new AtomicInteger(0);
  private AtomicLong ramUsed = new AtomicLong(0);

  // How much RAM we can use before flushing.  This is 0 if
  // we are flushing by doc count instead.
  private long ramBufferSize = (long) (IndexWriterConfig.DEFAULT_RAM_BUFFER_SIZE_MB*1024*1024);

  // Flush @ this number of docs.  If ramBufferSize is
  // non-zero we will flush by RAM usage instead.
  private int maxBufferedDocs = IndexWriterConfig.DEFAULT_MAX_BUFFERED_DOCS;

  private final FieldInfos fieldInfos;

  final BufferedDeletes bufferedDeletes;
  SegmentDeletes pendingDeletes;
  final IndexingChain chain;

  final DocumentsWriterPerThreadPool perThreadPool;

  DocumentsWriter(Directory directory, IndexWriter writer, IndexingChain chain, DocumentsWriterPerThreadPool indexerThreadPool, FieldInfos fieldInfos, BufferedDeletes bufferedDeletes) throws IOException {
    this.directory = directory;
    this.indexWriter = writer;
    this.similarity = writer.getConfig().getSimilarity();
    this.fieldInfos = fieldInfos;
    this.bufferedDeletes = bufferedDeletes;
    this.perThreadPool = indexerThreadPool;
    this.pendingDeletes = new SegmentDeletes();
    this.chain = chain;
    this.perThreadPool.initialize(this);
  }

  boolean deleteQueries(final Query... queries) throws IOException {
    Iterator<ThreadState> threadsIterator = perThreadPool.getActivePerThreadsIterator();

    boolean deleted = false;
    while (threadsIterator.hasNext()) {
      ThreadState state = threadsIterator.next();
      state.lock();
      try {
        state.perThread.deleteQueries(queries);
        deleted = true;
      } finally {
        state.unlock();
      }
    }

    if (!deleted) {
      synchronized(this) {
        for (Query query : queries) {
          pendingDeletes.addQuery(query, SegmentDeletes.MAX_INT);
        }
      }
    }

    return false;
  }

  boolean deleteQuery(final Query query) throws IOException {
    return deleteQueries(query);
  }

  boolean deleteTerms(final Term... terms) throws IOException {
    Iterator<ThreadState> threadsIterator = perThreadPool.getActivePerThreadsIterator();

    boolean deleted = false;
    while (threadsIterator.hasNext()) {
      ThreadState state = threadsIterator.next();
      deleted = true;
      state.lock();
      try {
        state.perThread.deleteTerms(terms);
      } finally {
        state.unlock();
      }
    }

    if (!deleted) {
      synchronized(this) {
        for (Term term : terms) {
          pendingDeletes.addTerm(term, SegmentDeletes.MAX_INT);
        }
      }
    }

    return false;
  }

  boolean deleteTerm(final Term term) throws IOException {
    return deleteTerms(term);
  }

  boolean deleteTerm(final Term term, ThreadState exclude) {
    Iterator<ThreadState> threadsIterator = perThreadPool.getActivePerThreadsIterator();

    boolean deleted = false;
    while (threadsIterator.hasNext()) {
      deleted = true;
      ThreadState state = threadsIterator.next();
      if (state != exclude) {
        state.lock();
        try {
          state.perThread.deleteTerms(term);
        } finally {
          state.unlock();
        }
      }
    }

    return deleted;
  }

  public FieldInfos getFieldInfos() {
    return fieldInfos;
  }

  /** Returns true if any of the fields in the current
   *  buffered docs have omitTermFreqAndPositions==false */
  boolean hasProx() {
    return fieldInfos.hasProx();
  }

  /** If non-null, various details of indexing are printed
   *  here. */
  synchronized void setInfoStream(PrintStream infoStream) {
    this.infoStream = infoStream;
    pushConfigChange();
  }

  synchronized void setMaxFieldLength(int maxFieldLength) {
    this.maxFieldLength = maxFieldLength;
    pushConfigChange();
  }

  synchronized void setSimilarity(Similarity similarity) {
    this.similarity = similarity;
    pushConfigChange();
  }

  private final void pushConfigChange() {
    Iterator<ThreadState> it = perThreadPool.getAllPerThreadsIterator();
    while (it.hasNext()) {
      DocumentsWriterPerThread perThread = it.next().perThread;
      perThread.docState.infoStream = this.infoStream;
      perThread.docState.maxFieldLength = this.maxFieldLength;
      perThread.docState.similarity = this.similarity;
    }
  }

  /** Set how much RAM we can use before flushing. */
  synchronized void setRAMBufferSizeMB(double mb) {
    if (mb == IndexWriterConfig.DISABLE_AUTO_FLUSH) {
      ramBufferSize = IndexWriterConfig.DISABLE_AUTO_FLUSH;
    } else {
      ramBufferSize = (long) (mb*1024*1024);
    }
  }

  synchronized double getRAMBufferSizeMB() {
    if (ramBufferSize == IndexWriterConfig.DISABLE_AUTO_FLUSH) {
      return ramBufferSize;
    } else {
      return ramBufferSize/1024./1024.;
    }
  }

  /** Set max buffered docs, which means we will flush by
   *  doc count instead of by RAM usage. */
  void setMaxBufferedDocs(int count) {
    maxBufferedDocs = count;
  }

  int getMaxBufferedDocs() {
    return maxBufferedDocs;
  }

  /** Returns how many docs are currently buffered in RAM. */
  int getNumDocs() {
    return numDocsInRAM.get();
  }
  private Collection<String> abortedFiles;               // List of files that were written before last abort()

  Collection<String> abortedFiles() {
    return abortedFiles;
  }

  void message(String message) {
    if (infoStream != null)
      indexWriter.message("DW: " + message);
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

    try {
      if (infoStream != null) {
        message("docWriter: abort");
      }

      Iterator<ThreadState> threadsIterator = perThreadPool.getActivePerThreadsIterator();

      while (threadsIterator.hasNext()) {
        ThreadState perThread = threadsIterator.next();
        perThread.lock();
        try {
          perThread.perThread.abort();
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

  synchronized boolean anyChanges() {
    return numDocsInRAM.get() != 0 || anyDeletions();
  }

  public int getBufferedDeleteTermsSize() {
    int size = 0;
    Iterator<ThreadState> it = perThreadPool.getActivePerThreadsIterator();
    while (it.hasNext()) {
      DocumentsWriterPerThread dwpt = it.next().perThread;
      size += dwpt.pendingDeletes.terms.size();
    }
    size += pendingDeletes.terms.size();
    return size;
  }

  //for testing
  public int getNumBufferedDeleteTerms() {
    int numDeletes = 0;
    Iterator<ThreadState> it = perThreadPool.getActivePerThreadsIterator();
    while (it.hasNext()) {
      DocumentsWriterPerThread dwpt = it.next().perThread;
      numDeletes += dwpt.pendingDeletes.numTermDeletes.get();
    }
    numDeletes += pendingDeletes.numTermDeletes.get();
    return numDeletes;
  }

  // TODO: can we improve performance of this method by keeping track
  // here in DW of whether any DWPT has deletions?
  public synchronized boolean anyDeletions() {
    if (pendingDeletes.any()) {
      return true;
    }

    Iterator<ThreadState> threadsIterator = perThreadPool.getActivePerThreadsIterator();
    while (threadsIterator.hasNext()) {
      ThreadState state = threadsIterator.next();
      state.lock();
      try {
        if (state.perThread.pendingDeletes.any()) {
          return true;
        }
      } finally {
        state.unlock();
      }
    }

    return false;
  }

  void close() {
    closed = true;
  }

  boolean updateDocument(final Document doc, final Analyzer analyzer, final Term delTerm)
      throws CorruptIndexException, IOException {
    ensureOpen();

    SegmentInfo newSegment = null;
    SegmentDeletes segmentDeletes = null;

    ThreadState perThread = perThreadPool.getAndLock(Thread.currentThread(), this, doc);
    try {
      DocumentsWriterPerThread dwpt = perThread.perThread;
      long perThreadRAMUsedBeforeAdd = dwpt.bytesUsed();
      dwpt.updateDocument(doc, analyzer, delTerm);
      numDocsInRAM.incrementAndGet();

      newSegment = finishAddDocument(dwpt, perThreadRAMUsedBeforeAdd);
      if (newSegment != null) {
        fieldInfos.update(dwpt.getFieldInfos());
        if (dwpt.pendingDeletes.any()) {
          segmentDeletes = dwpt.pendingDeletes;
          dwpt.pendingDeletes = new SegmentDeletes();
        }
      }
    } finally {
      perThread.unlock();
    }

    if (segmentDeletes != null) {
      pushDeletes(newSegment, segmentDeletes);
    }

    if (newSegment != null) {
      perThreadPool.clearThreadBindings(perThread);
      indexWriter.addFlushedSegment(newSegment);
      return true;
    }

    // delete term from other DWPTs later, so that this thread
    // doesn't have to lock multiple DWPTs at the same time
    if (delTerm != null) {
      deleteTerm(delTerm, perThread);
    }

    return false;
  }

  private final SegmentInfo finishAddDocument(DocumentsWriterPerThread perThread,
      long perThreadRAMUsedBeforeAdd) throws IOException {
    SegmentInfo newSegment = null;

    if (perThread.getNumDocsInRAM() == maxBufferedDocs) {
      newSegment = perThread.flush();
    }

    long deltaRAM = perThread.bytesUsed() - perThreadRAMUsedBeforeAdd;
    long oldValue = ramUsed.get();
    while (!ramUsed.compareAndSet(oldValue, oldValue + deltaRAM)) {
      oldValue = ramUsed.get();
    }

    return newSegment;
  }

  final void substractFlushedNumDocs(int numFlushed) {
    int oldValue = numDocsInRAM.get();
    while (!numDocsInRAM.compareAndSet(oldValue, oldValue - numFlushed)) {
      oldValue = numDocsInRAM.get();
    }
  }

  private final void pushDeletes(SegmentInfo segmentInfo, SegmentDeletes segmentDeletes) {
    synchronized(indexWriter) {
      // Lock order: DW -> BD
      if (segmentDeletes.any()) {
        if (segmentInfo != null) {
          bufferedDeletes.pushDeletes(segmentDeletes, segmentInfo);
        } else if (indexWriter.segmentInfos.size() > 0) {
          if (infoStream != null) {
            message("flush: push buffered deletes to previously flushed segment " + indexWriter.segmentInfos.lastElement());
          }
          bufferedDeletes.pushDeletes(segmentDeletes, indexWriter.segmentInfos.lastElement(), true);
        } else {
          if (infoStream != null) {
            message("flush: drop buffered deletes: no segments");
          }
          // We can safely discard these deletes: since
          // there are no segments, the deletions cannot
          // affect anything.
        }
      }
    }
  }

  final boolean flushAllThreads(final boolean flushDeletes)
    throws IOException {

    if (flushDeletes) {
      synchronized (this) {
        pushDeletes(null, pendingDeletes);
        pendingDeletes = new SegmentDeletes();
      }
    }

    Iterator<ThreadState> threadsIterator = perThreadPool.getActivePerThreadsIterator();
    boolean anythingFlushed = false;

    while (threadsIterator.hasNext()) {
      SegmentInfo newSegment = null;
      SegmentDeletes segmentDeletes = null;

      ThreadState perThread = threadsIterator.next();
      perThread.lock();
      try {

        DocumentsWriterPerThread dwpt = perThread.perThread;
        final int numDocs = dwpt.getNumDocsInRAM();

        // Always flush docs if there are any
        boolean flushDocs = numDocs > 0;

        String segment = dwpt.getSegment();

        // If we are flushing docs, segment must not be null:
        assert segment != null || !flushDocs;

        if (flushDocs) {
          newSegment = dwpt.flush();

          if (newSegment != null) {
            fieldInfos.update(dwpt.getFieldInfos());
            anythingFlushed = true;
            perThreadPool.clearThreadBindings(perThread);
            if (dwpt.pendingDeletes.any()) {
              segmentDeletes = dwpt.pendingDeletes;
              dwpt.pendingDeletes = new SegmentDeletes();
            }
          }
        } else if (flushDeletes && dwpt.pendingDeletes.any()) {
          segmentDeletes = dwpt.pendingDeletes;
          dwpt.pendingDeletes = new SegmentDeletes();
        }
      } finally {
        perThread.unlock();
      }

      if (segmentDeletes != null) {
          pushDeletes(newSegment, segmentDeletes);
      }


      if (newSegment != null) {
        // important do unlock the perThread before finishFlushedSegment
        // is called to prevent deadlock on IndexWriter mutex
        indexWriter.addFlushedSegment(newSegment);
      }
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
