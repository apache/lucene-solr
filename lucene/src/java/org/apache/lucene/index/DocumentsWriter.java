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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DocumentsWriterPerThread.IndexingChain;
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

  int numDocsInStore;                     // # docs written to doc stores

  boolean bufferIsFull;                   // True when it's time to write segment
  private boolean closed;

  PrintStream infoStream;
  int maxFieldLength = IndexWriterConfig.UNLIMITED_FIELD_LENGTH;
  Similarity similarity;

  List<String> newFiles;

  private final DocumentsWriterThreadPool threadPool;
  final IndexWriter indexWriter;
  private final Lock sequenceIDLock = new ReentrantLock();

  private AtomicInteger numDocsInRAM = new AtomicInteger(0);
  private AtomicLong ramUsed = new AtomicLong(0);
  private int numDocumentsWriterPerThreads;

  static class DocState {
    DocumentsWriter docWriter;
    Analyzer analyzer;
    int maxFieldLength;
    PrintStream infoStream;
    Similarity similarity;
    int docID;
    Document doc;
    String maxTermPrefix;

    // Only called by asserts
    public boolean testPoint(String name) {
      return docWriter.indexWriter.testPoint(name);
    }

    public void clear() {
      // don't hold onto doc nor analyzer, in case it is
      // largish:
      doc = null;
      analyzer = null;
    }
  }

  // How much RAM we can use before flushing.  This is 0 if
  // we are flushing by doc count instead.
  private long ramBufferSize = (long) (IndexWriterConfig.DEFAULT_RAM_BUFFER_SIZE_MB*1024*1024);

  // If we've allocated 5% over our RAM budget, we then
  // free down to 95%
  private long freeLevel = (long) (IndexWriterConfig.DEFAULT_RAM_BUFFER_SIZE_MB*1024*1024*0.95);

  // Flush @ this number of docs.  If ramBufferSize is
  // non-zero we will flush by RAM usage instead.
  private int maxBufferedDocs = IndexWriterConfig.DEFAULT_MAX_BUFFERED_DOCS;

  private final FieldInfos fieldInfos;

  final BufferedDeletes bufferedDeletes;
  private final IndexWriter.FlushControl flushControl;
  private final IndexingChain chain;

  DocumentsWriter(Directory directory, IndexWriter writer, IndexingChain chain, DocumentsWriterThreadPool indexerThreadPool, FieldInfos fieldInfos, BufferedDeletes bufferedDeletes) throws IOException {
    this.directory = directory;
    this.indexWriter = writer;
    this.similarity = writer.getConfig().getSimilarity();
    this.fieldInfos = fieldInfos;
    this.bufferedDeletes = bufferedDeletes;
    this.threadPool = indexerThreadPool;
    this.chain = chain;
    flushControl = writer.flushControl;
  }

  boolean deleteQueries(Query... queries) {
    final boolean doFlush = flushControl.waitUpdate(0, queries.length);
    Iterator<DocumentsWriterPerThread> it = threadPool.getPerThreadIterator();
    while (it.hasNext()) {
      it.next().deleteQueries(queries);
    }
    return doFlush;
  }

  boolean deleteQuery(Query query) {
    final boolean doFlush = flushControl.waitUpdate(0, 1);
    Iterator<DocumentsWriterPerThread> it = threadPool.getPerThreadIterator();
    while (it.hasNext()) {
      it.next().deleteQuery(query);
    }
    return doFlush;
  }

  boolean deleteTerms(Term... terms) {
    final boolean doFlush = flushControl.waitUpdate(0, terms.length);
    Iterator<DocumentsWriterPerThread> it = threadPool.getPerThreadIterator();
    while (it.hasNext()) {
      it.next().deleteTerms(terms);
    }
    return doFlush;
  }

  boolean deleteTerm(Term term, boolean skipWait) {
    final boolean doFlush = flushControl.waitUpdate(0, 1, skipWait);
    Iterator<DocumentsWriterPerThread> it = threadPool.getPerThreadIterator();
    while (it.hasNext()) {
      it.next().deleteTerm(term);
    }
    return doFlush;
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
    Iterator<DocumentsWriterPerThread> it = threadPool.getPerThreadIterator();
    while (it.hasNext()) {
      it.next().docState.infoStream = infoStream;
    }
  }

  synchronized void setMaxFieldLength(int maxFieldLength) {
    this.maxFieldLength = maxFieldLength;
    Iterator<DocumentsWriterPerThread> it = threadPool.getPerThreadIterator();
    while (it.hasNext()) {
      it.next().docState.maxFieldLength = maxFieldLength;
    }
  }

  synchronized void setSimilarity(Similarity similarity) {
    this.similarity = similarity;
    Iterator<DocumentsWriterPerThread> it = threadPool.getPerThreadIterator();
    while (it.hasNext()) {
      it.next().docState.similarity = similarity;
    }
  }

  /** Set how much RAM we can use before flushing. */
  synchronized void setRAMBufferSizeMB(double mb) {
    if (mb == IndexWriterConfig.DISABLE_AUTO_FLUSH) {
      ramBufferSize = IndexWriterConfig.DISABLE_AUTO_FLUSH;
    } else {
      ramBufferSize = (long) (mb*1024*1024);
      freeLevel = (long) (0.95 * ramBufferSize);
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
    if (infoStream != null) {
      message("docWriter: abort");
    }

    boolean success = false;
    try {

      threadPool.abort();
      success = true;
    } finally {
      notifyAll();
      if (infoStream != null) {
        message("docWriter: done abort; abortedFiles=" + abortedFiles + " success=" + success);
      }
    }
  }

  synchronized boolean anyChanges() {
    // nocommit
    return numDocsInRAM.get() != 0;
    //return numDocsInRAM.get() != 0 || pendingDeletes.any();
  }

  // for testing
  public SegmentDeletes getPendingDeletes() {
    return null;
    // nocommit
    //return pendingDeletes;
  }

  public boolean anyDeletions() {
    // nocommit
    return true;
    //return pendingDeletes.any();
  }

  synchronized void close() {
    closed = true;
    notifyAll();
  }

  DocumentsWriterPerThread newDocumentsWriterPerThread() {
    DocumentsWriterPerThread perThread = new DocumentsWriterPerThread(directory, this, chain);
    sequenceIDLock.lock();
    try {
      numDocumentsWriterPerThreads++;
      return perThread;
    } finally {
      sequenceIDLock.unlock();
    }
  }

  boolean updateDocument(final Document doc, final Analyzer analyzer, final Term delTerm)
      throws CorruptIndexException, IOException {

    boolean flushed = threadPool.executePerThread(this, doc,
        new DocumentsWriterThreadPool.PerThreadTask<Boolean>() {
          @Override
          public Boolean process(final DocumentsWriterPerThread perThread) throws IOException {
            long perThreadRAMUsedBeforeAdd = perThread.bytesUsed();
            perThread.addDocument(doc, analyzer);

            sequenceIDLock.lock();
            try {
              ensureOpen();
              if (delTerm != null) {
                deleteTerm(delTerm, true);
              }
              perThread.commitDocument();
              numDocsInRAM.incrementAndGet();
            } finally {
              sequenceIDLock.unlock();
            }

            if (finishAddDocument(perThread, perThreadRAMUsedBeforeAdd)) {
              super.clearThreadBindings();
              return true;
            }
            return false;
          }
        });

    if (flushed) {
      indexWriter.maybeMerge();
      return true;
    }

    return false;
  }

  private final boolean finishAddDocument(DocumentsWriterPerThread perThread,
      long perThreadRAMUsedBeforeAdd) throws IOException {
    int numDocsPerThread = perThread.getNumDocsInRAM();
    boolean flushed = maybeFlushPerThread(perThread);
    if (flushed) {
      int oldValue = numDocsInRAM.get();
      while (!numDocsInRAM.compareAndSet(oldValue, oldValue - numDocsPerThread)) {
        oldValue = numDocsInRAM.get();
      }
    }

    long deltaRAM = perThread.bytesUsed() - perThreadRAMUsedBeforeAdd;
    long oldValue = ramUsed.get();
    while (!ramUsed.compareAndSet(oldValue, oldValue + deltaRAM)) {
      oldValue = ramUsed.get();
    }

    return flushed;
  }

  private boolean flushSegment(DocumentsWriterPerThread perThread) throws IOException {
    if (perThread.getNumDocsInRAM() == 0) {
      return false;
    }

    SegmentInfo newSegment = perThread.flush();
    newSegment.dir = indexWriter.getDirectory();

    finishFlushedSegment(newSegment, perThread);
    return true;
  }

  private final boolean maybeFlushPerThread(DocumentsWriterPerThread perThread) throws IOException {
    if (perThread.getNumDocsInRAM() == maxBufferedDocs) {
      flushSegment(perThread);
      assert perThread.getNumDocsInRAM() == 0;

      return true;
    }

    return false;
  }

  final boolean flushAllThreads(final boolean flushDeletes)
    throws IOException {

    return threadPool.executeAllThreads(this, new DocumentsWriterThreadPool.AllThreadsTask<Boolean>() {
      @Override
      public Boolean process(Iterator<DocumentsWriterPerThread> threadsIterator) throws IOException {
        boolean anythingFlushed = false;

        while (threadsIterator.hasNext()) {
          DocumentsWriterPerThread perThread = threadsIterator.next();
          final int numDocs = perThread.getNumDocsInRAM();

          // Always flush docs if there are any
          boolean flushDocs = numDocs > 0;

          String segment = perThread.getSegment();

          // If we are flushing docs, segment must not be null:
          assert segment != null || !flushDocs;

          if (flushDocs) {
            SegmentInfo newSegment = perThread.flush();
            newSegment.dir = indexWriter.getDirectory();

            if (newSegment != null) {
              anythingFlushed = true;

              IndexWriter.setDiagnostics(newSegment, "flush");
              finishFlushedSegment(newSegment, perThread);
            }
          } else if (flushDeletes) {
            perThread.pushDeletes(null, indexWriter.segmentInfos);
          }
        }

        if (anythingFlushed) {
          clearThreadBindings();
          numDocsInRAM.set(0);
        }

        return anythingFlushed;
      }
    });
  }

  /** Build compound file for the segment we just flushed */
  void createCompoundFile(String compoundFileName, DocumentsWriterPerThread perThread) throws IOException {
    CompoundFileWriter cfsWriter = new CompoundFileWriter(directory, compoundFileName);
    for(String fileName : perThread.flushState.flushedFiles) {
      cfsWriter.addFile(fileName);
    }

    // Perform the merge
    cfsWriter.close();
  }

  void finishFlushedSegment(SegmentInfo newSegment, DocumentsWriterPerThread perThread) throws IOException {
    perThread.pushDeletes(newSegment, indexWriter.segmentInfos);

    if (indexWriter.useCompoundFile(newSegment)) {
      String compoundFileName = IndexFileNames.segmentFileName(newSegment.name, "", IndexFileNames.COMPOUND_FILE_EXTENSION);
      message("creating compound file " + compoundFileName);
      // Now build compound file
      boolean success = false;
      try {
        createCompoundFile(compoundFileName, perThread);
        success = true;
      } finally {
        if (!success) {
          if (infoStream != null) {
            message("hit exception " +
                "reating compound file for newly flushed segment " + newSegment.name);
          }

          indexWriter.deleter.deleteFile(IndexFileNames.segmentFileName(newSegment.name, "",
              IndexFileNames.COMPOUND_FILE_EXTENSION));
          for (String file : perThread.flushState.flushedFiles) {
            indexWriter.deleter.deleteFile(file);
          }

        }
      }

      for (String file : perThread.flushState.flushedFiles) {
        indexWriter.deleter.deleteFile(file);
      }

      newSegment.setUseCompoundFile(true);

    }

    indexWriter.addNewSegment(newSegment);
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
