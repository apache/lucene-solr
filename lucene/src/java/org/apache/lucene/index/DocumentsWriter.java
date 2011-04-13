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
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
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
  // TODO: cut over to BytesRefHash in BufferedDeletes
  volatile DocumentsWriterDeleteQueue deleteQueue = new DocumentsWriterDeleteQueue(new BufferedDeletes(false));
  private final Queue<FlushTicket> ticketQueue = new LinkedList<DocumentsWriter.FlushTicket>();

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
    flushControl = new DocumentsWriterFlushControl(this, healthiness, maxRamPerDWPT);
  }

  synchronized boolean deleteQueries(final Query... queries) throws IOException {
    final DocumentsWriterDeleteQueue deleteQueue = this.deleteQueue;
    deleteQueue.addDelete(queries);
    return false;
  }

  boolean deleteQuery(final Query query) throws IOException {
    return deleteQueries(query);
  }

  synchronized boolean deleteTerms(final Term... terms) throws IOException {
    final DocumentsWriterDeleteQueue deleteQueue = this.deleteQueue;
    deleteQueue.addDelete(terms);
    flushControl.doOnDelete();
    if (flushControl.flushDeletes.getAndSet(false)) {
      flushDeletes(deleteQueue);
    }
    return false;
  }

  // TODO: we could check w/ FreqProxTermsWriter: if the
  // term doesn't exist, don't bother buffering into the
  // per-DWPT map (but still must go into the global map)
  boolean deleteTerm(final Term term) throws IOException {
    return deleteTerms(term);
  }


  
  DocumentsWriterDeleteQueue currentDeleteSession() {
    return deleteQueue;
  }
  
  private void flushDeletes(DocumentsWriterDeleteQueue deleteQueue) throws IOException {
    if (deleteQueue != null) {
      synchronized (ticketQueue) {
        // freeze and insert the delete flush ticket in the queue
        ticketQueue.add(new FlushTicket(deleteQueue.freezeGlobalBuffer(null), false));
        applyFlushTickets(null, null);
       }
    }
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
      deleteQueue.clear();
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
    return deleteQueue.getBufferedDeleteTermsSize();
  }

  //for testing
  public int getNumBufferedDeleteTerms() {
    return deleteQueue.numGlobalTermDeletes();
  }

  public boolean anyDeletions() {
    return deleteQueue.anyChanges();
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
      DocumentsWriterPerThread flushingDWPT;
      while ( (flushingDWPT = flushControl.nextPendingFlush()) != null){
       // don't push the delete here since the update could fail!
        maybeMerge = doFlush(flushingDWPT);
        if (!healthiness.isStalled()) {
          break;
        }
      }
      if (infoStream != null && healthiness.isStalled()) {
        message("WARNING DocumentsWriter is stalled might block thread until DocumentsWriter is not stalled anymore");
      }
      healthiness.waitIfStalled(); // block if stalled
    }
    final ThreadState perThread = perThreadPool.getAndLock(Thread.currentThread(),
        this, doc);
    final DocumentsWriterPerThread flushingDWPT;
    final DocumentsWriterPerThread dwpt;
    try {
      if (!perThread.isActive()) {
        ensureOpen();
        assert false: "perThread is not active but we are still open";
      }
       
      dwpt = perThread.perThread;
      try {
        dwpt.updateDocument(doc, analyzer, delTerm); 
        numDocsInRAM.incrementAndGet();
      } finally {
        if(dwpt.checkAndResetHasAborted()) {
            flushControl.doOnAbort(perThread);
        }
      }
      flushingDWPT = flushControl.doAfterDocument(perThread, isUpdate);
    } finally {
      perThread.unlock();
    }
    
    maybeMerge |= doFlush(flushingDWPT);
    return maybeMerge;
  }

  private  boolean doFlush(DocumentsWriterPerThread flushingDWPT) throws IOException {
    boolean maybeMerge = false;
    while (flushingDWPT != null) {
      maybeMerge = true;
      boolean success = false;
      FlushTicket ticket = null;
      try {
        /*
         * Since with DWPT the flush process is concurrent and several DWPT
         * could flush at the same time we must maintain the order of the
         * flushes before we can apply the flushed segment and the frozen global
         * deletes it is buffering. The reason for this is that the global
         * deletes mark a certain point in time where we took a DWPT out of
         * rotation and freeze the global deletes.
         * 
         * Example: A flush 'A' starts and freezes the global deletes, then
         * flush 'B' starts and freezes all deletes occurred since 'A' has
         * started. if 'B' finishes before 'A' we need to wait until 'A' is done
         * otherwise the deletes frozen by 'B' are not applied to 'A' and we
         * might miss to deletes documents in 'A'.
         */
        synchronized (ticketQueue) {
         // each flush is assigned a ticket in the order they accquire the ticketQueue lock
         ticket =  new FlushTicket(flushingDWPT.prepareFlush(), true);
         ticketQueue.add(ticket);
        }
        // flush concurrently without locking
        final FlushedSegment newSegment = flushingDWPT.flush();
        success = true;
        /*
         * now we are done and try to flush the ticket queue if the head of the
         * queue has already finished the flush.
         */
        applyFlushTickets(ticket, newSegment);
      } finally {
          flushControl.doAfterFlush(flushingDWPT);
          flushingDWPT.checkAndResetHasAborted();
          indexWriter.flushCount.incrementAndGet();
          if (!success && ticket != null) {
            synchronized (ticketQueue) {
            // in the case of a failure make sure we are making progress and
            // apply all the deletes since the segment flush failed
              ticket.isSegmentFlush = false;
             
            }
          }
      }
      flushingDWPT =  flushControl.nextPendingFlush() ;
    }
    return maybeMerge;
  }
  

  private void applyFlushTickets(FlushTicket current, FlushedSegment segment) throws IOException {
    synchronized (ticketQueue) {
      if (current != null) {
        // this is a segment FlushTicket so assign the flushed segment so we can make progress.
        assert segment != null;
        current.segment = segment;
      }
      while (true) {
        // while we can publish flushes keep on making the queue empty.
        final FlushTicket head = ticketQueue.peek();
        if (head != null && head.canPublish()) {
          ticketQueue.poll();
          finishFlushedSegment(head.segment, head.frozenDeletes);
        } else {
          break;
        }
      }
    }
  }
  

  private void finishFlushedSegment(FlushedSegment newSegment, FrozenBufferedDeletes bufferedDeletes)
      throws IOException {
    // this is eventually finishing the flushed segment and publishing it to the IndexWriter
    if (bufferedDeletes != null && bufferedDeletes.any()) {
      bufferedDeletesStream.push(bufferedDeletes);
      if (infoStream != null) {
        message("flush: push buffered deletes: " + bufferedDeletes);
      }
    }
    publishFlushedSegment(newSegment);

  }

  final void subtractFlushedNumDocs(int numFlushed) {
    int oldValue = numDocsInRAM.get();
    while (!numDocsInRAM.compareAndSet(oldValue, oldValue - numFlushed)) {
      oldValue = numDocsInRAM.get();
    }
  }
  
  private void publishFlushedSegment(FlushedSegment newSegment)
      throws IOException {
    if (newSegment != null) {
      final SegmentInfo segInfo = indexWriter.prepareFlushedSegment(newSegment);
      final BufferedDeletes deletes = newSegment.segmentDeletes;
      FrozenBufferedDeletes packet = null;
      if (deletes != null && deletes.any()) {
        // segment private delete
        packet = new FrozenBufferedDeletes(deletes, true);
        if (infoStream != null) {
          message("flush: push buffered seg private deletes: " + packet);
        }
      }
      indexWriter.publishFlushedSegment(segInfo, packet);
    }
  }
  
  private final Object flushAllLock = new Object();
  // for asserts
  private volatile DocumentsWriterDeleteQueue currentFlusingSession = null;
  private boolean setFlushingDeleteQueue(DocumentsWriterDeleteQueue session) {
    currentFlusingSession = session;
    return true;
  }
  
  final boolean flushAllThreads(final boolean flushDeletes)
    throws IOException {
    synchronized (flushAllLock) {
      final DocumentsWriterDeleteQueue flushingDeleteQueue;
      synchronized (this) {
        flushingDeleteQueue = deleteQueue;
        deleteQueue = new DocumentsWriterDeleteQueue(new BufferedDeletes(false));
        assert setFlushingDeleteQueue(flushingDeleteQueue);
      }
      assert flushingDeleteQueue == currentFlusingSession;
      boolean anythingFlushed = false;
      boolean success = false;
      try {
        flushControl.markForFullFlush();
        DocumentsWriterPerThread flushingDWPT;
        // now try help out with flushing
        while ((flushingDWPT = flushControl.nextPendingFlush()) != null) {
          anythingFlushed |= doFlush(flushingDWPT);
        }
        // if a concurrent flush is still in flight wait for it
        while (!flushControl.allFlushesDue()) {
          flushControl.waitForFlush();  
        }
        if (!anythingFlushed && flushDeletes) {
          synchronized (ticketQueue) {
            ticketQueue.add(new FlushTicket(flushingDeleteQueue.freezeGlobalBuffer(null), false));
           }
          applyFlushTickets(null, null);
        }
        success = true;
        
      } finally {
        assert flushingDeleteQueue == currentFlusingSession;
        assert setFlushingDeleteQueue(null);
        if (!success) {
          flushControl.abortFullFlushes();
        } else {
          // release the flush lock
          flushControl.finishFullFlush();
        }
      }
      return anythingFlushed;
    }
  }
  
  static final class FlushTicket {
    final FrozenBufferedDeletes frozenDeletes;
    FlushedSegment segment;
    boolean isSegmentFlush;
    
    FlushTicket(FrozenBufferedDeletes frozenDeletes, boolean isSegmentFlush) {
      this.frozenDeletes = frozenDeletes;
      this.isSegmentFlush = isSegmentFlush;
    }
    
    boolean canPublish() {
      return (!isSegmentFlush || segment != null);  
    }
  }
}
