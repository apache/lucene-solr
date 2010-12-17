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
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMFile;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ThreadInterruptedException;
import org.apache.lucene.util.RamUsageEstimator;

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
  IndexWriter writer;
  Directory directory;

  String segment;                         // Current segment we are working on
  private String docStoreSegment;         // Current doc-store segment we are writing
  private int docStoreOffset;                     // Current starting doc-store offset of current segment

  private int nextDocID;                          // Next docID to be added
  private int numDocsInRAM;                       // # docs buffered in RAM
  int numDocsInStore;                     // # docs written to doc stores

  // Max # ThreadState instances; if there are more threads
  // than this they share ThreadStates
  private DocumentsWriterThreadState[] threadStates = new DocumentsWriterThreadState[0];
  private final HashMap<Thread,DocumentsWriterThreadState> threadBindings = new HashMap<Thread,DocumentsWriterThreadState>();

  boolean bufferIsFull;                   // True when it's time to write segment
  private boolean aborting;               // True if an abort is pending

  private DocFieldProcessor docFieldProcessor;

  PrintStream infoStream;
  int maxFieldLength = IndexWriterConfig.UNLIMITED_FIELD_LENGTH;
  Similarity similarity;

  // max # simultaneous threads; if there are more than
  // this, they wait for others to finish first
  private final int maxThreadStates;

  List<String> newFiles;

  // Deletes for our still-in-RAM (to be flushed next) segment
  private SegmentDeletes pendingDeletes = new SegmentDeletes();
  
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
      return docWriter.writer.testPoint(name);
    }

    public void clear() {
      // don't hold onto doc nor analyzer, in case it is
      // largish:
      doc = null;
      analyzer = null;
    }
  }

  /** Consumer returns this on each doc.  This holds any
   *  state that must be flushed synchronized "in docID
   *  order".  We gather these and flush them in order. */
  abstract static class DocWriter {
    DocWriter next;
    int docID;
    abstract void finish() throws IOException;
    abstract void abort();
    abstract long sizeInBytes();

    void setNext(DocWriter next) {
      this.next = next;
    }
  }

  /**
   * Create and return a new DocWriterBuffer.
   */
  PerDocBuffer newPerDocBuffer() {
    return new PerDocBuffer();
  }

  /**
   * RAMFile buffer for DocWriters.
   */
  class PerDocBuffer extends RAMFile {
    
    /**
     * Allocate bytes used from shared pool.
     */
    protected byte[] newBuffer(int size) {
      assert size == PER_DOC_BLOCK_SIZE;
      return perDocAllocator.getByteBlock();
    }
    
    /**
     * Recycle the bytes used.
     */
    synchronized void recycle() {
      if (buffers.size() > 0) {
        setLength(0);
        
        // Recycle the blocks
        perDocAllocator.recycleByteBlocks(buffers);
        buffers.clear();
        sizeInBytes = 0;
        
        assert numBuffers() == 0;
      }
    }
  }
  
  /**
   * The IndexingChain must define the {@link #getChain(DocumentsWriter)} method
   * which returns the DocConsumer that the DocumentsWriter calls to process the
   * documents. 
   */
  abstract static class IndexingChain {
    abstract DocConsumer getChain(DocumentsWriter documentsWriter);
  }
  
  static final IndexingChain defaultIndexingChain = new IndexingChain() {

    @Override
    DocConsumer getChain(DocumentsWriter documentsWriter) {
      /*
      This is the current indexing chain:

      DocConsumer / DocConsumerPerThread
        --> code: DocFieldProcessor / DocFieldProcessorPerThread
          --> DocFieldConsumer / DocFieldConsumerPerThread / DocFieldConsumerPerField
            --> code: DocFieldConsumers / DocFieldConsumersPerThread / DocFieldConsumersPerField
              --> code: DocInverter / DocInverterPerThread / DocInverterPerField
                --> InvertedDocConsumer / InvertedDocConsumerPerThread / InvertedDocConsumerPerField
                  --> code: TermsHash / TermsHashPerThread / TermsHashPerField
                    --> TermsHashConsumer / TermsHashConsumerPerThread / TermsHashConsumerPerField
                      --> code: FreqProxTermsWriter / FreqProxTermsWriterPerThread / FreqProxTermsWriterPerField
                      --> code: TermVectorsTermsWriter / TermVectorsTermsWriterPerThread / TermVectorsTermsWriterPerField
                --> InvertedDocEndConsumer / InvertedDocConsumerPerThread / InvertedDocConsumerPerField
                  --> code: NormsWriter / NormsWriterPerThread / NormsWriterPerField
              --> code: StoredFieldsWriter / StoredFieldsWriterPerThread / StoredFieldsWriterPerField
    */

    // Build up indexing chain:

      final TermsHashConsumer termVectorsWriter = new TermVectorsTermsWriter(documentsWriter);
      final TermsHashConsumer freqProxWriter = new FreqProxTermsWriter();

      final InvertedDocConsumer  termsHash = new TermsHash(documentsWriter, true, freqProxWriter,
                                                           new TermsHash(documentsWriter, false, termVectorsWriter, null));
      final NormsWriter normsWriter = new NormsWriter();
      final DocInverter docInverter = new DocInverter(termsHash, normsWriter);
      return new DocFieldProcessor(documentsWriter, docInverter);
    }
  };

  final DocConsumer consumer;

  // How much RAM we can use before flushing.  This is 0 if
  // we are flushing by doc count instead.
  private long ramBufferSize = (long) (IndexWriterConfig.DEFAULT_RAM_BUFFER_SIZE_MB*1024*1024);
  private long waitQueuePauseBytes = (long) (ramBufferSize*0.1);
  private long waitQueueResumeBytes = (long) (ramBufferSize*0.05);

  // If we've allocated 5% over our RAM budget, we then
  // free down to 95%
  private long freeLevel = (long) (IndexWriterConfig.DEFAULT_RAM_BUFFER_SIZE_MB*1024*1024*0.95);

  // Flush @ this number of docs.  If ramBufferSize is
  // non-zero we will flush by RAM usage instead.
  private int maxBufferedDocs = IndexWriterConfig.DEFAULT_MAX_BUFFERED_DOCS;

  private boolean closed;
  private final FieldInfos fieldInfos;

  private final BufferedDeletes bufferedDeletes;
  private final IndexWriter.FlushControl flushControl;

  DocumentsWriter(Directory directory, IndexWriter writer, IndexingChain indexingChain, int maxThreadStates, FieldInfos fieldInfos, BufferedDeletes bufferedDeletes) throws IOException {
    this.directory = directory;
    this.writer = writer;
    this.similarity = writer.getConfig().getSimilarity();
    this.maxThreadStates = maxThreadStates;
    this.fieldInfos = fieldInfos;
    this.bufferedDeletes = bufferedDeletes;
    flushControl = writer.flushControl;

    consumer = indexingChain.getChain(this);
    if (consumer instanceof DocFieldProcessor) {
      docFieldProcessor = (DocFieldProcessor) consumer;
    }
  }

  // Buffer a specific docID for deletion.  Currently only
  // used when we hit a exception when adding a document
  synchronized void deleteDocID(int docIDUpto) {
    pendingDeletes.addDocID(docIDUpto);
    // NOTE: we do not trigger flush here.  This is
    // potentially a RAM leak, if you have an app that tries
    // to add docs but every single doc always hits a
    // non-aborting exception.  Allowing a flush here gets
    // very messy because we are only invoked when handling
    // exceptions so to do this properly, while handling an
    // exception we'd have to go off and flush new deletes
    // which is risky (likely would hit some other
    // confounding exception).
  }
  
  boolean deleteQueries(Query... queries) {
    final boolean doFlush = flushControl.waitUpdate(0, queries.length);
    synchronized(this) {
      for (Query query : queries) {
        pendingDeletes.addQuery(query, numDocsInRAM);
      }
    }
    return doFlush;
  }
  
  boolean deleteQuery(Query query) { 
    final boolean doFlush = flushControl.waitUpdate(0, 1);
    synchronized(this) {
      pendingDeletes.addQuery(query, numDocsInRAM);
    }
    return doFlush;
  }
  
  boolean deleteTerms(Term... terms) {
    final boolean doFlush = flushControl.waitUpdate(0, terms.length);
    synchronized(this) {
      for (Term term : terms) {
        pendingDeletes.addTerm(term, numDocsInRAM);
      }
    }
    return doFlush;
  }

  boolean deleteTerm(Term term, boolean skipWait) {
    final boolean doFlush = flushControl.waitUpdate(0, 1, skipWait);
    synchronized(this) {
      pendingDeletes.addTerm(term, numDocsInRAM);
    }
    return doFlush;
  }

  public FieldInfos getFieldInfos() {
    return fieldInfos;
  }

  /** Returns true if any of the fields in the current
   *  buffered docs have omitTermFreqAndPositions==false */
  boolean hasProx() {
    return (docFieldProcessor != null) ? fieldInfos.hasProx()
                                       : true;
  }

  /** If non-null, various details of indexing are printed
   *  here. */
  synchronized void setInfoStream(PrintStream infoStream) {
    this.infoStream = infoStream;
    for(int i=0;i<threadStates.length;i++)
      threadStates[i].docState.infoStream = infoStream;
  }

  synchronized void setMaxFieldLength(int maxFieldLength) {
    this.maxFieldLength = maxFieldLength;
    for(int i=0;i<threadStates.length;i++)
      threadStates[i].docState.maxFieldLength = maxFieldLength;
  }

  synchronized void setSimilarity(Similarity similarity) {
    this.similarity = similarity;
    for(int i=0;i<threadStates.length;i++)
      threadStates[i].docState.similarity = similarity;
  }

  /** Set how much RAM we can use before flushing. */
  synchronized void setRAMBufferSizeMB(double mb) {
    if (mb == IndexWriterConfig.DISABLE_AUTO_FLUSH) {
      ramBufferSize = IndexWriterConfig.DISABLE_AUTO_FLUSH;
      waitQueuePauseBytes = 4*1024*1024;
      waitQueueResumeBytes = 2*1024*1024;
    } else {
      ramBufferSize = (long) (mb*1024*1024);
      waitQueuePauseBytes = (long) (ramBufferSize*0.1);
      waitQueueResumeBytes = (long) (ramBufferSize*0.05);
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

  /** Get current segment name we are writing. */
  synchronized String getSegment() {
    return segment;
  }

  /** Returns how many docs are currently buffered in RAM. */
  synchronized int getNumDocsInRAM() {
    return numDocsInRAM;
  }

  /** Returns the current doc store segment we are writing
   *  to. */
  synchronized String getDocStoreSegment() {
    return docStoreSegment;
  }

  /** Returns the doc offset into the shared doc store for
   *  the current buffered docs. */
  synchronized int getDocStoreOffset() {
    return docStoreOffset;
  }

  /** Closes the current open doc stores an sets the
   *  docStoreSegment and docStoreUseCFS on the provided
   *  SegmentInfo. */
  synchronized void closeDocStore(SegmentWriteState flushState, IndexWriter writer, IndexFileDeleter deleter, SegmentInfo newSegment, MergePolicy mergePolicy, SegmentInfos segmentInfos) throws IOException {
    
    final boolean isSeparate = numDocsInRAM == 0 || !segment.equals(docStoreSegment);

    assert docStoreSegment != null;

    if (infoStream != null) {
      message("closeDocStore: files=" + openFiles + "; segment=" + docStoreSegment + "; docStoreOffset=" + docStoreOffset + "; numDocsInStore=" + numDocsInStore + "; isSeparate=" + isSeparate);
    }

    closedFiles.clear();
    consumer.closeDocStore(flushState);
    flushState.numDocsInStore = 0;
    assert 0 == openFiles.size();

    if (isSeparate) {
      flushState.flushedFiles.clear();

      if (mergePolicy.useCompoundDocStore(segmentInfos)) {

        final String compoundFileName = IndexFileNames.segmentFileName(docStoreSegment, IndexFileNames.COMPOUND_FILE_STORE_EXTENSION);

        if (infoStream != null) {
          message("closeDocStore: create compound file " + compoundFileName);
        }

        boolean success = false;
        try {

          CompoundFileWriter cfsWriter = new CompoundFileWriter(directory, compoundFileName);
          for (final String file : closedFiles) {
            cfsWriter.addFile(file);
          }
      
          // Perform the merge
          cfsWriter.close();

          success = true;
        } finally {
          if (!success) {
            deleter.deleteFile(compoundFileName);
          }
        }

        // In case the files we just merged into a CFS were
        // not registered w/ IFD:
        deleter.deleteNewFiles(closedFiles);

        final int numSegments = segmentInfos.size();
        for(int i=0;i<numSegments;i++) {
          SegmentInfo si = segmentInfos.info(i);
          if (si.getDocStoreOffset() != -1 &&
              si.getDocStoreSegment().equals(docStoreSegment)) {
            si.setDocStoreIsCompoundFile(true);
          }
        }

        newSegment.setDocStoreIsCompoundFile(true);
        if (infoStream != null) {
          message("closeDocStore: after compound file index=" + segmentInfos);
        }

        writer.checkpoint();
      }
    }

    docStoreSegment = null;
    docStoreOffset = 0;
    numDocsInStore = 0;
  }

  private Collection<String> abortedFiles;               // List of files that were written before last abort()

  Collection<String> abortedFiles() {
    return abortedFiles;
  }

  void message(String message) {
    if (infoStream != null)
      writer.message("DW: " + message);
  }

  final List<String> openFiles = new ArrayList<String>();
  final List<String> closedFiles = new ArrayList<String>();

  /* Returns Collection of files in use by this instance,
   * including any flushed segments. */
  @SuppressWarnings("unchecked")
  synchronized List<String> openFiles() {
    return (List<String>) ((ArrayList<String>) openFiles).clone();
  }

  synchronized void addOpenFile(String name) {
    assert !openFiles.contains(name);
    openFiles.add(name);
  }

  synchronized void removeOpenFile(String name) {
    assert openFiles.contains(name);
    openFiles.remove(name);
    closedFiles.add(name);
  }

  synchronized void setAborting() {
    if (infoStream != null) {
      message("setAborting");
    }
    aborting = true;
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

      // Forcefully remove waiting ThreadStates from line
      waitQueue.abort();

      // Wait for all other threads to finish with
      // DocumentsWriter:
      waitIdle();

      if (infoStream != null) {
        message("docWriter: abort waitIdle done");
      }

      assert 0 == waitQueue.numWaiting: "waitQueue.numWaiting=" + waitQueue.numWaiting;

      waitQueue.waitingBytes = 0;

      try {
        abortedFiles = openFiles();
      } catch (Throwable t) {
        abortedFiles = null;
      }

      pendingDeletes.clear();
        
      openFiles.clear();

      for(int i=0;i<threadStates.length;i++)
        try {
          threadStates[i].consumer.abort();
        } catch (Throwable t) {
        }

      try {
        consumer.abort();
      } catch (Throwable t) {
      }

      docStoreSegment = null;
      numDocsInStore = 0;
      docStoreOffset = 0;

      // Reset all postings data
      doAfterFlush();
      success = true;
    } finally {
      aborting = false;
      notifyAll();
      if (infoStream != null) {
        message("docWriter: done abort; abortedFiles=" + abortedFiles + " success=" + success);
      }
    }
  }

  /** Reset after a flush */
  private void doAfterFlush() throws IOException {
    // All ThreadStates should be idle when we are called
    assert allThreadsIdle();
    threadBindings.clear();
    waitQueue.reset();
    segment = null;
    numDocsInRAM = 0;
    nextDocID = 0;
    bufferIsFull = false;
    for(int i=0;i<threadStates.length;i++)
      threadStates[i].doAfterFlush();
  }

  private synchronized boolean allThreadsIdle() {
    for(int i=0;i<threadStates.length;i++)
      if (!threadStates[i].isIdle)
        return false;
    return true;
  }

  synchronized boolean anyChanges() {
    return numDocsInRAM != 0 || pendingDeletes.any();
  }

  // for testing
  public SegmentDeletes getPendingDeletes() {
    return pendingDeletes;
  }

  private void pushDeletes(SegmentInfo newSegment, SegmentInfos segmentInfos) {
    // Lock order: DW -> BD
    if (pendingDeletes.any()) {
      if (newSegment != null) {
        if (infoStream != null) {
          message("flush: push buffered deletes to newSegment");
        }
        bufferedDeletes.pushDeletes(pendingDeletes, newSegment);
      } else if (segmentInfos.size() > 0) {
        if (infoStream != null) {
          message("flush: push buffered deletes to previously flushed segment " + segmentInfos.lastElement());
        }
        bufferedDeletes.pushDeletes(pendingDeletes, segmentInfos.lastElement(), true);
      } else {
        if (infoStream != null) {
          message("flush: drop buffered deletes: no segments");
        }
        // We can safely discard these deletes: since
        // there are no segments, the deletions cannot
        // affect anything.
      }
      pendingDeletes = new SegmentDeletes();
    }
  }

  public boolean anyDeletions() {
    return pendingDeletes.any();
  }

  /** Flush all pending docs to a new segment */
  // Lock order: IW -> DW
  synchronized SegmentInfo flush(IndexWriter writer, boolean closeDocStore, IndexFileDeleter deleter, MergePolicy mergePolicy, SegmentInfos segmentInfos) throws IOException {

    // We change writer's segmentInfos:
    assert Thread.holdsLock(writer);

    waitIdle();

    if (numDocsInRAM == 0 && numDocsInStore == 0) {
      // nothing to do!
      if (infoStream != null) {
        message("flush: no docs; skipping");
      }
      // Lock order: IW -> DW -> BD
      pushDeletes(null, segmentInfos);
      return null;
    }

    if (aborting) {
      if (infoStream != null) {
        message("flush: skip because aborting is set");
      }
      return null;
    }

    boolean success = false;

    SegmentInfo newSegment;

    try {

      assert waitQueue.waitingBytes == 0;

      assert docStoreSegment != null || numDocsInRAM == 0: "dss=" + docStoreSegment + " numDocsInRAM=" + numDocsInRAM;

      assert numDocsInStore >= numDocsInRAM: "numDocsInStore=" + numDocsInStore + " numDocsInRAM=" + numDocsInRAM;

      final SegmentWriteState flushState = new SegmentWriteState(this, directory, segment, docStoreSegment, numDocsInRAM, numDocsInStore, writer.getConfig().getTermIndexInterval());

      newSegment = new SegmentInfo(segment, numDocsInRAM, directory, false, true, -1, null, false, hasProx());

      if (!closeDocStore || docStoreOffset != 0) {
        newSegment.setDocStoreSegment(docStoreSegment);
        newSegment.setDocStoreOffset(docStoreOffset);
      }

      if (closeDocStore) {
        closeDocStore(flushState, writer, deleter, newSegment, mergePolicy, segmentInfos);
      }

      if (numDocsInRAM > 0) {

        assert nextDocID == numDocsInRAM;
        assert waitQueue.numWaiting == 0;
        assert waitQueue.waitingBytes == 0;

        if (infoStream != null) {
          message("flush postings as segment " + segment + " numDocs=" + numDocsInRAM);
        }
    
        final Collection<DocConsumerPerThread> threads = new HashSet<DocConsumerPerThread>();
        for(int i=0;i<threadStates.length;i++) {
          threads.add(threadStates[i].consumer);
        }

        final long startNumBytesUsed = bytesUsed();
        consumer.flush(threads, flushState);

        if (infoStream != null) {
          message("flushedFiles=" + flushState.flushedFiles);
        }

        if (mergePolicy.useCompoundFile(segmentInfos, newSegment)) {

          final String cfsFileName = IndexFileNames.segmentFileName(segment, IndexFileNames.COMPOUND_FILE_EXTENSION);

          if (infoStream != null) {
            message("flush: create compound file \"" + cfsFileName + "\"");
          }

          CompoundFileWriter cfsWriter = new CompoundFileWriter(directory, cfsFileName);
          for(String fileName : flushState.flushedFiles) {
            cfsWriter.addFile(fileName);
          }
          cfsWriter.close();
          deleter.deleteNewFiles(flushState.flushedFiles);

          newSegment.setUseCompoundFile(true);
        }

        if (infoStream != null) {
          message("flush: segment=" + newSegment);
          final long newSegmentSize = newSegment.sizeInBytes();
          String message = "  ramUsed=" + nf.format(startNumBytesUsed/1024./1024.) + " MB" +
            " newFlushedSize=" + nf.format(newSegmentSize/1024/1024) + " MB" +
            " docs/MB=" + nf.format(numDocsInRAM/(newSegmentSize/1024./1024.)) +
            " new/old=" + nf.format(100.0*newSegmentSize/startNumBytesUsed) + "%";
          message(message);
        }

      } else {
        if (infoStream != null) {
          message("skip flushing segment: no docs");
        }
        newSegment = null;
      }

      success = true;
    } finally {
      notifyAll();
      if (!success) {
        if (segment != null) {
          deleter.refresh(segment);
        }
        abort();
      }
    }

    doAfterFlush();

    // Lock order: IW -> DW -> BD
    pushDeletes(newSegment, segmentInfos);

    docStoreOffset = numDocsInStore;

    return newSegment;
  }

  synchronized void close() {
    closed = true;
    notifyAll();
  }

  synchronized void initSegmentName(boolean onlyDocStore) {
    if (segment == null && (!onlyDocStore || docStoreSegment == null)) {
      segment = writer.newSegmentName();
      assert numDocsInRAM == 0;
    }
    if (docStoreSegment == null) {
      docStoreSegment = segment;
      assert numDocsInStore == 0;
    }
  }

  /** Returns a free (idle) ThreadState that may be used for
   * indexing this one document.  This call also pauses if a
   * flush is pending.  If delTerm is non-null then we
   * buffer this deleted term after the thread state has
   * been acquired. */
  synchronized DocumentsWriterThreadState getThreadState(Document doc, Term delTerm) throws IOException {

    final Thread currentThread = Thread.currentThread();
    assert !Thread.holdsLock(writer);

    // First, find a thread state.  If this thread already
    // has affinity to a specific ThreadState, use that one
    // again.
    DocumentsWriterThreadState state = threadBindings.get(currentThread);
    if (state == null) {

      // First time this thread has called us since last
      // flush.  Find the least loaded thread state:
      DocumentsWriterThreadState minThreadState = null;
      for(int i=0;i<threadStates.length;i++) {
        DocumentsWriterThreadState ts = threadStates[i];
        if (minThreadState == null || ts.numThreads < minThreadState.numThreads)
          minThreadState = ts;
      }
      if (minThreadState != null && (minThreadState.numThreads == 0 || threadStates.length >= maxThreadStates)) {
        state = minThreadState;
        state.numThreads++;
      } else {
        // Just create a new "private" thread state
        DocumentsWriterThreadState[] newArray = new DocumentsWriterThreadState[1+threadStates.length];
        if (threadStates.length > 0)
          System.arraycopy(threadStates, 0, newArray, 0, threadStates.length);
        state = newArray[threadStates.length] = new DocumentsWriterThreadState(this);
        threadStates = newArray;
      }
      threadBindings.put(currentThread, state);
    }

    // Next, wait until my thread state is idle (in case
    // it's shared with other threads), and no flush/abort
    // pending 
    waitReady(state);

    // Allocate segment name if this is the first doc since
    // last flush:
    initSegmentName(false);

    state.docState.docID = nextDocID++;

    if (delTerm != null) {
      pendingDeletes.addTerm(delTerm, state.docState.docID);
    }

    numDocsInRAM++;
    state.isIdle = false;
    return state;
  }
  
  boolean addDocument(Document doc, Analyzer analyzer) throws CorruptIndexException, IOException {
    return updateDocument(doc, analyzer, null);
  }
  
  boolean updateDocument(Document doc, Analyzer analyzer, Term delTerm)
    throws CorruptIndexException, IOException {

    // Possibly trigger a flush, or wait until any running flush completes:
    boolean doFlush = flushControl.waitUpdate(1, delTerm != null ? 1 : 0);

    // This call is synchronized but fast
    final DocumentsWriterThreadState state = getThreadState(doc, delTerm);

    final DocState docState = state.docState;
    docState.doc = doc;
    docState.analyzer = analyzer;

    boolean success = false;
    try {
      // This call is not synchronized and does all the
      // work
      final DocWriter perDoc;
      try {
        perDoc = state.consumer.processDocument();
      } finally {
        docState.clear();
      }

      // This call is synchronized but fast
      finishDocument(state, perDoc);

      success = true;
    } finally {
      if (!success) {

        // If this thread state had decided to flush, we
        // must clear it so another thread can flush
        if (doFlush) {
          flushControl.clearFlushPending();
        }

        if (infoStream != null) {
          message("exception in updateDocument aborting=" + aborting);
        }

        synchronized(this) {

          state.isIdle = true;
          notifyAll();
            
          if (aborting) {
            abort();
          } else {
            skipDocWriter.docID = docState.docID;
            boolean success2 = false;
            try {
              waitQueue.add(skipDocWriter);
              success2 = true;
            } finally {
              if (!success2) {
                abort();
                return false;
              }
            }

            // Immediately mark this document as deleted
            // since likely it was partially added.  This
            // keeps indexing as "all or none" (atomic) when
            // adding a document:
            deleteDocID(state.docState.docID);
          }
        }
      }
    }

    doFlush |= flushControl.flushByRAMUsage("new document");

    return doFlush;
  }

  public synchronized void waitIdle() {
    while (!allThreadsIdle()) {
      try {
        wait();
      } catch (InterruptedException ie) {
        throw new ThreadInterruptedException(ie);
      }
    }
  }

  synchronized void waitReady(DocumentsWriterThreadState state) {
    while (!closed && (!state.isIdle || aborting)) {
      try {
        wait();
      } catch (InterruptedException ie) {
        throw new ThreadInterruptedException(ie);
      }
    }

    if (closed) {
      throw new AlreadyClosedException("this IndexWriter is closed");
    }
  }

  /** Does the synchronized work to finish/flush the
   *  inverted document. */
  private void finishDocument(DocumentsWriterThreadState perThread, DocWriter docWriter) throws IOException {

    // Must call this w/o holding synchronized(this) else
    // we'll hit deadlock:
    balanceRAM();

    synchronized(this) {

      assert docWriter == null || docWriter.docID == perThread.docState.docID;

      if (aborting) {

        // We are currently aborting, and another thread is
        // waiting for me to become idle.  We just forcefully
        // idle this threadState; it will be fully reset by
        // abort()
        if (docWriter != null) {
          try {
            docWriter.abort();
          } catch (Throwable t) {
          }
        }

        perThread.isIdle = true;

        // wakes up any threads waiting on the wait queue
        notifyAll();

        return;
      }

      final boolean doPause;

      if (docWriter != null)
        doPause = waitQueue.add(docWriter);
      else {
        skipDocWriter.docID = perThread.docState.docID;
        doPause = waitQueue.add(skipDocWriter);
      }

      if (doPause)
        waitForWaitQueue();

      perThread.isIdle = true;

      // wakes up any threads waiting on the wait queue
      notifyAll();
    }
  }

  synchronized void waitForWaitQueue() {
    do {
      try {
        wait();
      } catch (InterruptedException ie) {
        throw new ThreadInterruptedException(ie);
      }
    } while (!waitQueue.doResume());
  }

  private static class SkipDocWriter extends DocWriter {
    @Override
    void finish() {
    }
    @Override
    void abort() {
    }
    @Override
    long sizeInBytes() {
      return 0;
    }
  }
  final SkipDocWriter skipDocWriter = new SkipDocWriter();

  NumberFormat nf = NumberFormat.getInstance();

  /* Initial chunks size of the shared byte[] blocks used to
     store postings data */
  final static int BYTE_BLOCK_SHIFT = 15;
  final static int BYTE_BLOCK_SIZE = 1 << BYTE_BLOCK_SHIFT;
  final static int BYTE_BLOCK_MASK = BYTE_BLOCK_SIZE - 1;
  final static int BYTE_BLOCK_NOT_MASK = ~BYTE_BLOCK_MASK;

  private class ByteBlockAllocator extends ByteBlockPool.Allocator {
    final int blockSize;

    ByteBlockAllocator(int blockSize) {
      this.blockSize = blockSize;
    }

    ArrayList<byte[]> freeByteBlocks = new ArrayList<byte[]>();
    
    /* Allocate another byte[] from the shared pool */
    @Override
    byte[] getByteBlock() {
      synchronized(DocumentsWriter.this) {
        final int size = freeByteBlocks.size();
        final byte[] b;
        if (0 == size) {
          b = new byte[blockSize];
          bytesUsed.addAndGet(blockSize);
        } else
          b = freeByteBlocks.remove(size-1);
        return b;
      }
    }

    /* Return byte[]'s to the pool */

    @Override
    void recycleByteBlocks(byte[][] blocks, int start, int end) {
      synchronized(DocumentsWriter.this) {
        for(int i=start;i<end;i++) {
          freeByteBlocks.add(blocks[i]);
          blocks[i] = null;
        }
      }
    }

    @Override
    void recycleByteBlocks(List<byte[]> blocks) {
      synchronized(DocumentsWriter.this) {
        final int size = blocks.size();
        for(int i=0;i<size;i++) {
          freeByteBlocks.add(blocks.get(i));
          blocks.set(i, null);
        }
      }
    }
  }

  /* Initial chunks size of the shared int[] blocks used to
     store postings data */
  final static int INT_BLOCK_SHIFT = 13;
  final static int INT_BLOCK_SIZE = 1 << INT_BLOCK_SHIFT;
  final static int INT_BLOCK_MASK = INT_BLOCK_SIZE - 1;

  private ArrayList<int[]> freeIntBlocks = new ArrayList<int[]>();

  /* Allocate another int[] from the shared pool */
  synchronized int[] getIntBlock() {
    final int size = freeIntBlocks.size();
    final int[] b;
    if (0 == size) {
      b = new int[INT_BLOCK_SIZE];
      bytesUsed.addAndGet(INT_BLOCK_SIZE*RamUsageEstimator.NUM_BYTES_INT);
    } else
      b = freeIntBlocks.remove(size-1);
    return b;
  }

  synchronized void bytesUsed(long numBytes) {
    bytesUsed.addAndGet(numBytes);
  }

  long bytesUsed() {
    return bytesUsed.get() + pendingDeletes.bytesUsed.get();
  }

  /* Return int[]s to the pool */
  synchronized void recycleIntBlocks(int[][] blocks, int start, int end) {
    for(int i=start;i<end;i++) {
      freeIntBlocks.add(blocks[i]);
      blocks[i] = null;
    }
  }

  ByteBlockAllocator byteBlockAllocator = new ByteBlockAllocator(BYTE_BLOCK_SIZE);

  final static int PER_DOC_BLOCK_SIZE = 1024;

  final ByteBlockAllocator perDocAllocator = new ByteBlockAllocator(PER_DOC_BLOCK_SIZE);


  /* Initial chunk size of the shared char[] blocks used to
     store term text */
  final static int CHAR_BLOCK_SHIFT = 14;
  final static int CHAR_BLOCK_SIZE = 1 << CHAR_BLOCK_SHIFT;
  final static int CHAR_BLOCK_MASK = CHAR_BLOCK_SIZE - 1;

  final static int MAX_TERM_LENGTH = CHAR_BLOCK_SIZE-1;

  private ArrayList<char[]> freeCharBlocks = new ArrayList<char[]>();

  /* Allocate another char[] from the shared pool */
  synchronized char[] getCharBlock() {
    final int size = freeCharBlocks.size();
    final char[] c;
    if (0 == size) {
      bytesUsed.addAndGet(CHAR_BLOCK_SIZE * RamUsageEstimator.NUM_BYTES_CHAR);
      c = new char[CHAR_BLOCK_SIZE];
    } else
      c = freeCharBlocks.remove(size-1);
    // We always track allocations of char blocks, for now,
    // because nothing that skips allocation tracking
    // (currently only term vectors) uses its own char
    // blocks.
    return c;
  }

  /* Return char[]s to the pool */
  synchronized void recycleCharBlocks(char[][] blocks, int numBlocks) {
    for(int i=0;i<numBlocks;i++) {
      freeCharBlocks.add(blocks[i]);
      blocks[i] = null;
    }
  }

  String toMB(long v) {
    return nf.format(v/1024./1024.);
  }

  /* We have four pools of RAM: Postings, byte blocks
   * (holds freq/prox posting data), char blocks (holds
   * characters in the term) and per-doc buffers (stored fields/term vectors).  
   * Different docs require varying amount of storage from 
   * these four classes.
   * 
   * For example, docs with many unique single-occurrence
   * short terms will use up the Postings RAM and hardly any
   * of the other two.  Whereas docs with very large terms
   * will use alot of char blocks RAM and relatively less of
   * the other two.  This method just frees allocations from
   * the pools once we are over-budget, which balances the
   * pools to match the current docs. */
  void balanceRAM() {

    final boolean doBalance;
    final long deletesRAMUsed;

    deletesRAMUsed = bufferedDeletes.bytesUsed();

    synchronized(this) {
      if (ramBufferSize == IndexWriterConfig.DISABLE_AUTO_FLUSH || bufferIsFull) {
        return;
      }
    
      doBalance = bytesUsed() + deletesRAMUsed >= ramBufferSize;
    }

    if (doBalance) {

      if (infoStream != null)
        message("  RAM: balance allocations: usedMB=" + toMB(bytesUsed()) +
                " vs trigger=" + toMB(ramBufferSize) +
                " deletesMB=" + toMB(deletesRAMUsed) +
                " byteBlockFree=" + toMB(byteBlockAllocator.freeByteBlocks.size()*BYTE_BLOCK_SIZE) +
                " perDocFree=" + toMB(perDocAllocator.freeByteBlocks.size()*PER_DOC_BLOCK_SIZE) +
                " charBlockFree=" + toMB(freeCharBlocks.size()*CHAR_BLOCK_SIZE*RamUsageEstimator.NUM_BYTES_CHAR));

      final long startBytesUsed = bytesUsed() + deletesRAMUsed;

      int iter = 0;

      // We free equally from each pool in 32 KB
      // chunks until we are below our threshold
      // (freeLevel)

      boolean any = true;

      while(bytesUsed()+deletesRAMUsed > freeLevel) {
      
        synchronized(this) {
          if (0 == perDocAllocator.freeByteBlocks.size() 
              && 0 == byteBlockAllocator.freeByteBlocks.size() 
              && 0 == freeCharBlocks.size() 
              && 0 == freeIntBlocks.size() 
              && !any) {
            // Nothing else to free -- must flush now.
            bufferIsFull = bytesUsed()+deletesRAMUsed > ramBufferSize;
            if (infoStream != null) {
              if (bytesUsed()+deletesRAMUsed > ramBufferSize)
                message("    nothing to free; set bufferIsFull");
              else
                message("    nothing to free");
            }
            break;
          }

          if ((0 == iter % 5) && byteBlockAllocator.freeByteBlocks.size() > 0) {
            byteBlockAllocator.freeByteBlocks.remove(byteBlockAllocator.freeByteBlocks.size()-1);
            bytesUsed.addAndGet(-BYTE_BLOCK_SIZE);
          }

          if ((1 == iter % 5) && freeCharBlocks.size() > 0) {
            freeCharBlocks.remove(freeCharBlocks.size()-1);
            bytesUsed.addAndGet(-CHAR_BLOCK_SIZE * RamUsageEstimator.NUM_BYTES_CHAR);
          }

          if ((2 == iter % 5) && freeIntBlocks.size() > 0) {
            freeIntBlocks.remove(freeIntBlocks.size()-1);
            bytesUsed.addAndGet(-INT_BLOCK_SIZE * RamUsageEstimator.NUM_BYTES_INT);
          }

          if ((3 == iter % 5) && perDocAllocator.freeByteBlocks.size() > 0) {
            // Remove upwards of 32 blocks (each block is 1K)
            for (int i = 0; i < 32; ++i) {
              perDocAllocator.freeByteBlocks.remove(perDocAllocator.freeByteBlocks.size() - 1);
              bytesUsed.addAndGet(-PER_DOC_BLOCK_SIZE);
              if (perDocAllocator.freeByteBlocks.size() == 0) {
                break;
              }
            }
          }
        }

        if ((4 == iter % 5) && any)
          // Ask consumer to free any recycled state
          any = consumer.freeRAM();

        iter++;
      }

      if (infoStream != null)
        message("    after free: freedMB=" + nf.format((startBytesUsed-bytesUsed()-deletesRAMUsed)/1024./1024.) + " usedMB=" + nf.format((bytesUsed()+deletesRAMUsed)/1024./1024.));
    }
  }

  final WaitQueue waitQueue = new WaitQueue();

  private class WaitQueue {
    DocWriter[] waiting;
    int nextWriteDocID;
    int nextWriteLoc;
    int numWaiting;
    long waitingBytes;

    public WaitQueue() {
      waiting = new DocWriter[10];
    }

    synchronized void reset() {
      // NOTE: nextWriteLoc doesn't need to be reset
      assert numWaiting == 0;
      assert waitingBytes == 0;
      nextWriteDocID = 0;
    }

    synchronized boolean doResume() {
      return waitingBytes <= waitQueueResumeBytes;
    }

    synchronized boolean doPause() {
      return waitingBytes > waitQueuePauseBytes;
    }

    synchronized void abort() {
      int count = 0;
      for(int i=0;i<waiting.length;i++) {
        final DocWriter doc = waiting[i];
        if (doc != null) {
          doc.abort();
          waiting[i] = null;
          count++;
        }
      }
      waitingBytes = 0;
      assert count == numWaiting;
      numWaiting = 0;
    }

    private void writeDocument(DocWriter doc) throws IOException {
      assert doc == skipDocWriter || nextWriteDocID == doc.docID;
      boolean success = false;
      try {
        doc.finish();
        nextWriteDocID++;
        numDocsInStore++;
        nextWriteLoc++;
        assert nextWriteLoc <= waiting.length;
        if (nextWriteLoc == waiting.length)
          nextWriteLoc = 0;
        success = true;
      } finally {
        if (!success) {
          setAborting();
        }
      }
    }

    synchronized public boolean add(DocWriter doc) throws IOException {

      assert doc.docID >= nextWriteDocID;

      if (doc.docID == nextWriteDocID) {
        writeDocument(doc);
        while(true) {
          doc = waiting[nextWriteLoc];
          if (doc != null) {
            numWaiting--;
            waiting[nextWriteLoc] = null;
            waitingBytes -= doc.sizeInBytes();
            writeDocument(doc);
          } else {
            break;
          }
        }
      } else {

        // I finished before documents that were added
        // before me.  This can easily happen when I am a
        // small doc and the docs before me were large, or,
        // just due to luck in the thread scheduling.  Just
        // add myself to the queue and when that large doc
        // finishes, it will flush me:
        int gap = doc.docID - nextWriteDocID;
        if (gap >= waiting.length) {
          // Grow queue
          DocWriter[] newArray = new DocWriter[ArrayUtil.oversize(gap, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
          assert nextWriteLoc >= 0;
          System.arraycopy(waiting, nextWriteLoc, newArray, 0, waiting.length-nextWriteLoc);
          System.arraycopy(waiting, 0, newArray, waiting.length-nextWriteLoc, nextWriteLoc);
          nextWriteLoc = 0;
          waiting = newArray;
          gap = doc.docID - nextWriteDocID;
        }

        int loc = nextWriteLoc + gap;
        if (loc >= waiting.length)
          loc -= waiting.length;

        // We should only wrap one time
        assert loc < waiting.length;

        // Nobody should be in my spot!
        assert waiting[loc] == null;
        waiting[loc] = doc;
        numWaiting++;
        waitingBytes += doc.sizeInBytes();
      }
      
      return doPause();
    }
  }
}
