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

import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_MASK;
import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_SIZE;

import java.io.IOException;
import java.io.PrintStream;
import java.text.NumberFormat;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.ByteBlockPool.DirectAllocator;
import org.apache.lucene.util.RamUsageEstimator;

public class DocumentsWriterPerThread {

  /**
   * The IndexingChain must define the {@link #getChain(DocumentsWriter)} method
   * which returns the DocConsumer that the DocumentsWriter calls to process the
   * documents.
   */
  abstract static class IndexingChain {
    abstract DocConsumer getChain(DocumentsWriterPerThread documentsWriterPerThread);
  }


  static final IndexingChain defaultIndexingChain = new IndexingChain() {

    @Override
    DocConsumer getChain(DocumentsWriterPerThread documentsWriterPerThread) {
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

      final TermsHashConsumer termVectorsWriter = new TermVectorsTermsWriter(documentsWriterPerThread);
      final TermsHashConsumer freqProxWriter = new FreqProxTermsWriter();

      final InvertedDocConsumer  termsHash = new TermsHash(documentsWriterPerThread, freqProxWriter,
                                                           new TermsHash(documentsWriterPerThread, termVectorsWriter, null));
      final NormsWriter normsWriter = new NormsWriter();
      final DocInverter docInverter = new DocInverter(documentsWriterPerThread.docState, termsHash, normsWriter);
      return new DocFieldProcessor(documentsWriterPerThread, docInverter);
    }
  };

  // Deletes for our still-in-RAM (to be flushed next) segment
  private SegmentDeletes pendingDeletes = new SegmentDeletes();

  static class DocState {
    final DocumentsWriterPerThread docWriter;
    Analyzer analyzer;
    int maxFieldLength;
    PrintStream infoStream;
    Similarity similarity;
    int docID;
    Document doc;
    String maxTermPrefix;

    DocState(DocumentsWriterPerThread docWriter) {
      this.docWriter = docWriter;
    }

    // Only called by asserts
    public boolean testPoint(String name) {
      return docWriter.writer.testPoint(name);
    }
  }

  /** Called if we hit an exception at a bad time (when
   *  updating the index files) and must discard all
   *  currently buffered docs.  This resets our state,
   *  discarding any docs added since last flush. */
  void abort() throws IOException {
    aborting = true;
    try {
      if (infoStream != null) {
        message("docWriter: now abort");
      }
      try {
        consumer.abort();
      } catch (Throwable t) {
      }

      pendingDeletes.clear();
      // Reset all postings data
      doAfterFlush();

    } finally {
      aborting = false;
      if (infoStream != null) {
        message("docWriter: done abort");
      }
    }
  }

  final DocumentsWriter parent;
  final IndexWriter writer;

  final Directory directory;
  final DocState docState;
  final DocConsumer consumer;
  private DocFieldProcessor docFieldProcessor;

  String segment;                         // Current segment we are working on
  boolean aborting;               // True if an abort is pending

  private final PrintStream infoStream;
  private int numDocsInRAM;
  private int flushedDocCount;
  SegmentWriteState flushState;

  final AtomicLong bytesUsed = new AtomicLong(0);

  FieldInfos fieldInfos = new FieldInfos();

  public DocumentsWriterPerThread(Directory directory, DocumentsWriter parent, IndexingChain indexingChain) {
    this.directory = directory;
    this.parent = parent;
    this.writer = parent.indexWriter;
    this.infoStream = parent.indexWriter.getInfoStream();
    this.docState = new DocState(this);
    this.docState.similarity = parent.indexWriter.getConfig().getSimilarity();
    this.docState.maxFieldLength = IndexWriterConfig.UNLIMITED_FIELD_LENGTH;

    consumer = indexingChain.getChain(this);
    if (consumer instanceof DocFieldProcessor) {
      docFieldProcessor = (DocFieldProcessor) consumer;
    }
  }

  void setAborting() {
    aborting = true;
  }

  public void addDocument(Document doc, Analyzer analyzer) throws IOException {
    docState.doc = doc;
    docState.analyzer = analyzer;
    docState.docID = numDocsInRAM;
    if (segment == null) {
      // this call is synchronized on IndexWriter.segmentInfos
      segment = writer.newSegmentName();
      assert numDocsInRAM == 0;
    }

    boolean success = false;
    try {
      consumer.processDocument();

      success = true;
    } finally {
      if (!success) {
        if (!aborting) {
          // mark document as deleted
          deleteDocID(docState.docID);
          numDocsInRAM++;
        }
      }
    }

    success = false;
    try {
      consumer.finishDocument();

      success = true;
    } finally {
      if (!success) {
        abort();
      }
    }
  }

  void pushDeletes(SegmentInfo newSegment, SegmentInfos segmentInfos) {
    // Lock order: DW -> BD
    if (pendingDeletes.any()) {
      if (newSegment != null) {
        if (infoStream != null) {
          message("flush: push buffered deletes to newSegment");
        }
        parent.bufferedDeletes.pushDeletes(pendingDeletes, newSegment);
      } else if (segmentInfos.size() > 0) {
        if (infoStream != null) {
          message("flush: push buffered deletes to previously flushed segment " + segmentInfos.lastElement());
        }
        parent.bufferedDeletes.pushDeletes(pendingDeletes, segmentInfos.lastElement(), true);
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

  synchronized void deleteQueries(Query... queries) {
    for (Query query : queries) {
      pendingDeletes.addQuery(query, numDocsInRAM);
    }
  }

  synchronized void deleteQuery(Query query) {
    pendingDeletes.addQuery(query, numDocsInRAM);
  }

  synchronized void deleteTerms(Term... terms) {
    for (Term term : terms) {
      pendingDeletes.addTerm(term, numDocsInRAM);
    }
  }

  synchronized void deleteTerm(Term term) {
    pendingDeletes.addTerm(term, numDocsInRAM);
  }

  public void commitDocument() {
    numDocsInRAM++;
  }

  int getNumDocsInRAM() {
    return numDocsInRAM;
  }

  /** Returns true if any of the fields in the current
  *  buffered docs have omitTermFreqAndPositions==false */
  boolean hasProx() {
    return (docFieldProcessor != null) ? docFieldProcessor.fieldInfos.hasProx()
                                      : true;
  }

  SegmentCodecs getCodec() {
    return flushState.segmentCodecs;
  }

  /** Reset after a flush */
  private void doAfterFlush() throws IOException {
    segment = null;
    numDocsInRAM = 0;
  }

  /** Flush all pending docs to a new segment */
  SegmentInfo flush() throws IOException {
    assert numDocsInRAM > 0;

    flushState = new SegmentWriteState(infoStream, directory, segment, fieldInfos,
        numDocsInRAM, writer.getConfig().getTermIndexInterval(),
        SegmentCodecs.build(fieldInfos, writer.codecs));

    if (infoStream != null) {
      message("flush postings as segment " + flushState.segmentName + " numDocs=" + numDocsInRAM);
    }

    boolean success = false;

    try {
      consumer.flush(flushState);

      boolean hasVectors = flushState.hasVectors;

      if (infoStream != null) {
        SegmentInfo si = new SegmentInfo(flushState.segmentName,
            flushState.numDocs,
            directory, false,
            hasProx(),
            getCodec(),
            hasVectors);

        final long newSegmentSize = si.sizeInBytes(true);
        String message = "  ramUsed=" + nf.format(((double) bytesUsed.get())/1024./1024.) + " MB" +
          " newFlushedSize=" + newSegmentSize +
          " docs/MB=" + nf.format(numDocsInRAM/(newSegmentSize/1024./1024.)) +
          " new/old=" + nf.format(100.0*newSegmentSize/bytesUsed.get()) + "%";
        message(message);
      }

      flushedDocCount += flushState.numDocs;

      doAfterFlush();

      // Create new SegmentInfo, but do not add to our
      // segmentInfos until deletes are flushed
      // successfully.
      SegmentInfo newSegment = new SegmentInfo(flushState.segmentName,
                                   flushState.numDocs,
                                   directory, false,
                                   hasProx(),
                                   getCodec(),
                                   hasVectors);


      IndexWriter.setDiagnostics(newSegment, "flush");
      success = true;

      return newSegment;
    } finally {
      if (!success) {
        abort();
      }
    }
  }

  /** Get current segment name we are writing. */
  String getSegment() {
    return segment;
  }

  long bytesUsed() {
    return bytesUsed.get();
  }

  FieldInfos getFieldInfos() {
    return fieldInfos;
  }

  void message(String message) {
    writer.message("DW: " + message);
  }

  NumberFormat nf = NumberFormat.getInstance();

  /* Initial chunks size of the shared byte[] blocks used to
     store postings data */
  final static int BYTE_BLOCK_NOT_MASK = ~BYTE_BLOCK_MASK;

  /* if you increase this, you must fix field cache impl for
   * getTerms/getTermsIndex requires <= 32768 */
  final static int MAX_TERM_LENGTH_UTF8 = BYTE_BLOCK_SIZE-2;

  /* Initial chunks size of the shared int[] blocks used to
     store postings data */
  final static int INT_BLOCK_SHIFT = 13;
  final static int INT_BLOCK_SIZE = 1 << INT_BLOCK_SHIFT;
  final static int INT_BLOCK_MASK = INT_BLOCK_SIZE - 1;

  /* Allocate another int[] from the shared pool */
  int[] getIntBlock() {
    int[] b = new int[INT_BLOCK_SIZE];
    bytesUsed.addAndGet(INT_BLOCK_SIZE*RamUsageEstimator.NUM_BYTES_INT);
    return b;
  }

  final DirectAllocator byteBlockAllocator = new DirectAllocator();

  String toMB(long v) {
    return nf.format(v/1024./1024.);
  }

}
