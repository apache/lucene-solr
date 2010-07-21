package org.apache.lucene.index;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMFile;
import org.apache.lucene.util.ArrayUtil;

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
    try {
      if (infoStream != null) {
        message("docWriter: now abort");
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

    } finally {
      aborting = false;
      if (infoStream != null) {
        message("docWriter: done abort");
      }
    }
  }

  
  final DocumentsWriterRAMAllocator ramAllocator = new DocumentsWriterRAMAllocator();

  final DocumentsWriter parent;
  final IndexWriter writer;
  
  final Directory directory;
  final DocState docState;
  final DocConsumer consumer;
  private DocFieldProcessor docFieldProcessor;
  
  String segment;                         // Current segment we are working on
  private String docStoreSegment;         // Current doc-store segment we are writing
  private int docStoreOffset;                     // Current starting doc-store offset of current segment
  boolean aborting;               // True if an abort is pending
  
  private final PrintStream infoStream;
  private int numDocsInRAM;
  private int numDocsInStore;
  private int flushedDocCount;
  SegmentWriteState flushState;

  long[] sequenceIDs = new long[8];
  
  final List<String> closedFiles = new ArrayList<String>();
  
  long numBytesUsed;
  
  public DocumentsWriterPerThread(Directory directory, DocumentsWriter parent, IndexingChain indexingChain) {
    this.directory = directory;
    this.parent = parent;
    this.writer = parent.indexWriter;
    this.infoStream = parent.indexWriter.getInfoStream();
    this.docState = new DocState(this);
    this.docState.similarity = parent.config.getSimilarity();
    this.docState.maxFieldLength = parent.config.getMaxFieldLength();
    
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
    initSegmentName(false);
  
    final DocWriter perDoc;
    
    boolean success = false;
    try {
      perDoc = consumer.processDocument();
      
      success = true;
    } finally {
      if (!success) {
        if (!aborting) {
          // mark document as deleted
          commitDocument(-1);
        }
      }
    }

    success = false;
    try {
      if (perDoc != null) {
        perDoc.finish();
      }
      
      success = true;
    } finally {
      if (!success) {
        setAborting();
      }
    }

  }

  public void commitDocument(long sequenceID) {
    if (numDocsInRAM == sequenceIDs.length) {
      sequenceIDs = ArrayUtil.grow(sequenceIDs);
    }
    
    sequenceIDs[numDocsInRAM] = sequenceID;
    numDocsInRAM++;
    numDocsInStore++;
  }
  
  int getNumDocsInRAM() {
    return numDocsInRAM;
  }
  
  long getMinSequenceID() {
    if (numDocsInRAM == 0) {
      return -1;
    }
    return sequenceIDs[0];
  }
  
  /** Returns true if any of the fields in the current
  *  buffered docs have omitTermFreqAndPositions==false */
  boolean hasProx() {
    return (docFieldProcessor != null) ? docFieldProcessor.fieldInfos.hasProx()
                                      : true;
  }
  
  Codec getCodec() {
    return flushState.codec;
  }
  
  void initSegmentName(boolean onlyDocStore) {
    if (segment == null && (!onlyDocStore || docStoreSegment == null)) {
      // this call is synchronized on IndexWriter.segmentInfos
      segment = writer.newSegmentName();
      assert numDocsInRAM == 0;
    }
    if (docStoreSegment == null) {
      docStoreSegment = segment;
      assert numDocsInStore == 0;
    }
  }

  
  private void initFlushState(boolean onlyDocStore) {
    initSegmentName(onlyDocStore);
    flushState = new SegmentWriteState(infoStream, directory, segment, docFieldProcessor.fieldInfos,
                                       docStoreSegment, numDocsInRAM, numDocsInStore, writer.getConfig().getTermIndexInterval(),
                                       writer.codecs);
  }
  
  /** Reset after a flush */
  private void doAfterFlush() throws IOException {
    segment = null;
    numDocsInRAM = 0;
  }
    
  /** Flush all pending docs to a new segment */
  SegmentInfo flush(boolean closeDocStore) throws IOException {
    assert numDocsInRAM > 0;

    initFlushState(closeDocStore);

    docStoreOffset = numDocsInStore;

    if (infoStream != null) {
      message("flush postings as segment " + flushState.segmentName + " numDocs=" + numDocsInRAM);
    }
    
    boolean success = false;

    try {

      if (closeDocStore) {
        assert flushState.docStoreSegmentName != null;
        assert flushState.docStoreSegmentName.equals(flushState.segmentName);
        closeDocStore();
        flushState.numDocsInStore = 0;
      }
      
      consumer.flush(flushState);

      if (infoStream != null) {
        SegmentInfo si = new SegmentInfo(flushState.segmentName,
            flushState.numDocs,
            directory, false,
            docStoreOffset, flushState.docStoreSegmentName,
            false,    
            hasProx(),
            getCodec());

        final long newSegmentSize = si.sizeInBytes();
        String message = "  ramUsed=" + ramAllocator.nf.format(((double) numBytesUsed)/1024./1024.) + " MB" +
          " newFlushedSize=" + newSegmentSize +
          " docs/MB=" + ramAllocator.nf.format(numDocsInRAM/(newSegmentSize/1024./1024.)) +
          " new/old=" + ramAllocator.nf.format(100.0*newSegmentSize/numBytesUsed) + "%";
        message(message);
      }

      flushedDocCount += flushState.numDocs;

      long maxSequenceID = sequenceIDs[numDocsInRAM-1];
      doAfterFlush();
      
      // Create new SegmentInfo, but do not add to our
      // segmentInfos until deletes are flushed
      // successfully.
      SegmentInfo newSegment = new SegmentInfo(flushState.segmentName,
                                   flushState.numDocs,
                                   directory, false,
                                   docStoreOffset, flushState.docStoreSegmentName,
                                   false,    
                                   hasProx(),
                                   getCodec());

      
      newSegment.setMinSequenceID(sequenceIDs[0]);
      newSegment.setMaxSequenceID(maxSequenceID);
      
      IndexWriter.setDiagnostics(newSegment, "flush");
      success = true;

      return newSegment;
    } finally {
      if (!success) {
        setAborting();
      }
    }
  }

  /** Closes the current open doc stores an returns the doc
   *  store segment name.  This returns null if there are *
   *  no buffered documents. */
  String closeDocStore() throws IOException {

    // nocommit
//    if (infoStream != null)
//      message("closeDocStore: " + openFiles.size() + " files to flush to segment " + docStoreSegment + " numDocs=" + numDocsInStore);
    
    boolean success = false;

    try {
      initFlushState(true);
      closedFiles.clear();

      consumer.closeDocStore(flushState);
      // nocommit
      //assert 0 == openFiles.size();

      String s = docStoreSegment;
      docStoreSegment = null;
      docStoreOffset = 0;
      numDocsInStore = 0;
      success = true;
      return s;
    } finally {
      if (!success) {
        parent.abort();
      }
    }
  }

  
  /** Get current segment name we are writing. */
  String getSegment() {
    return segment;
  }
  
  /** Returns the current doc store segment we are writing
   *  to. */
  String getDocStoreSegment() {
    return docStoreSegment;
  }

  /** Returns the doc offset into the shared doc store for
   *  the current buffered docs. */
  int getDocStoreOffset() {
    return docStoreOffset;
  }


  @SuppressWarnings("unchecked")
  List<String> closedFiles() {
    return (List<String>) ((ArrayList<String>) closedFiles).clone();
  }

  void addOpenFile(String name) {
    synchronized(parent.openFiles) {
      assert !parent.openFiles.contains(name);
      parent.openFiles.add(name);
    }
  }

  void removeOpenFile(String name) {
    synchronized(parent.openFiles) {
      assert parent.openFiles.contains(name);
      parent.openFiles.remove(name);
    }
    closedFiles.add(name);
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
      assert size == DocumentsWriterRAMAllocator.PER_DOC_BLOCK_SIZE;
      return ramAllocator.perDocAllocator.getByteBlock();
    }
    
    /**
     * Recycle the bytes used.
     */
    synchronized void recycle() {
      if (buffers.size() > 0) {
        setLength(0);
        
        // Recycle the blocks
        ramAllocator.perDocAllocator.recycleByteBlocks(buffers);
        buffers.clear();
        sizeInBytes = 0;
        
        assert numBuffers() == 0;
      }
    }
  }
  
  void bytesUsed(long numBytes) {
    ramAllocator.bytesUsed(numBytes);
  }
  
  void message(String message) {
    if (infoStream != null)
      writer.message("DW: " + message);
  }
}
