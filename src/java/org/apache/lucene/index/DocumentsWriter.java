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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.UnicodeUtil;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.text.NumberFormat;
import java.util.Collections;

/**
 * This class accepts multiple added documents and directly
 * writes a single segment file.  It does this more
 * efficiently than creating a single segment per document
 * (with DocumentWriter) and doing standard merges on those
 * segments.
 *
 * When a document is added, its stored fields (if any) and
 * term vectors (if any) are immediately written to the
 * Directory (ie these do not consume RAM).  The freq/prox
 * postings are accumulated into a Postings hash table keyed
 * by term.  Each entry in this hash table holds a separate
 * byte stream (allocated as incrementally growing slices
 * into large shared byte[] arrays) for freq and prox, that
 * contains the postings data for multiple documents.  If
 * vectors are enabled, each unique term for each document
 * also allocates a PostingVector instance to similarly
 * track the offsets & positions byte stream.
 *
 * Once the Postings hash is full (ie is consuming the
 * allowed RAM) or the number of added docs is large enough
 * (in the case we are flushing by doc count instead of RAM
 * usage), we create a real segment and flush it to disk and
 * reset the Postings hash.
 *
 * In adding a document we first organize all of its fields
 * by field name.  We then process field by field, and
 * record the Posting hash per-field.  After each field we
 * flush its term vectors.  When it's time to flush the full
 * segment we first sort the fields by name, and then go
 * field by field and sorts its postings.
 *
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
 * Each ThreadState instance has its own Posting hash. Once
 * we're using too much RAM, we flush all Posting hashes to
 * a segment by merging the docIDs in the posting lists for
 * the same term across multiple thread states (see
 * writeSegment and appendPostings).
 *
 * When flush is called by IndexWriter, or, we flush
 * internally when autoCommit=false, we forcefully idle all
 * threads and flush only once they are all idle.  This
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

  IndexWriter writer;
  Directory directory;

  FieldInfos fieldInfos = new FieldInfos(); // All fields we've seen
  IndexOutput tvx, tvf, tvd;              // To write term vectors
  FieldsWriter fieldsWriter;              // To write stored fields

  String segment;                         // Current segment we are working on
  String docStoreSegment;                 // Current doc-store segment we are writing
  private int docStoreOffset;                     // Current starting doc-store offset of current segment

  private int nextDocID;                          // Next docID to be added
  private int numDocsInRAM;                       // # docs buffered in RAM
  int numDocsInStore;                     // # docs written to doc stores
  private int nextWriteDocID;                     // Next docID to be written

  // Max # ThreadState instances; if there are more threads
  // than this they share ThreadStates
  private final static int MAX_THREAD_STATE = 5;
  private DocumentsWriterThreadState[] threadStates = new DocumentsWriterThreadState[0];
  private final HashMap threadBindings = new HashMap();
  private int numWaiting;
  private final DocumentsWriterThreadState[] waitingThreadStates = new DocumentsWriterThreadState[MAX_THREAD_STATE];
  private int pauseThreads;                       // Non-zero when we need all threads to
                                                  // pause (eg to flush)
  boolean flushPending;                   // True when a thread has decided to flush
  boolean bufferIsFull;                   // True when it's time to write segment
  private int abortCount;                         // Non-zero while abort is pending or running

  PrintStream infoStream;

  boolean hasNorms;                       // Whether any norms were seen since last flush

  List newFiles;

  // Deletes done after the last flush; these are discarded
  // on abort
  private BufferedDeletes deletesInRAM = new BufferedDeletes();

  // Deletes done before the last flush; these are still
  // kept on abort
  private BufferedDeletes deletesFlushed = new BufferedDeletes();

  // The max number of delete terms that can be buffered before
  // they must be flushed to disk.
  private int maxBufferedDeleteTerms = IndexWriter.DEFAULT_MAX_BUFFERED_DELETE_TERMS;

  // How much RAM we can use before flushing.  This is 0 if
  // we are flushing by doc count instead.
  private long ramBufferSize = (long) (IndexWriter.DEFAULT_RAM_BUFFER_SIZE_MB*1024*1024);

  // Flush @ this number of docs.  If rarmBufferSize is
  // non-zero we will flush by RAM usage instead.
  private int maxBufferedDocs = IndexWriter.DEFAULT_MAX_BUFFERED_DOCS;

  private int flushedDocCount;                      // How many docs already flushed to index

  synchronized void updateFlushedDocCount(int n) {
    flushedDocCount += n;
  }
  synchronized int getFlushedDocCount() {
    return flushedDocCount;
  }
  synchronized void setFlushedDocCount(int n) {
    flushedDocCount = n;
  }

  private boolean closed;

  // Coarse estimates used to measure RAM usage of buffered deletes
  private static int OBJECT_HEADER_BYTES = 8;

  BufferedNorms[] norms = new BufferedNorms[0];   // Holds norms until we flush

  DocumentsWriter(Directory directory, IndexWriter writer) throws IOException {
    this.directory = directory;
    this.writer = writer;
    flushedDocCount = writer.docCount();
    postingsFreeList = new Posting[0];
  }

  /** If non-null, various details of indexing are printed
   *  here. */
  void setInfoStream(PrintStream infoStream) {
    this.infoStream = infoStream;
  }

  /** Set how much RAM we can use before flushing. */
  void setRAMBufferSizeMB(double mb) {
    if (mb == IndexWriter.DISABLE_AUTO_FLUSH) {
      ramBufferSize = IndexWriter.DISABLE_AUTO_FLUSH;
    } else {
      ramBufferSize = (long) (mb*1024*1024);
    }
  }

  double getRAMBufferSizeMB() {
    if (ramBufferSize == IndexWriter.DISABLE_AUTO_FLUSH) {
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
  String getSegment() {
    return segment;
  }

  /** Returns how many docs are currently buffered in RAM. */
  int getNumDocsInRAM() {
    return numDocsInRAM;
  }

  /** Returns the current doc store segment we are writing
   *  to.  This will be the same as segment when autoCommit
   *  * is true. */
  String getDocStoreSegment() {
    return docStoreSegment;
  }

  /** Returns the doc offset into the shared doc store for
   *  the current buffered docs. */
  int getDocStoreOffset() {
    return docStoreOffset;
  }

  /** Closes the current open doc stores an returns the doc
   *  store segment name.  This returns null if there are *
   *  no buffered documents. */
  String closeDocStore() throws IOException {

    assert allThreadsIdle();

    List flushedFiles = files();

    if (infoStream != null)
      message("closeDocStore: " + flushedFiles.size() + " files to flush to segment " + docStoreSegment + " numDocs=" + numDocsInStore);

    if (flushedFiles.size() > 0) {
      files = null;

      if (tvx != null) {
        // At least one doc in this run had term vectors enabled
        assert docStoreSegment != null;
        tvx.close();
        tvf.close();
        tvd.close();
        tvx = null;
        assert 4+numDocsInStore*16 == directory.fileLength(docStoreSegment + "." + IndexFileNames.VECTORS_INDEX_EXTENSION):
          "after flush: tvx size mismatch: " + numDocsInStore + " docs vs " + directory.fileLength(docStoreSegment + "." + IndexFileNames.VECTORS_INDEX_EXTENSION) + " length in bytes of " + docStoreSegment + "." + IndexFileNames.VECTORS_INDEX_EXTENSION;
      }

      if (fieldsWriter != null) {
        assert docStoreSegment != null;
        fieldsWriter.close();
        fieldsWriter = null;
        assert 4+numDocsInStore*8 == directory.fileLength(docStoreSegment + "." + IndexFileNames.FIELDS_INDEX_EXTENSION):
          "after flush: fdx size mismatch: " + numDocsInStore + " docs vs " + directory.fileLength(docStoreSegment + "." + IndexFileNames.FIELDS_INDEX_EXTENSION) + " length in bytes of " + docStoreSegment + "." + IndexFileNames.FIELDS_INDEX_EXTENSION;
      }

      String s = docStoreSegment;
      docStoreSegment = null;
      docStoreOffset = 0;
      numDocsInStore = 0;
      return s;
    } else {
      return null;
    }
  }

  List files = null;                      // Cached list of files we've created
  private List abortedFiles = null;               // List of files that were written before last abort()

  List abortedFiles() {
    return abortedFiles;
  }

  void message(String message) {
    writer.message("DW: " + message);
  }

  /* Returns list of files in use by this instance,
   * including any flushed segments. */
  synchronized List files() {

    if (files != null)
      return files;

    files = new ArrayList();

    // Stored fields:
    if (fieldsWriter != null) {
      assert docStoreSegment != null;
      files.add(docStoreSegment + "." + IndexFileNames.FIELDS_EXTENSION);
      files.add(docStoreSegment + "." + IndexFileNames.FIELDS_INDEX_EXTENSION);
    }

    // Vectors:
    if (tvx != null) {
      assert docStoreSegment != null;
      files.add(docStoreSegment + "." + IndexFileNames.VECTORS_INDEX_EXTENSION);
      files.add(docStoreSegment + "." + IndexFileNames.VECTORS_FIELDS_EXTENSION);
      files.add(docStoreSegment + "." + IndexFileNames.VECTORS_DOCUMENTS_EXTENSION);
    }

    return files;
  }

  synchronized void setAborting() {
    abortCount++;
  }

  /** Called if we hit an exception at a bad time (when
   *  updating the index files) and must discard all
   *  currently buffered docs.  This resets our state,
   *  discarding any docs added since last flush.  If ae is
   *  non-null, it contains the root cause exception (which
   *  we re-throw after we are done aborting). */
  synchronized void abort(AbortException ae) throws IOException {

    // Anywhere that throws an AbortException must first
    // mark aborting to make sure while the exception is
    // unwinding the un-synchronized stack, no thread grabs
    // the corrupt ThreadState that hit the aborting
    // exception:
    assert ae == null || abortCount>0;

    try {

      if (infoStream != null)
        message("docWriter: now abort");

      // Forcefully remove waiting ThreadStates from line
      for(int i=0;i<numWaiting;i++)
        waitingThreadStates[i].isIdle = true;
      numWaiting = 0;

      // Wait for all other threads to finish with DocumentsWriter:
      pauseAllThreads();

      assert 0 == numWaiting;

      try {

        deletesInRAM.clear();

        try {
          abortedFiles = files();
        } catch (Throwable t) {
          abortedFiles = null;
        }

        docStoreSegment = null;
        numDocsInStore = 0;
        docStoreOffset = 0;
        files = null;

        // Clear vectors & fields from ThreadStates
        for(int i=0;i<threadStates.length;i++) {
          DocumentsWriterThreadState state = threadStates[i];
          state.tvfLocal.reset();
          state.fdtLocal.reset();
          if (state.localFieldsWriter != null) {
            try {
              state.localFieldsWriter.close();
            } catch (Throwable t) {
            }
            state.localFieldsWriter = null;
          }
        }

        // Reset vectors writer
        if (tvx != null) {
          try {
            tvx.close();
          } catch (Throwable t) {
          }
          tvx = null;
        }
        if (tvd != null) {
          try {
            tvd.close();
          } catch (Throwable t) {
          }
          tvd = null;
        }
        if (tvf != null) {
          try {
            tvf.close();
          } catch (Throwable t) {
          }
          tvf = null;
        }

        // Reset fields writer
        if (fieldsWriter != null) {
          try {
            fieldsWriter.close();
          } catch (Throwable t) {
          }
          fieldsWriter = null;
        }

        // Discard pending norms:
        final int numField = fieldInfos.size();
        for (int i=0;i<numField;i++) {
          FieldInfo fi = fieldInfos.fieldInfo(i);
          if (fi.isIndexed && !fi.omitNorms) {
            BufferedNorms n = norms[i];
            if (n != null)
              try {
                n.reset();
              } catch (Throwable t) {
              }
          }
        }

        // Reset all postings data
        resetPostingsData();

      } finally {
        resumeAllThreads();
      }

      // If we have a root cause exception, re-throw it now:
      if (ae != null) {
        Throwable t = ae.getCause();
        if (t instanceof IOException)
          throw (IOException) t;
        else if (t instanceof RuntimeException)
          throw (RuntimeException) t;
        else if (t instanceof Error)
          throw (Error) t;
        else
          // Should not get here
          assert false: "unknown exception: " + t;
      }
    } finally {
      if (ae != null)
        abortCount--;
      notifyAll();
    }
  }

  /** Reset after a flush */
  private void resetPostingsData() throws IOException {
    // All ThreadStates should be idle when we are called
    assert allThreadsIdle();
    threadBindings.clear();
    segment = null;
    numDocsInRAM = 0;
    nextDocID = 0;
    nextWriteDocID = 0;
    files = null;
    balanceRAM();
    bufferIsFull = false;
    flushPending = false;
    for(int i=0;i<threadStates.length;i++) {
      threadStates[i].numThreads = 0;
      threadStates[i].resetPostings();
    }
    numBytesUsed = 0;
  }

  // Returns true if an abort is in progress
  synchronized boolean pauseAllThreads() {
    pauseThreads++;
    while(!allThreadsIdle()) {
      try {
        wait();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    return abortCount > 0;
  }

  synchronized void resumeAllThreads() {
    pauseThreads--;
    assert pauseThreads >= 0;
    if (0 == pauseThreads)
      notifyAll();
  }

  private synchronized boolean allThreadsIdle() {
    for(int i=0;i<threadStates.length;i++)
      if (!threadStates[i].isIdle)
        return false;
    return true;
  }

  /** Flush all pending docs to a new segment */
  synchronized int flush(boolean closeDocStore) throws IOException {

    assert allThreadsIdle();

    if (segment == null)
      // In case we are asked to flush an empty segment
      segment = writer.newSegmentName();

    newFiles = new ArrayList();

    docStoreOffset = numDocsInStore;

    int docCount;

    assert numDocsInRAM > 0;

    if (infoStream != null)
      message("flush postings as segment " + segment + " numDocs=" + numDocsInRAM);
    
    boolean success = false;

    try {

      if (closeDocStore) {
        assert docStoreSegment != null;
        assert docStoreSegment.equals(segment);
        newFiles.addAll(files());
        closeDocStore();
      }
    
      fieldInfos.write(directory, segment + ".fnm");

      docCount = numDocsInRAM;

      newFiles.addAll(writeSegment());

      flushedDocCount += docCount;

      success = true;

    } finally {
      if (!success)
        abort(null);
    }

    return docCount;
  }

  /** Build compound file for the segment we just flushed */
  void createCompoundFile(String segment) throws IOException
  {
    CompoundFileWriter cfsWriter = new CompoundFileWriter(directory, segment + "." + IndexFileNames.COMPOUND_FILE_EXTENSION);
    final int size = newFiles.size();
    for(int i=0;i<size;i++)
      cfsWriter.addFile((String) newFiles.get(i));
      
    // Perform the merge
    cfsWriter.close();
  }

  /** Set flushPending if it is not already set and returns
   *  whether it was set. This is used by IndexWriter to *
   *  trigger a single flush even when multiple threads are
   *  * trying to do so. */
  synchronized boolean setFlushPending() {
    if (flushPending)
      return false;
    else {
      flushPending = true;
      return true;
    }
  }

  synchronized void clearFlushPending() {
    flushPending = false;
  }

  private static final byte defaultNorm = Similarity.encodeNorm(1.0f);

  /** Write norms in the "true" segment format.  This is
   *  called only during commit, to create the .nrm file. */
  void writeNorms(String segmentName, int totalNumDoc) throws IOException {

    IndexOutput normsOut = directory.createOutput(segmentName + "." + IndexFileNames.NORMS_EXTENSION);

    try {
      normsOut.writeBytes(SegmentMerger.NORMS_HEADER, 0, SegmentMerger.NORMS_HEADER.length);

      final int numField = fieldInfos.size();

      for (int fieldIdx=0;fieldIdx<numField;fieldIdx++) {
        FieldInfo fi = fieldInfos.fieldInfo(fieldIdx);
        if (fi.isIndexed && !fi.omitNorms) {
          BufferedNorms n = norms[fieldIdx];
          final long v;
          if (n == null)
            v = 0;
          else {
            v = n.out.getFilePointer();
            n.out.writeTo(normsOut);
            n.reset();
          }
          if (v < totalNumDoc)
            fillBytes(normsOut, defaultNorm, (int) (totalNumDoc-v));
        }
      }
    } finally {
      normsOut.close();
    }
  }

  private DefaultSkipListWriter skipListWriter = null;

  private boolean currentFieldStorePayloads;

  /** Creates a segment from all Postings in the Postings
   *  hashes across all ThreadStates & FieldDatas. */
  private List writeSegment() throws IOException {

    assert allThreadsIdle();

    assert nextDocID == numDocsInRAM;

    final String segmentName;

    segmentName = segment;

    TermInfosWriter termsOut = new TermInfosWriter(directory, segmentName, fieldInfos,
                                                   writer.getTermIndexInterval());

    IndexOutput freqOut = directory.createOutput(segmentName + ".frq");
    IndexOutput proxOut = directory.createOutput(segmentName + ".prx");

    // Gather all FieldData's that have postings, across all
    // ThreadStates
    ArrayList allFields = new ArrayList();
    assert allThreadsIdle();
    for(int i=0;i<threadStates.length;i++) {
      DocumentsWriterThreadState state = threadStates[i];
      state.trimFields();
      final int numFields = state.numAllFieldData;
      for(int j=0;j<numFields;j++) {
        DocumentsWriterFieldData fp = state.allFieldDataArray[j];
        if (fp.numPostings > 0)
          allFields.add(fp);
      }
    }

    // Sort by field name
    Collections.sort(allFields);
    final int numAllFields = allFields.size();

    skipListWriter = new DefaultSkipListWriter(termsOut.skipInterval,
                                               termsOut.maxSkipLevels,
                                               numDocsInRAM, freqOut, proxOut);

    int start = 0;
    while(start < numAllFields) {

      final String fieldName = ((DocumentsWriterFieldData) allFields.get(start)).fieldInfo.name;

      int end = start+1;
      while(end < numAllFields && ((DocumentsWriterFieldData) allFields.get(end)).fieldInfo.name.equals(fieldName))
        end++;
      
      DocumentsWriterFieldData[] fields = new DocumentsWriterFieldData[end-start];
      for(int i=start;i<end;i++)
        fields[i-start] = (DocumentsWriterFieldData) allFields.get(i);

      // If this field has postings then add them to the
      // segment
      appendPostings(fields, termsOut, freqOut, proxOut);

      for(int i=0;i<fields.length;i++)
        fields[i].resetPostingArrays();

      start = end;
    }

    freqOut.close();
    proxOut.close();
    termsOut.close();
    
    // Record all files we have flushed
    List flushedFiles = new ArrayList();
    flushedFiles.add(segmentFileName(IndexFileNames.FIELD_INFOS_EXTENSION));
    flushedFiles.add(segmentFileName(IndexFileNames.FREQ_EXTENSION));
    flushedFiles.add(segmentFileName(IndexFileNames.PROX_EXTENSION));
    flushedFiles.add(segmentFileName(IndexFileNames.TERMS_EXTENSION));
    flushedFiles.add(segmentFileName(IndexFileNames.TERMS_INDEX_EXTENSION));

    if (hasNorms) {
      writeNorms(segmentName, numDocsInRAM);
      flushedFiles.add(segmentFileName(IndexFileNames.NORMS_EXTENSION));
    }

    if (infoStream != null) {
      final long newSegmentSize = segmentSize(segmentName);
      String message = "  oldRAMSize=" + numBytesUsed + " newFlushedSize=" + newSegmentSize + " docs/MB=" + nf.format(numDocsInRAM/(newSegmentSize/1024./1024.)) + " new/old=" + nf.format(100.0*newSegmentSize/numBytesUsed) + "%";
      message(message);
    }

    resetPostingsData();
    
    // Maybe downsize postingsFreeList array
    if (postingsFreeList.length > 1.5*postingsFreeCount) {
      int newSize = postingsFreeList.length;
      while(newSize > 1.25*postingsFreeCount) {
        newSize = (int) (newSize*0.8);
      }
      Posting[] newArray = new Posting[newSize];
      System.arraycopy(postingsFreeList, 0, newArray, 0, postingsFreeCount);
      postingsFreeList = newArray;
    }

    return flushedFiles;
  }

  synchronized void pushDeletes() {
    deletesFlushed.update(deletesInRAM);
  }

  /** Returns the name of the file with this extension, on
   *  the current segment we are working on. */
  private String segmentFileName(String extension) {
    return segment + "." + extension;
  }

  private static int compareText(final char[] text1, int pos1, final char[] text2, int pos2) {
    while(true) {
      final char c1 = text1[pos1++];
      final char c2 = text2[pos2++];
      if (c1 != c2) {
        if (0xffff == c2)
          return 1;
        else if (0xffff == c1)
          return -1;
        else
          return c1-c2;
      } else if (0xffff == c1)
        return 0;
    }
  }

  private final TermInfo termInfo = new TermInfo(); // minimize consing

  final UnicodeUtil.UTF8Result termsUTF8 = new UnicodeUtil.UTF8Result();

  /* Walk through all unique text tokens (Posting
   * instances) found in this field and serialize them
   * into a single RAM segment. */
  void appendPostings(DocumentsWriterFieldData[] fields,
                      TermInfosWriter termsOut,
                      IndexOutput freqOut,
                      IndexOutput proxOut)
    throws CorruptIndexException, IOException {

    final int fieldNumber = fields[0].fieldInfo.number;
    int numFields = fields.length;

    final DocumentsWriterFieldMergeState[] mergeStates = new DocumentsWriterFieldMergeState[numFields];

    for(int i=0;i<numFields;i++) {
      DocumentsWriterFieldMergeState fms = mergeStates[i] = new DocumentsWriterFieldMergeState();
      fms.field = fields[i];
      fms.postings = fms.field.sortPostings();

      assert fms.field.fieldInfo == fields[0].fieldInfo;

      // Should always be true
      boolean result = fms.nextTerm();
      assert result;
    }

    final int skipInterval = termsOut.skipInterval;
    currentFieldStorePayloads = fields[0].fieldInfo.storePayloads;

    DocumentsWriterFieldMergeState[] termStates = new DocumentsWriterFieldMergeState[numFields];

    while(numFields > 0) {

      // Get the next term to merge
      termStates[0] = mergeStates[0];
      int numToMerge = 1;

      for(int i=1;i<numFields;i++) {
        final char[] text = mergeStates[i].text;
        final int textOffset = mergeStates[i].textOffset;
        final int cmp = compareText(text, textOffset, termStates[0].text, termStates[0].textOffset);

        if (cmp < 0) {
          termStates[0] = mergeStates[i];
          numToMerge = 1;
        } else if (cmp == 0)
          termStates[numToMerge++] = mergeStates[i];
      }

      int df = 0;
      int lastPayloadLength = -1;

      int lastDoc = 0;

      final char[] text = termStates[0].text;
      final int start = termStates[0].textOffset;

      long freqPointer = freqOut.getFilePointer();
      long proxPointer = proxOut.getFilePointer();

      skipListWriter.resetSkip();

      // Now termStates has numToMerge FieldMergeStates
      // which all share the same term.  Now we must
      // interleave the docID streams.
      while(numToMerge > 0) {
        
        if ((++df % skipInterval) == 0) {
          skipListWriter.setSkipData(lastDoc, currentFieldStorePayloads, lastPayloadLength);
          skipListWriter.bufferSkip(df);
        }

        DocumentsWriterFieldMergeState minState = termStates[0];
        for(int i=1;i<numToMerge;i++)
          if (termStates[i].docID < minState.docID)
            minState = termStates[i];

        final int doc = minState.docID;
        final int termDocFreq = minState.termFreq;

        assert doc < numDocsInRAM;
        assert doc > lastDoc || df == 1;

        final int newDocCode = (doc-lastDoc)<<1;
        lastDoc = doc;

        final ByteSliceReader prox = minState.prox;

        // Carefully copy over the prox + payload info,
        // changing the format to match Lucene's segment
        // format.
        for(int j=0;j<termDocFreq;j++) {
          final int code = prox.readVInt();
          if (currentFieldStorePayloads) {
            final int payloadLength;
            if ((code & 1) != 0) {
              // This position has a payload
              payloadLength = prox.readVInt();
            } else
              payloadLength = 0;
            if (payloadLength != lastPayloadLength) {
              proxOut.writeVInt(code|1);
              proxOut.writeVInt(payloadLength);
              lastPayloadLength = payloadLength;
            } else
              proxOut.writeVInt(code & (~1));
            if (payloadLength > 0)
              copyBytes(prox, proxOut, payloadLength);
          } else {
            assert 0 == (code & 1);
            proxOut.writeVInt(code>>1);
          }
        }

        if (1 == termDocFreq) {
          freqOut.writeVInt(newDocCode|1);
        } else {
          freqOut.writeVInt(newDocCode);
          freqOut.writeVInt(termDocFreq);
        }

        if (!minState.nextDoc()) {

          // Remove from termStates
          int upto = 0;
          for(int i=0;i<numToMerge;i++)
            if (termStates[i] != minState)
              termStates[upto++] = termStates[i];
          numToMerge--;
          assert upto == numToMerge;

          // Advance this state to the next term

          if (!minState.nextTerm()) {
            // OK, no more terms, so remove from mergeStates
            // as well
            upto = 0;
            for(int i=0;i<numFields;i++)
              if (mergeStates[i] != minState)
                mergeStates[upto++] = mergeStates[i];
            numFields--;
            assert upto == numFields;
          }
        }
      }

      assert df > 0;

      // Done merging this term

      long skipPointer = skipListWriter.writeSkip(freqOut);

      // Write term
      termInfo.set(df, freqPointer, proxPointer, (int) (skipPointer - freqPointer));

      // TODO: we could do this incrementally
      UnicodeUtil.UTF16toUTF8(text, start, termsUTF8);

      // TODO: we could save O(n) re-scan of the term by
      // computing the shared prefix with the last term
      // while during the UTF8 encoding
      termsOut.add(fieldNumber,
                   termsUTF8.result,
                   termsUTF8.length,
                   termInfo);
    }
  }

  synchronized void close() {
    closed = true;
    notifyAll();
  }

  /** Returns a free (idle) ThreadState that may be used for
   * indexing this one document.  This call also pauses if a
   * flush is pending.  If delTerm is non-null then we
   * buffer this deleted term after the thread state has
   * been acquired. */
  synchronized DocumentsWriterThreadState getThreadState(Document doc, Term delTerm) throws IOException {

    // First, find a thread state.  If this thread already
    // has affinity to a specific ThreadState, use that one
    // again.
    DocumentsWriterThreadState state = (DocumentsWriterThreadState) threadBindings.get(Thread.currentThread());
    if (state == null) {
      // First time this thread has called us since last flush
      DocumentsWriterThreadState minThreadState = null;
      for(int i=0;i<threadStates.length;i++) {
        DocumentsWriterThreadState ts = threadStates[i];
        if (minThreadState == null || ts.numThreads < minThreadState.numThreads)
          minThreadState = ts;
      }
      if (minThreadState != null && (minThreadState.numThreads == 0 || threadStates.length == MAX_THREAD_STATE)) {
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
      threadBindings.put(Thread.currentThread(), state);
    }

    // Next, wait until my thread state is idle (in case
    // it's shared with other threads) and for threads to
    // not be paused nor a flush pending:
    waitReady(state);

    if (segment == null)
      segment = writer.newSegmentName();

    state.isIdle = false;

    try {
      boolean success = false;
      try {
        state.init(doc, nextDocID);
        if (delTerm != null) {
          addDeleteTerm(delTerm, state.docID);
          state.doFlushAfter = timeToFlushDeletes();
        }
        // Only increment nextDocID & numDocsInRAM on successful init
        nextDocID++;
        numDocsInRAM++;

        // We must at this point commit to flushing to ensure we
        // always get N docs when we flush by doc count, even if
        // > 1 thread is adding documents:
        if (!flushPending && maxBufferedDocs != IndexWriter.DISABLE_AUTO_FLUSH
            && numDocsInRAM >= maxBufferedDocs) {
          flushPending = true;
          state.doFlushAfter = true;
        }

        success = true;
      } finally {
        if (!success) {
          // Forcefully idle this ThreadState:
          state.isIdle = true;
          notifyAll();
          if (state.doFlushAfter) {
            state.doFlushAfter = false;
            flushPending = false;
          }
        }
      }
    } catch (AbortException ae) {
      abort(ae);
    }

    return state;
  }

  /** Returns true if the caller (IndexWriter) should now
   * flush. */
  boolean addDocument(Document doc, Analyzer analyzer)
    throws CorruptIndexException, IOException {
    return updateDocument(doc, analyzer, null);
  }

  boolean updateDocument(Term t, Document doc, Analyzer analyzer)
    throws CorruptIndexException, IOException {
    return updateDocument(doc, analyzer, t);
  }

  boolean updateDocument(Document doc, Analyzer analyzer, Term delTerm)
    throws CorruptIndexException, IOException {

    // This call is synchronized but fast
    final DocumentsWriterThreadState state = getThreadState(doc, delTerm);
    try {
      boolean success = false;
      try {
        try {
          // This call is not synchronized and does all the work
          state.processDocument(analyzer);
        } finally {
          // Note that we must call finishDocument even on
          // exception, because for a non-aborting
          // exception, a portion of the document has been
          // indexed (and its ID is marked for deletion), so
          // all index files must be updated to record this
          // document.  This call is synchronized but fast.
          finishDocument(state);
        }
        success = true;
      } finally {
        if (!success) {
          synchronized(this) {

            // If this thread state had decided to flush, we
            // must clear it so another thread can flush
            if (state.doFlushAfter) {
              state.doFlushAfter = false;
              flushPending = false;
              notifyAll();
            }

            // Immediately mark this document as deleted
            // since likely it was partially added.  This
            // keeps indexing as "all or none" (atomic) when
            // adding a document:
            addDeleteDocID(state.docID);
          }
        }
      }
    } catch (AbortException ae) {
      abort(ae);
    }

    return state.doFlushAfter || timeToFlushDeletes();
  }

  // for testing
  synchronized int getNumBufferedDeleteTerms() {
    return deletesInRAM.numTerms;
  }

  // for testing
  synchronized HashMap getBufferedDeleteTerms() {
    return deletesInRAM.terms;
  }

  /** Called whenever a merge has completed and the merged segments had deletions */
  synchronized void remapDeletes(SegmentInfos infos, int[][] docMaps, int[] delCounts, MergePolicy.OneMerge merge, int mergeDocCount) {
    if (docMaps == null)
      // The merged segments had no deletes so docIDs did not change and we have nothing to do
      return;
    MergeDocIDRemapper mapper = new MergeDocIDRemapper(infos, docMaps, delCounts, merge, mergeDocCount);
    deletesInRAM.remap(mapper, infos, docMaps, delCounts, merge, mergeDocCount);
    deletesFlushed.remap(mapper, infos, docMaps, delCounts, merge, mergeDocCount);
    flushedDocCount -= mapper.docShift;
  }

  synchronized private void waitReady(DocumentsWriterThreadState state) {
    while(!closed && ((state != null && !state.isIdle) || pauseThreads != 0 || flushPending || abortCount > 0))
      try {
        wait();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }

    if (closed)
      throw new AlreadyClosedException("this IndexWriter is closed");
  }

  synchronized boolean bufferDeleteTerms(Term[] terms) throws IOException {
    waitReady(null);
    for (int i = 0; i < terms.length; i++)
      addDeleteTerm(terms[i], numDocsInRAM);
    return timeToFlushDeletes();
  }

  synchronized boolean bufferDeleteTerm(Term term) throws IOException {
    waitReady(null);
    addDeleteTerm(term, numDocsInRAM);
    return timeToFlushDeletes();
  }

  synchronized boolean bufferDeleteQueries(Query[] queries) throws IOException {
    waitReady(null);
    for (int i = 0; i < queries.length; i++)
      addDeleteQuery(queries[i], numDocsInRAM);
    return timeToFlushDeletes();
  }

  synchronized boolean bufferDeleteQuery(Query query) throws IOException {
    waitReady(null);
    addDeleteQuery(query, numDocsInRAM);
    return timeToFlushDeletes();
  }

  synchronized boolean deletesFull() {
    return maxBufferedDeleteTerms != IndexWriter.DISABLE_AUTO_FLUSH
      && ((deletesInRAM.numTerms + deletesInRAM.queries.size() + deletesInRAM.docIDs.size()) >= maxBufferedDeleteTerms);
  }

  synchronized private boolean timeToFlushDeletes() {
    return (bufferIsFull || deletesFull()) && setFlushPending();
  }

  void setMaxBufferedDeleteTerms(int maxBufferedDeleteTerms) {
    this.maxBufferedDeleteTerms = maxBufferedDeleteTerms;
  }

  int getMaxBufferedDeleteTerms() {
    return maxBufferedDeleteTerms;
  }

  synchronized boolean hasDeletes() {
    return deletesFlushed.any();
  }

  synchronized boolean applyDeletes(SegmentInfos infos) throws IOException {

    if (!hasDeletes())
      return false;

    if (infoStream != null)
      message("apply " + deletesFlushed.numTerms + " buffered deleted terms and " +
              deletesFlushed.docIDs.size() + " deleted docIDs and " +
              deletesFlushed.queries.size() + " deleted queries on " +
              + infos.size() + " segments.");

    final int infosEnd = infos.size();

    int docStart = 0;
    boolean any = false;
    for (int i = 0; i < infosEnd; i++) {
      IndexReader reader = SegmentReader.get(infos.info(i), false);
      boolean success = false;
      try {
        any |= applyDeletes(reader, docStart);
        docStart += reader.maxDoc();
        success = true;
      } finally {
        if (reader != null) {
          try {
            if (success)
              reader.doCommit();
          } finally {
            reader.doClose();
          }
        }
      }
    }

    deletesFlushed.clear();

    return any;
  }

  // Apply buffered delete terms, queries and docIDs to the
  // provided reader
  private final synchronized boolean applyDeletes(IndexReader reader, int docIDStart)
    throws CorruptIndexException, IOException {

    final int docEnd = docIDStart + reader.maxDoc();
    boolean any = false;

    // Delete by term
    Iterator iter = deletesFlushed.terms.entrySet().iterator();
    while (iter.hasNext()) {
      Entry entry = (Entry) iter.next();
      Term term = (Term) entry.getKey();

      TermDocs docs = reader.termDocs(term);
      if (docs != null) {
        int limit = ((BufferedDeletes.Num) entry.getValue()).getNum();
        try {
          while (docs.next()) {
            int docID = docs.doc();
            if (docIDStart+docID >= limit)
              break;
            reader.deleteDocument(docID);
            any = true;
          }
        } finally {
          docs.close();
        }
      }
    }

    // Delete by docID
    iter = deletesFlushed.docIDs.iterator();
    while(iter.hasNext()) {
      int docID = ((Integer) iter.next()).intValue();
      if (docID >= docIDStart && docID < docEnd) {
        reader.deleteDocument(docID-docIDStart);
        any = true;
      }
    }

    // Delete by query
    IndexSearcher searcher = new IndexSearcher(reader);
    iter = deletesFlushed.queries.entrySet().iterator();
    while(iter.hasNext()) {
      Entry entry = (Entry) iter.next();
      Query query = (Query) entry.getKey();
      int limit = ((Integer) entry.getValue()).intValue();
      Weight weight = query.weight(searcher);
      Scorer scorer = weight.scorer(reader);
      while(scorer.next()) {
        final int docID = scorer.doc();
        if (docIDStart + docID >= limit)
          break;
        reader.deleteDocument(docID);
        any = true;
      }
    }
    searcher.close();
    return any;
  }

  // Buffer a term in bufferedDeleteTerms, which records the
  // current number of documents buffered in ram so that the
  // delete term will be applied to those documents as well
  // as the disk segments.
  synchronized private void addDeleteTerm(Term term, int docCount) {
    BufferedDeletes.Num num = (BufferedDeletes.Num) deletesInRAM.terms.get(term);
    final int docIDUpto = flushedDocCount + docCount;
    if (num == null)
      deletesInRAM.terms.put(term, new BufferedDeletes.Num(docIDUpto));
    else
      num.setNum(docIDUpto);
    deletesInRAM.numTerms++;
  }

  // Buffer a specific docID for deletion.  Currently only
  // used when we hit a exception when adding a document
  synchronized private void addDeleteDocID(int docID) {
    deletesInRAM.docIDs.add(new Integer(flushedDocCount+docID));
  }

  synchronized private void addDeleteQuery(Query query, int docID) {
    deletesInRAM.queries.put(query, new Integer(flushedDocCount + docID));
  }

  /** Does the synchronized work to finish/flush the
   * inverted document. */
  private synchronized void finishDocument(DocumentsWriterThreadState state) throws IOException, AbortException {
    if (abortCount > 0) {
      // Forcefully idle this threadstate -- its state will
      // be reset by abort()
      state.isIdle = true;
      notifyAll();
      return;
    }

    if (ramBufferSize != IndexWriter.DISABLE_AUTO_FLUSH
        && numBytesUsed >= ramBufferSize)
      balanceRAM();

    // Now write the indexed document to the real files.
    if (nextWriteDocID == state.docID) {
      // It's my turn, so write everything now:
      nextWriteDocID++;
      state.writeDocument();
      state.isIdle = true;
      notifyAll();

      // If any states were waiting on me, sweep through and
      // flush those that are enabled by my write.
      if (numWaiting > 0) {
        boolean any = true;
        while(any) {
          any = false;
          for(int i=0;i<numWaiting;) {
            final DocumentsWriterThreadState s = waitingThreadStates[i];
            if (s.docID == nextWriteDocID) {
              s.writeDocument();
              s.isIdle = true;
              nextWriteDocID++;
              any = true;
              if (numWaiting > i+1)
                // Swap in the last waiting state to fill in
                // the hole we just created.  It's important
                // to do this as-we-go and not at the end of
                // the loop, because if we hit an aborting
                // exception in one of the s.writeDocument
                // calls (above), it leaves this array in an
                // inconsistent state:
                waitingThreadStates[i] = waitingThreadStates[numWaiting-1];
              numWaiting--;
            } else {
              assert !s.isIdle;
              i++;
            }
          }
        }
      }
    } else {
      // Another thread got a docID before me, but, it
      // hasn't finished its processing.  So add myself to
      // the line but don't hold up this thread.
      waitingThreadStates[numWaiting++] = state;
    }
  }

  long getRAMUsed() {
    return numBytesUsed;
  }

  long numBytesAlloc;
  long numBytesUsed;

  NumberFormat nf = NumberFormat.getInstance();

  /* Used only when writing norms to fill in default norm
   * value into the holes in docID stream for those docs
   * that didn't have this field. */
  static void fillBytes(IndexOutput out, byte b, int numBytes) throws IOException {
    for(int i=0;i<numBytes;i++)
      out.writeByte(b);
  }

  final byte[] copyByteBuffer = new byte[4096];

  /** Copy numBytes from srcIn to destIn */
  void copyBytes(IndexInput srcIn, IndexOutput destIn, long numBytes) throws IOException {
    // TODO: we could do this more efficiently (save a copy)
    // because it's always from a ByteSliceReader ->
    // IndexOutput
    while(numBytes > 0) {
      final int chunk;
      if (numBytes > 4096)
        chunk = 4096;
      else
        chunk = (int) numBytes;
      srcIn.readBytes(copyByteBuffer, 0, chunk);
      destIn.writeBytes(copyByteBuffer, 0, chunk);
      numBytes -= chunk;
    }
  }

  // Used only when infoStream != null
  private long segmentSize(String segmentName) throws IOException {
    assert infoStream != null;
    
    long size = directory.fileLength(segmentName + ".tii") +
      directory.fileLength(segmentName + ".tis") +
      directory.fileLength(segmentName + ".frq") +
      directory.fileLength(segmentName + ".prx");

    final String normFileName = segmentName + ".nrm";
    if (directory.fileExists(normFileName))
      size += directory.fileLength(normFileName);

    return size;
  }

  final private static int POINTER_NUM_BYTE = 4;
  final private static int INT_NUM_BYTE = 4;
  final private static int CHAR_NUM_BYTE = 2;

  // Why + 5*POINTER_NUM_BYTE below?
  //   1: Posting has "vector" field which is a pointer
  //   2: Posting is referenced by postingsFreeList array
  //   3,4,5: Posting is referenced by postings hash, which
  //          targets 25-50% fill factor; approximate this
  //          as 3X # pointers
  final static int POSTING_NUM_BYTE = OBJECT_HEADER_BYTES + 9*INT_NUM_BYTE + 5*POINTER_NUM_BYTE;

  // Holds free pool of Posting instances
  private Posting[] postingsFreeList;
  private int postingsFreeCount;
  private int postingsAllocCount;

  /* Allocate more Postings from shared pool */
  synchronized void getPostings(Posting[] postings) {
    numBytesUsed += postings.length * POSTING_NUM_BYTE;
    final int numToCopy;
    if (postingsFreeCount < postings.length)
      numToCopy = postingsFreeCount;
    else
      numToCopy = postings.length;
    final int start = postingsFreeCount-numToCopy;
    System.arraycopy(postingsFreeList, start,
                     postings, 0, numToCopy);
    postingsFreeCount -= numToCopy;

    // Directly allocate the remainder if any
    if (numToCopy < postings.length) {
      final int extra = postings.length - numToCopy;
      final int newPostingsAllocCount = postingsAllocCount + extra;
      if (newPostingsAllocCount > postingsFreeList.length)
        postingsFreeList = new Posting[(int) (1.25 * newPostingsAllocCount)];

      balanceRAM();
      for(int i=numToCopy;i<postings.length;i++) {
        postings[i] = new Posting();
        numBytesAlloc += POSTING_NUM_BYTE;
        postingsAllocCount++;
      }
    }
    assert numBytesUsed <= numBytesAlloc;
  }

  synchronized void recyclePostings(Posting[] postings, int numPostings) {
    // Move all Postings from this ThreadState back to our
    // free list.  We pre-allocated this array while we were
    // creating Postings to make sure it's large enough
    assert postingsFreeCount + numPostings <= postingsFreeList.length;
    System.arraycopy(postings, 0, postingsFreeList, postingsFreeCount, numPostings);
    postingsFreeCount += numPostings;
  }

  /* Initial chunks size of the shared byte[] blocks used to
     store postings data */
  final static int BYTE_BLOCK_SHIFT = 15;
  final static int BYTE_BLOCK_SIZE = (int) Math.pow(2.0, BYTE_BLOCK_SHIFT);
  final static int BYTE_BLOCK_MASK = BYTE_BLOCK_SIZE - 1;
  final static int BYTE_BLOCK_NOT_MASK = ~BYTE_BLOCK_MASK;

  private ArrayList freeByteBlocks = new ArrayList();

  /* Allocate another byte[] from the shared pool */
  synchronized byte[] getByteBlock(boolean trackAllocations) {
    final int size = freeByteBlocks.size();
    final byte[] b;
    if (0 == size) {
      numBytesAlloc += BYTE_BLOCK_SIZE;
      balanceRAM();
      b = new byte[BYTE_BLOCK_SIZE];
    } else
      b = (byte[]) freeByteBlocks.remove(size-1);
    if (trackAllocations)
      numBytesUsed += BYTE_BLOCK_SIZE;
    assert numBytesUsed <= numBytesAlloc;
    return b;
  }

  /* Return a byte[] to the pool */
  synchronized void recycleByteBlocks(byte[][] blocks, int start, int end) {
    for(int i=start;i<end;i++)
      freeByteBlocks.add(blocks[i]);
  }

  /* Initial chunk size of the shared char[] blocks used to
     store term text */
  final static int CHAR_BLOCK_SHIFT = 14;
  final static int CHAR_BLOCK_SIZE = (int) Math.pow(2.0, CHAR_BLOCK_SHIFT);
  final static int CHAR_BLOCK_MASK = CHAR_BLOCK_SIZE - 1;

  final static int MAX_TERM_LENGTH = CHAR_BLOCK_SIZE-1;

  private ArrayList freeCharBlocks = new ArrayList();

  /* Allocate another char[] from the shared pool */
  synchronized char[] getCharBlock() {
    final int size = freeCharBlocks.size();
    final char[] c;
    if (0 == size) {
      numBytesAlloc += CHAR_BLOCK_SIZE * CHAR_NUM_BYTE;
      balanceRAM();
      c = new char[CHAR_BLOCK_SIZE];
    } else
      c = (char[]) freeCharBlocks.remove(size-1);
    numBytesUsed += CHAR_BLOCK_SIZE * CHAR_NUM_BYTE;
    assert numBytesUsed <= numBytesAlloc;
    return c;
  }

  /* Return a char[] to the pool */
  synchronized void recycleCharBlocks(char[][] blocks, int numBlocks) {
    for(int i=0;i<numBlocks;i++)
      freeCharBlocks.add(blocks[i]);
  }

  String toMB(long v) {
    return nf.format(v/1024./1024.);
  }

  /* We have three pools of RAM: Postings, byte blocks
   * (holds freq/prox posting data) and char blocks (holds
   * characters in the term).  Different docs require
   * varying amount of storage from these three classes.
   * For example, docs with many unique single-occurrence
   * short terms will use up the Postings RAM and hardly any
   * of the other two.  Whereas docs with very large terms
   * will use alot of char blocks RAM and relatively less of
   * the other two.  This method just frees allocations from
   * the pools once we are over-budget, which balances the
   * pools to match the current docs. */
  synchronized void balanceRAM() {

    if (ramBufferSize == IndexWriter.DISABLE_AUTO_FLUSH || bufferIsFull)
      return;

    // We free our allocations if we've allocated 5% over
    // our allowed RAM buffer
    final long freeTrigger = (long) (1.05 * ramBufferSize);
    final long freeLevel = (long) (0.95 * ramBufferSize);
    
    // We flush when we've used our target usage
    final long flushTrigger = (long) ramBufferSize;

    if (numBytesAlloc > freeTrigger) {
      if (infoStream != null)
        message("  RAM: now balance allocations: usedMB=" + toMB(numBytesUsed) +
                " vs trigger=" + toMB(flushTrigger) +
                " allocMB=" + toMB(numBytesAlloc) +
                " vs trigger=" + toMB(freeTrigger) +
                " postingsFree=" + toMB(postingsFreeCount*POSTING_NUM_BYTE) +
                " byteBlockFree=" + toMB(freeByteBlocks.size()*BYTE_BLOCK_SIZE) +
                " charBlockFree=" + toMB(freeCharBlocks.size()*CHAR_BLOCK_SIZE*CHAR_NUM_BYTE));

      // When we've crossed 100% of our target Postings
      // RAM usage, try to free up until we're back down
      // to 95%
      final long startBytesAlloc = numBytesAlloc;

      final int postingsFreeChunk = (int) (BYTE_BLOCK_SIZE / POSTING_NUM_BYTE);

      int iter = 0;

      // We free equally from each pool in 64 KB
      // chunks until we are below our threshold
      // (freeLevel)

      while(numBytesAlloc > freeLevel) {
        if (0 == freeByteBlocks.size() && 0 == freeCharBlocks.size() && 0 == postingsFreeCount) {
          // Nothing else to free -- must flush now.
          bufferIsFull = true;
          if (infoStream != null)
            message("    nothing to free; now set bufferIsFull");
          break;
        }

        if ((0 == iter % 3) && freeByteBlocks.size() > 0) {
          freeByteBlocks.remove(freeByteBlocks.size()-1);
          numBytesAlloc -= BYTE_BLOCK_SIZE;
        }

        if ((1 == iter % 3) && freeCharBlocks.size() > 0) {
          freeCharBlocks.remove(freeCharBlocks.size()-1);
          numBytesAlloc -= CHAR_BLOCK_SIZE * CHAR_NUM_BYTE;
        }

        if ((2 == iter % 3) && postingsFreeCount > 0) {
          final int numToFree;
          if (postingsFreeCount >= postingsFreeChunk)
            numToFree = postingsFreeChunk;
          else
            numToFree = postingsFreeCount;
          Arrays.fill(postingsFreeList, postingsFreeCount-numToFree, postingsFreeCount, null);
          postingsFreeCount -= numToFree;
          postingsAllocCount -= numToFree;
          numBytesAlloc -= numToFree * POSTING_NUM_BYTE;
        }

        iter++;
      }
      
      if (infoStream != null)
        message("    after free: freedMB=" + nf.format((startBytesAlloc-numBytesAlloc)/1024./1024.) + " usedMB=" + nf.format(numBytesUsed/1024./1024.) + " allocMB=" + nf.format(numBytesAlloc/1024./1024.));
      
    } else {
      // If we have not crossed the 100% mark, but have
      // crossed the 95% mark of RAM we are actually
      // using, go ahead and flush.  This prevents
      // over-allocating and then freeing, with every
      // flush.
      if (numBytesUsed > flushTrigger) {
        if (infoStream != null)
          message("  RAM: now flush @ usedMB=" + nf.format(numBytesUsed/1024./1024.) +
                  " allocMB=" + nf.format(numBytesAlloc/1024./1024.) +
                  " triggerMB=" + nf.format(flushTrigger/1024./1024.));

        bufferIsFull = true;
      }
    }
  }
}
