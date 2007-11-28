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
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RAMOutputStream;

import java.io.IOException;
import java.io.PrintStream;
import java.io.Reader;
import java.util.Arrays;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
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
 */

final class DocumentsWriter {

  private IndexWriter writer;
  private Directory directory;

  private FieldInfos fieldInfos = new FieldInfos(); // All fields we've seen
  private IndexOutput tvx, tvf, tvd;              // To write term vectors
  private FieldsWriter fieldsWriter;              // To write stored fields

  private String segment;                         // Current segment we are working on
  private String docStoreSegment;                 // Current doc-store segment we are writing
  private int docStoreOffset;                     // Current starting doc-store offset of current segment

  private int nextDocID;                          // Next docID to be added
  private int numDocsInRAM;                       // # docs buffered in RAM
  private int numDocsInStore;                     // # docs written to doc stores
  private int nextWriteDocID;                     // Next docID to be written

  // Max # ThreadState instances; if there are more threads
  // than this they share ThreadStates
  private final static int MAX_THREAD_STATE = 5;
  private ThreadState[] threadStates = new ThreadState[0];
  private final HashMap threadBindings = new HashMap();
  private int numWaiting;
  private ThreadState[] waitingThreadStates = new ThreadState[1];
  private int pauseThreads;                       // Non-zero when we need all threads to
                                                  // pause (eg to flush)
  private boolean flushPending;                   // True when a thread has decided to flush
  private boolean bufferIsFull;                 // True when it's time to write segment

  private PrintStream infoStream;

  // This Hashmap buffers delete terms in ram before they
  // are applied.  The key is delete term; the value is
  // number of buffered documents the term applies to.
  private HashMap bufferedDeleteTerms = new HashMap();
  private int numBufferedDeleteTerms = 0;

  // The max number of delete terms that can be buffered before
  // they must be flushed to disk.
  private int maxBufferedDeleteTerms = IndexWriter.DEFAULT_MAX_BUFFERED_DELETE_TERMS;

  // How much RAM we can use before flushing.  This is 0 if
  // we are flushing by doc count instead.
  private long ramBufferSize = (long) (IndexWriter.DEFAULT_RAM_BUFFER_SIZE_MB*1024*1024);

  // Flush @ this number of docs.  If rarmBufferSize is
  // non-zero we will flush by RAM usage instead.
  private int maxBufferedDocs = IndexWriter.DEFAULT_MAX_BUFFERED_DOCS;

  // Coarse estimates used to measure RAM usage of buffered deletes
  private static int OBJECT_HEADER_BYTES = 12;
  private static int OBJECT_POINTER_BYTES = 4;    // TODO: should be 8 on 64-bit platform
  private static int BYTES_PER_CHAR = 2;

  private BufferedNorms[] norms = new BufferedNorms[0];   // Holds norms until we flush

  DocumentsWriter(Directory directory, IndexWriter writer) throws IOException {
    this.directory = directory;
    this.writer = writer;

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
      infoStream.println("\ncloseDocStore: " + flushedFiles.size() + " files to flush to segment " + docStoreSegment);

    if (flushedFiles.size() > 0) {
      files = null;

      if (tvx != null) {
        // At least one doc in this run had term vectors enabled
        assert docStoreSegment != null;
        tvx.close();
        tvf.close();
        tvd.close();
        tvx = null;
      }

      if (fieldsWriter != null) {
        assert docStoreSegment != null;
        fieldsWriter.close();
        fieldsWriter = null;
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

  private List files = null;                      // Cached list of files we've created
  private List abortedFiles = null;               // List of files that were written before last abort()

  List abortedFiles() {
    return abortedFiles;
  }

  /* Returns list of files in use by this instance,
   * including any flushed segments. */
  List files() {

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

  /** Called if we hit an exception when adding docs,
   *  flushing, etc.  This resets our state, discarding any
   *  docs added since last flush. */
  synchronized void abort() throws IOException {

    if (infoStream != null)
      infoStream.println("docWriter: now abort");

    // Forcefully remove waiting ThreadStates from line
    for(int i=0;i<numWaiting;i++)
      waitingThreadStates[i].isIdle = true;
    numWaiting = 0;

    pauseAllThreads();

    bufferedDeleteTerms.clear();
    numBufferedDeleteTerms = 0;

    try {

      abortedFiles = files();

      // Discard pending norms:
      final int numField = fieldInfos.size();
      for (int i=0;i<numField;i++) {
        FieldInfo fi = fieldInfos.fieldInfo(i);
        if (fi.isIndexed && !fi.omitNorms) {
          BufferedNorms n = norms[i];
          if (n != null) {
            n.out.reset();
            n.reset();
          }
        }
      }

      // Reset vectors writer
      if (tvx != null) {
        tvx.close();
        tvf.close();
        tvd.close();
        tvx = null;
      }

      // Reset fields writer
      if (fieldsWriter != null) {
        fieldsWriter.close();
        fieldsWriter = null;
      }

      // Reset all postings data
      resetPostingsData();

      // Clear vectors & fields from ThreadStates
      for(int i=0;i<threadStates.length;i++) {
        ThreadState state = threadStates[i];
        if (state.localFieldsWriter != null) {
          state.localFieldsWriter.close();
          state.localFieldsWriter = null;
        }
        state.tvfLocal.reset();
        state.fdtLocal.reset();
      }

      files = null;

    } finally {
      resumeAllThreads();
    }
  }

  /** Reset after a flush */
  private void resetPostingsData() throws IOException {
    // All ThreadStates should be idle when we are called
    assert allThreadsIdle();
    for(int i=0;i<threadStates.length;i++) {
      threadStates[i].resetPostings();
      threadStates[i].numThreads = 0;
    }
    threadBindings.clear();
    numBytesUsed = 0;
    balanceRAM();
    bufferIsFull = false;
    flushPending = false;
    segment = null;
    numDocsInRAM = 0;
    nextDocID = 0;
    nextWriteDocID = 0;
    files = null;
  }

  synchronized void pauseAllThreads() {
    pauseThreads++;
    if (1 == pauseThreads) {
      while(!allThreadsIdle()) {
        try {
          wait();
        } catch (InterruptedException e) {
        }
      }
    }
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

  private boolean hasNorms;                       // Whether any norms were seen since last flush

  List newFiles;

  /** Flush all pending docs to a new segment */
  int flush(boolean closeDocStore) throws IOException {

    assert allThreadsIdle();

    if (segment == null)
      // In case we are asked to flush an empty segment
      segment = writer.newSegmentName();

    newFiles = new ArrayList();

    docStoreOffset = numDocsInStore;

    if (closeDocStore) {
      assert docStoreSegment != null;
      assert docStoreSegment.equals(segment);
      newFiles.addAll(files());
      closeDocStore();
    }
    
    int docCount;

    assert numDocsInRAM > 0;

    if (infoStream != null)
      infoStream.println("\nflush postings as segment " + segment + " numDocs=" + numDocsInRAM);
    
    boolean success = false;

    try {

      fieldInfos.write(directory, segment + ".fnm");

      docCount = numDocsInRAM;

      newFiles.addAll(writeSegment());

      success = true;

    } finally {
      if (!success)
        abort();
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

  /** Per-thread state.  We keep a separate Posting hash and
   *  other state for each thread and then merge postings *
   *  hashes from all threads when writing the segment. */
  private final class ThreadState {

    Posting[] postingsFreeList;           // Free Posting instances
    int postingsFreeCount;

    RAMOutputStream tvfLocal = new RAMOutputStream();    // Term vectors for one doc
    RAMOutputStream fdtLocal = new RAMOutputStream();    // Stored fields for one doc
    FieldsWriter localFieldsWriter;       // Fields for one doc

    long[] vectorFieldPointers;
    int[] vectorFieldNumbers;

    boolean isIdle = true;                // Whether we are in use
    int numThreads = 1;                   // Number of threads that use this instance

    int docID;                            // docID we are now working on
    int numStoredFields;                  // How many stored fields in current doc
    float docBoost;                       // Boost for current doc

    FieldData[] fieldDataArray;           // Fields touched by current doc
    int numFieldData;                     // How many fields in current doc
    int numVectorFields;                  // How many vector fields in current doc

    FieldData[] allFieldDataArray = new FieldData[10]; // All FieldData instances
    int numAllFieldData;
    FieldData[] fieldDataHash;            // Hash FieldData instances by field name
    int fieldDataHashMask;

    boolean doFlushAfter;

    public ThreadState() {
      fieldDataArray = new FieldData[8];

      fieldDataHash = new FieldData[16];
      fieldDataHashMask = 15;

      vectorFieldPointers = new long[10];
      vectorFieldNumbers = new int[10];
      postingsFreeList = new Posting[256];
      postingsFreeCount = 0;
    }

    /** Clear the postings hash and return objects back to
     *  shared pool */
    public void resetPostings() throws IOException {
      if (localFieldsWriter != null) {
        localFieldsWriter.close();
        localFieldsWriter = null;
      }
      maxPostingsVectors = 0;
      doFlushAfter = false;
      postingsPool.reset();
      charPool.reset();
      recyclePostings(postingsFreeList, postingsFreeCount);
      postingsFreeCount = 0;
      for(int i=0;i<numAllFieldData;i++) {
        FieldData fp = allFieldDataArray[i];
        if (fp.numPostings > 0)
          fp.resetPostingArrays();
      }
    }

    /** Move all per-document state that was accumulated in
     *  the ThreadState into the "real" stores. */
    public void writeDocument() throws IOException {

      // Append stored fields to the real FieldsWriter:
      fieldsWriter.flushDocument(fdtLocal);
      fdtLocal.reset();

      // Append term vectors to the real outputs:
      if (tvx != null) {
        tvx.writeLong(tvd.getFilePointer());
        tvd.writeVInt(numVectorFields);
        if (numVectorFields > 0) {
          for(int i=0;i<numVectorFields;i++)
            tvd.writeVInt(vectorFieldNumbers[i]);
          assert 0 == vectorFieldPointers[0];
          tvd.writeVLong(tvf.getFilePointer());
          long lastPos = vectorFieldPointers[0];
          for(int i=1;i<numVectorFields;i++) {
            long pos = vectorFieldPointers[i];
            tvd.writeVLong(pos-lastPos);
            lastPos = pos;
          }
          tvfLocal.writeTo(tvf);
          tvfLocal.reset();
        }
      }

      // Append norms for the fields we saw:
      for(int i=0;i<numFieldData;i++) {
        FieldData fp = fieldDataArray[i];
        if (fp.doNorms) {
          BufferedNorms bn = norms[fp.fieldInfo.number];
          assert bn != null;
          assert bn.upto <= docID;
          bn.fill(docID);
          float norm = fp.boost * writer.getSimilarity().lengthNorm(fp.fieldInfo.name, fp.length);
          bn.add(norm);
        }
      }

      if (bufferIsFull && !flushPending) {
        flushPending = true;
        doFlushAfter = true;
      }
    }

    /** Initializes shared state for this new document */
    void init(Document doc, int docID) throws IOException {

      this.docID = docID;
      docBoost = doc.getBoost();
      numStoredFields = 0;
      numFieldData = 0;
      numVectorFields = 0;

      List docFields = doc.getFields();
      final int numDocFields = docFields.size();
      boolean docHasVectors = false;

      // Absorb any new fields first seen in this document.
      // Also absorb any changes to fields we had already
      // seen before (eg suddenly turning on norms or
      // vectors, etc.):

      for(int i=0;i<numDocFields;i++) {
        Fieldable field = (Fieldable) docFields.get(i);

        FieldInfo fi = fieldInfos.add(field.name(), field.isIndexed(), field.isTermVectorStored(),
                                      field.isStorePositionWithTermVector(), field.isStoreOffsetWithTermVector(),
                                      field.getOmitNorms(), false);
        numStoredFields += field.isStored() ? 1:0;
        if (fi.isIndexed && !fi.omitNorms) {
          // Maybe grow our buffered norms
          if (norms.length <= fi.number) {
            int newSize = (int) ((1+fi.number)*1.25);
            BufferedNorms[] newNorms = new BufferedNorms[newSize];
            System.arraycopy(norms, 0, newNorms, 0, norms.length);
            norms = newNorms;
          }
          
          if (norms[fi.number] == null)
            norms[fi.number] = new BufferedNorms();

          hasNorms = true;
        }

        // Make sure we have a FieldData allocated
        int hashPos = fi.name.hashCode() & fieldDataHashMask;
        FieldData fp = fieldDataHash[hashPos];
        while(fp != null && !fp.fieldInfo.name.equals(fi.name))
          fp = fp.next;

        if (fp == null) {

          fp = new FieldData(fi);
          fp.next = fieldDataHash[hashPos];
          fieldDataHash[hashPos] = fp;

          if (numAllFieldData == allFieldDataArray.length) {
            int newSize = (int) (allFieldDataArray.length*1.5);

            FieldData newArray[] = new FieldData[newSize];
            System.arraycopy(allFieldDataArray, 0, newArray, 0, numAllFieldData);
            allFieldDataArray = newArray;

            // Rehash
            newSize = fieldDataHash.length*2;
            newArray = new FieldData[newSize];
            fieldDataHashMask = newSize-1;
            for(int j=0;j<fieldDataHash.length;j++) {
              FieldData fp0 = fieldDataHash[j];
              while(fp0 != null) {
                hashPos = fp0.fieldInfo.name.hashCode() & fieldDataHashMask;
                FieldData nextFP0 = fp0.next;
                fp0.next = newArray[hashPos];
                newArray[hashPos] = fp0;
                fp0 = nextFP0;
              }
            }
            fieldDataHash = newArray;
          }
          allFieldDataArray[numAllFieldData++] = fp;
        } else {
          assert fp.fieldInfo == fi;
        }

        if (docID != fp.lastDocID) {

          // First time we're seeing this field for this doc
          fp.lastDocID = docID;
          fp.fieldCount = 0;
          fp.doVectors = fp.doVectorPositions = fp.doVectorOffsets = false;
          fp.doNorms = fi.isIndexed && !fi.omitNorms;

          if (numFieldData == fieldDataArray.length) {
            int newSize = fieldDataArray.length*2;
            FieldData newArray[] = new FieldData[newSize];
            System.arraycopy(fieldDataArray, 0, newArray, 0, numFieldData);
            fieldDataArray = newArray;

          }
          fieldDataArray[numFieldData++] = fp;
        }

        if (field.isTermVectorStored()) {
          if (!fp.doVectors) {
            if (numVectorFields++ == vectorFieldPointers.length) {
              final int newSize = (int) (numVectorFields*1.5);
              vectorFieldPointers = new long[newSize];
              vectorFieldNumbers = new int[newSize];
            }
          }
          fp.doVectors = true;
          docHasVectors = true;

          fp.doVectorPositions |= field.isStorePositionWithTermVector();
          fp.doVectorOffsets |= field.isStoreOffsetWithTermVector();
        }

        if (fp.fieldCount == fp.docFields.length) {
          Fieldable[] newArray = new Fieldable[fp.docFields.length*2];
          System.arraycopy(fp.docFields, 0, newArray, 0, fp.docFields.length);
          fp.docFields = newArray;
        }

        // Lazily allocate arrays for postings:
        if (field.isIndexed() && fp.postingsHash == null)
          fp.initPostingArrays();

        fp.docFields[fp.fieldCount++] = field;
      }

      // Maybe init the local & global fieldsWriter
      if (localFieldsWriter == null) {
        if (fieldsWriter == null) {
          assert docStoreSegment == null;
          assert segment != null;
          docStoreSegment = segment;
          fieldsWriter = new FieldsWriter(directory, docStoreSegment, fieldInfos);
          files = null;
        }
        localFieldsWriter = new FieldsWriter(null, fdtLocal, fieldInfos);
      }

      // First time we see a doc that has field(s) with
      // stored vectors, we init our tvx writer
      if (docHasVectors) {
        if (tvx == null) {
          assert docStoreSegment != null;
          tvx = directory.createOutput(docStoreSegment + "." + IndexFileNames.VECTORS_INDEX_EXTENSION);
          tvx.writeInt(TermVectorsReader.FORMAT_VERSION);
          tvd = directory.createOutput(docStoreSegment +  "." + IndexFileNames.VECTORS_DOCUMENTS_EXTENSION);
          tvd.writeInt(TermVectorsReader.FORMAT_VERSION);
          tvf = directory.createOutput(docStoreSegment +  "." + IndexFileNames.VECTORS_FIELDS_EXTENSION);
          tvf.writeInt(TermVectorsReader.FORMAT_VERSION);
          files = null;

          // We must "catch up" for all docIDs that had no
          // vectors before this one
          for(int i=0;i<docID;i++)
            tvx.writeLong(0);
        }

        numVectorFields = 0;
      }
    }

    /** Do in-place sort of Posting array */
    void doPostingSort(Posting[] postings, int numPosting) {
      quickSort(postings, 0, numPosting-1);
    }

    void quickSort(Posting[] postings, int lo, int hi) {
      if (lo >= hi)
        return;

      int mid = (lo + hi) >>> 1;

      if (comparePostings(postings[lo], postings[mid]) > 0) {
        Posting tmp = postings[lo];
        postings[lo] = postings[mid];
        postings[mid] = tmp;
      }

      if (comparePostings(postings[mid], postings[hi]) > 0) {
        Posting tmp = postings[mid];
        postings[mid] = postings[hi];
        postings[hi] = tmp;

        if (comparePostings(postings[lo], postings[mid]) > 0) {
          Posting tmp2 = postings[lo];
          postings[lo] = postings[mid];
          postings[mid] = tmp2;
        }
      }

      int left = lo + 1;
      int right = hi - 1;

      if (left >= right)
        return;

      Posting partition = postings[mid];

      for (; ;) {
        while (comparePostings(postings[right], partition) > 0)
          --right;

        while (left < right && comparePostings(postings[left], partition) <= 0)
          ++left;

        if (left < right) {
          Posting tmp = postings[left];
          postings[left] = postings[right];
          postings[right] = tmp;
          --right;
        } else {
          break;
        }
      }

      quickSort(postings, lo, left);
      quickSort(postings, left + 1, hi);
    }

    /** Do in-place sort of PostingVector array */
    void doVectorSort(PostingVector[] postings, int numPosting) {
      quickSort(postings, 0, numPosting-1);
    }

    void quickSort(PostingVector[] postings, int lo, int hi) {
      if (lo >= hi)
        return;

      int mid = (lo + hi) >>> 1;

      if (comparePostings(postings[lo].p, postings[mid].p) > 0) {
        PostingVector tmp = postings[lo];
        postings[lo] = postings[mid];
        postings[mid] = tmp;
      }

      if (comparePostings(postings[mid].p, postings[hi].p) > 0) {
        PostingVector tmp = postings[mid];
        postings[mid] = postings[hi];
        postings[hi] = tmp;

        if (comparePostings(postings[lo].p, postings[mid].p) > 0) {
          PostingVector tmp2 = postings[lo];
          postings[lo] = postings[mid];
          postings[mid] = tmp2;
        }
      }

      int left = lo + 1;
      int right = hi - 1;

      if (left >= right)
        return;

      PostingVector partition = postings[mid];

      for (; ;) {
        while (comparePostings(postings[right].p, partition.p) > 0)
          --right;

        while (left < right && comparePostings(postings[left].p, partition.p) <= 0)
          ++left;

        if (left < right) {
          PostingVector tmp = postings[left];
          postings[left] = postings[right];
          postings[right] = tmp;
          --right;
        } else {
          break;
        }
      }

      quickSort(postings, lo, left);
      quickSort(postings, left + 1, hi);
    }

    /** If there are fields we've seen but did not see again
     *  in the last run, then free them up.  Also reduce
     *  postings hash size. */
    void trimFields() {

      int upto = 0;
      for(int i=0;i<numAllFieldData;i++) {
        FieldData fp = allFieldDataArray[i];
        if (fp.lastDocID == -1) {
          // This field was not seen since the previous
          // flush, so, free up its resources now

          // Unhash
          final int hashPos = fp.fieldInfo.name.hashCode() & fieldDataHashMask;
          FieldData last = null;
          FieldData fp0 = fieldDataHash[hashPos];
          while(fp0 != fp) {
            last = fp0;
            fp0 = fp0.next;
          }
          assert fp0 != null;

          if (last == null)
            fieldDataHash[hashPos] = fp.next;
          else
            last.next = fp.next;

          if (infoStream != null)
            infoStream.println("  remove field=" + fp.fieldInfo.name);

        } else {
          // Reset
          fp.lastDocID = -1;
          allFieldDataArray[upto++] = fp;
          
          if (fp.numPostings > 0 && ((float) fp.numPostings) / fp.postingsHashSize < 0.2) {
            int hashSize = fp.postingsHashSize;

            // Reduce hash so it's between 25-50% full
            while (fp.numPostings < (hashSize>>1) && hashSize >= 2)
              hashSize >>= 1;
            hashSize <<= 1;

            if (hashSize != fp.postingsHash.length)
              fp.rehashPostings(hashSize);
          }
        }
      }

      // If we didn't see any norms for this field since
      // last flush, free it
      for(int i=0;i<norms.length;i++) {
        BufferedNorms n = norms[i];
        if (n != null && n.upto == 0)
          norms[i] = null;
      }

      numAllFieldData = upto;

      // Also pare back PostingsVectors if it's excessively
      // large
      if (maxPostingsVectors * 1.5 < postingsVectors.length) {
        final int newSize;
        if (0 == maxPostingsVectors)
          newSize = 1;
        else
          newSize = (int) (1.5*maxPostingsVectors);
        PostingVector[] newArray = new PostingVector[newSize];
        System.arraycopy(postingsVectors, 0, newArray, 0, newSize);
        postingsVectors = newArray;
      }
    }

    /** Tokenizes the fields of a document into Postings */
    void processDocument(Analyzer analyzer)
      throws IOException {

      final int numFields = numFieldData;

      fdtLocal.writeVInt(numStoredFields);

      if (tvx != null)
        // If we are writing vectors then we must visit
        // fields in sorted order so they are written in
        // sorted order.  TODO: we actually only need to
        // sort the subset of fields that have vectors
        // enabled; we could save [small amount of] CPU
        // here.
        Arrays.sort(fieldDataArray, 0, numFields);

      // We process the document one field at a time
      for(int i=0;i<numFields;i++)
        fieldDataArray[i].processField(analyzer);

      if (ramBufferSize != IndexWriter.DISABLE_AUTO_FLUSH
          && numBytesUsed > 0.95 * ramBufferSize)
        balanceRAM();
    }

    final ByteBlockPool postingsPool = new ByteBlockPool();
    final ByteBlockPool vectorsPool = new ByteBlockPool();
    final CharBlockPool charPool = new CharBlockPool();

    // Current posting we are working on
    Posting p;
    PostingVector vector;

    // USE ONLY FOR DEBUGGING!
    /*
      public String getPostingText() {
      char[] text = charPool.buffers[p.textStart >> CHAR_BLOCK_SHIFT];
      int upto = p.textStart & CHAR_BLOCK_MASK;
      while(text[upto] != 0xffff)
      upto++;
      return new String(text, p.textStart, upto-(p.textStart & BYTE_BLOCK_MASK));
      }
    */

    /** Test whether the text for current Posting p equals
     *  current tokenText. */
    boolean postingEquals(final char[] tokenText, final int tokenTextLen) {

      final char[] text = charPool.buffers[p.textStart >> CHAR_BLOCK_SHIFT];
      assert text != null;
      int pos = p.textStart & CHAR_BLOCK_MASK;

      int tokenPos = 0;
      for(;tokenPos<tokenTextLen;pos++,tokenPos++)
        if (tokenText[tokenPos] != text[pos])
          return false;
      return 0xffff == text[pos];
    }

    /** Compares term text for two Posting instance and
     *  returns -1 if p1 < p2; 1 if p1 > p2; else 0.
     */
    int comparePostings(Posting p1, Posting p2) {
      final char[] text1 = charPool.buffers[p1.textStart >> CHAR_BLOCK_SHIFT];
      int pos1 = p1.textStart & CHAR_BLOCK_MASK;
      final char[] text2 = charPool.buffers[p2.textStart >> CHAR_BLOCK_SHIFT];
      int pos2 = p2.textStart & CHAR_BLOCK_MASK;
      while(true) {
        final char c1 = text1[pos1++];
        final char c2 = text2[pos2++];
        if (c1 < c2)
          if (0xffff == c2)
            return 1;
          else
            return -1;
        else if (c2 < c1)
          if (0xffff == c1)
            return -1;
          else
            return 1;
        else if (0xffff == c1)
          return 0;
      }
    }

    /** Write vInt into freq stream of current Posting */
    public void writeFreqVInt(int i) {
      while ((i & ~0x7F) != 0) {
        writeFreqByte((byte)((i & 0x7f) | 0x80));
        i >>>= 7;
      }
      writeFreqByte((byte) i);
    }

    /** Write vInt into prox stream of current Posting */
    public void writeProxVInt(int i) {
      while ((i & ~0x7F) != 0) {
        writeProxByte((byte)((i & 0x7f) | 0x80));
        i >>>= 7;
      }
      writeProxByte((byte) i);
    }

    /** Write byte into freq stream of current Posting */
    byte[] freq;
    int freqUpto;
    public void writeFreqByte(byte b) {
      assert freq != null;
      if (freq[freqUpto] != 0) {
        freqUpto = postingsPool.allocSlice(freq, freqUpto);
        freq = postingsPool.buffer;
        p.freqUpto = postingsPool.byteOffset;
      }
      freq[freqUpto++] = b;
    }

    /** Write byte into prox stream of current Posting */
    byte[] prox;
    int proxUpto;
    public void writeProxByte(byte b) {
      assert prox != null;
      if (prox[proxUpto] != 0) {
        proxUpto = postingsPool.allocSlice(prox, proxUpto);
        prox = postingsPool.buffer;
        p.proxUpto = postingsPool.byteOffset;
        assert prox != null;
      }
      prox[proxUpto++] = b;
      assert proxUpto != prox.length;
    }

    /** Currently only used to copy a payload into the prox
     *  stream. */
    public void writeProxBytes(byte[] b, int offset, int len) {
      final int offsetEnd = offset + len;
      while(offset < offsetEnd) {
        if (prox[proxUpto] != 0) {
          // End marker
          proxUpto = postingsPool.allocSlice(prox, proxUpto);
          prox = postingsPool.buffer;
          p.proxUpto = postingsPool.byteOffset;
        }

        prox[proxUpto++] = b[offset++];
        assert proxUpto != prox.length;
      }
    }

    /** Write vInt into offsets stream of current
     *  PostingVector */
    public void writeOffsetVInt(int i) {
      while ((i & ~0x7F) != 0) {
        writeOffsetByte((byte)((i & 0x7f) | 0x80));
        i >>>= 7;
      }
      writeOffsetByte((byte) i);
    }

    byte[] offsets;
    int offsetUpto;

    /** Write byte into offsets stream of current
     *  PostingVector */
    public void writeOffsetByte(byte b) {
      assert offsets != null;
      if (offsets[offsetUpto] != 0) {
        offsetUpto = vectorsPool.allocSlice(offsets, offsetUpto);
        offsets = vectorsPool.buffer;
        vector.offsetUpto = vectorsPool.byteOffset;
      }
      offsets[offsetUpto++] = b;
    }

    /** Write vInt into pos stream of current
     *  PostingVector */
    public void writePosVInt(int i) {
      while ((i & ~0x7F) != 0) {
        writePosByte((byte)((i & 0x7f) | 0x80));
        i >>>= 7;
      }
      writePosByte((byte) i);
    }

    byte[] pos;
    int posUpto;

    /** Write byte into pos stream of current
     *  PostingVector */
    public void writePosByte(byte b) {
      assert pos != null;
      if (pos[posUpto] != 0) {
        posUpto = vectorsPool.allocSlice(pos, posUpto);
        pos = vectorsPool.buffer;
        vector.posUpto = vectorsPool.byteOffset;
      }
      pos[posUpto++] = b;
    }

    PostingVector[] postingsVectors = new PostingVector[1];
    int maxPostingsVectors;

    // Used to read a string value for a field
    ReusableStringReader stringReader = new ReusableStringReader();

    /** Holds data associated with a single field, including
     *  the Postings hash.  A document may have many *
     *  occurrences for a given field name; we gather all *
     *  such occurrences here (in docFields) so that we can
     *  * process the entire field at once. */
    private final class FieldData implements Comparable {

      ThreadState threadState;
      FieldInfo fieldInfo;

      int fieldCount;
      Fieldable[] docFields = new Fieldable[1];

      int lastDocID = -1;
      FieldData next;

      boolean doNorms;
      boolean doVectors;
      boolean doVectorPositions;
      boolean doVectorOffsets;

      int numPostings;
      
      Posting[] postingsHash;
      int postingsHashSize;
      int postingsHashHalfSize;
      int postingsHashMask;

      int position;
      int length;
      int offset;
      float boost;
      int postingsVectorsUpto;

      public FieldData(FieldInfo fieldInfo) {
        this.fieldInfo = fieldInfo;
        threadState = ThreadState.this;
      }

      void resetPostingArrays() {
        recyclePostings(this.postingsHash, numPostings);
        Arrays.fill(postingsHash, 0, postingsHash.length, null);
        numPostings = 0;
      }

      void initPostingArrays() {
        // Target hash fill factor of <= 50%
        // NOTE: must be a power of two for hash collision
        // strategy to work correctly
        postingsHashSize = 4;
        postingsHashHalfSize = 2;
        postingsHashMask = postingsHashSize-1;
        postingsHash = new Posting[postingsHashSize];
      }

      /** So Arrays.sort can sort us. */
      public int compareTo(Object o) {
        return fieldInfo.name.compareTo(((FieldData) o).fieldInfo.name);
      }

      /** Collapse the hash table & sort in-place. */
      public Posting[] sortPostings() {
        int upto = 0;
        for(int i=0;i<postingsHashSize;i++)
          if (postingsHash[i] != null)
            postingsHash[upto++] = postingsHash[i];

        assert upto == numPostings;
        doPostingSort(postingsHash, upto);
        return postingsHash;
      }

      /** Process all occurrences of one field in the document. */
      public void processField(Analyzer analyzer) throws IOException {
        length = 0;
        position = 0;
        offset = 0;
        boost = docBoost;

        final int maxFieldLength = writer.getMaxFieldLength();

        final int limit = fieldCount;
        final Fieldable[] docFieldsFinal = docFields;

        // Walk through all occurrences in this doc for this field:
        for(int j=0;j<limit;j++) {
          Fieldable field = docFieldsFinal[j];

          if (field.isIndexed())
            invertField(field, analyzer, maxFieldLength);

          if (field.isStored())
            localFieldsWriter.writeField(fieldInfo, field);

          docFieldsFinal[j] = null;
        }

        if (postingsVectorsUpto > 0) {
          // Add term vectors for this field
          writeVectors(fieldInfo);
          if (postingsVectorsUpto > maxPostingsVectors)
            maxPostingsVectors = postingsVectorsUpto;
          postingsVectorsUpto = 0;
          vectorsPool.reset();
        }
      }

      int offsetEnd;
      Token localToken = new Token();

      /* Invert one occurrence of one field in the document */
      public void invertField(Fieldable field, Analyzer analyzer, final int maxFieldLength) throws IOException {

        if (length>0)
          position += analyzer.getPositionIncrementGap(fieldInfo.name);

        if (!field.isTokenized()) {		  // un-tokenized field
          String stringValue = field.stringValue();
          Token token = localToken;
          token.clear();
          token.setTermText(stringValue);
          token.setStartOffset(offset);
          token.setEndOffset(offset + stringValue.length());
          addPosition(token);
          offset += stringValue.length();
          length++;
        } else {                                  // tokenized field
          final TokenStream stream;
          final TokenStream streamValue = field.tokenStreamValue();

          if (streamValue != null) 
            stream = streamValue;
          else {
            // the field does not have a TokenStream,
            // so we have to obtain one from the analyzer
            final Reader reader;			  // find or make Reader
            final Reader readerValue = field.readerValue();

            if (readerValue != null)
              reader = readerValue;
            else {
              String stringValue = field.stringValue();
              if (stringValue == null)
                throw new IllegalArgumentException("field must have either TokenStream, String or Reader value");
              stringReader.init(stringValue);
              reader = stringReader;
            }
          
            // Tokenize field and add to postingTable
            stream = analyzer.reusableTokenStream(fieldInfo.name, reader);
          }

          // reset the TokenStream to the first token
          stream.reset();

          try {
            offsetEnd = offset-1;
            Token token;
            for(;;) {
              localToken.clear();
              token = stream.next(localToken);
              if (token == null) break;
              position += (token.getPositionIncrement() - 1);
              addPosition(token);
              if (++length >= maxFieldLength) {
                if (infoStream != null)
                  infoStream.println("maxFieldLength " +maxFieldLength+ " reached for field " + fieldInfo.name + ", ignoring following tokens");
                break;
              }
            }
            offset = offsetEnd+1;
          } finally {
            stream.close();
          }
        }

        boost *= field.getBoost();
      }

      /** Only called when term vectors are enabled.  This
       *  is called the first time we see a given term for
       *  each * document, to allocate a PostingVector
       *  instance that * is used to record data needed to
       *  write the posting * vectors. */
      private PostingVector addNewVector() {

        if (postingsVectorsUpto == postingsVectors.length) {
          final int newSize;
          if (postingsVectors.length < 2)
            newSize = 2;
          else
            newSize = (int) (1.5*postingsVectors.length);
          PostingVector[] newArray = new PostingVector[newSize];
          System.arraycopy(postingsVectors, 0, newArray, 0, postingsVectors.length);
          postingsVectors = newArray;
        }
        
        p.vector = postingsVectors[postingsVectorsUpto];
        if (p.vector == null)
          p.vector = postingsVectors[postingsVectorsUpto] = new PostingVector();

        postingsVectorsUpto++;

        final PostingVector v = p.vector;
        v.p = p;

        final int firstSize = levelSizeArray[0];

        if (doVectorPositions) {
          final int upto = vectorsPool.newSlice(firstSize);
          v.posStart = v.posUpto = vectorsPool.byteOffset + upto;
        }

        if (doVectorOffsets) {
          final int upto = vectorsPool.newSlice(firstSize);
          v.offsetStart = v.offsetUpto = vectorsPool.byteOffset + upto;
        }

        return v;
      }

      int offsetStartCode;
      int offsetStart;

      /** This is the hotspot of indexing: it's called once
       *  for every term of every document.  Its job is to *
       *  update the postings byte stream (Postings hash) *
       *  based on the occurence of a single term. */
      private void addPosition(Token token) {

        final Payload payload = token.getPayload();

        // Get the text of this term.  Term can either
        // provide a String token or offset into a char[]
        // array
        final char[] tokenText = token.termBuffer();
        final int tokenTextLen = token.termLength();

        int code = 0;

        // Compute hashcode
        int downto = tokenTextLen;
        while (downto > 0)
          code = (code*31) + tokenText[--downto];

        // System.out.println("  addPosition: buffer=" + new String(tokenText, 0, tokenTextLen) + " pos=" + position + " offsetStart=" + (offset+token.startOffset()) + " offsetEnd=" + (offset + token.endOffset()) + " docID=" + docID + " doPos=" + doVectorPositions + " doOffset=" + doVectorOffsets);

        int hashPos = code & postingsHashMask;

        // Locate Posting in hash
        p = postingsHash[hashPos];

        if (p != null && !postingEquals(tokenText, tokenTextLen)) {
          // Conflict: keep searching different locations in
          // the hash table.
          final int inc = ((code>>8)+code)|1;
          do {
            code += inc;
            hashPos = code & postingsHashMask;
            p = postingsHash[hashPos];
          } while (p != null && !postingEquals(tokenText, tokenTextLen));
        }
        
        final int proxCode;

        if (p != null) {       // term seen since last flush

          if (docID != p.lastDocID) { // term not yet seen in this doc
            
            // System.out.println("    seen before (new docID=" + docID + ") freqUpto=" + p.freqUpto +" proxUpto=" + p.proxUpto);

            assert p.docFreq > 0;

            // Now that we know doc freq for previous doc,
            // write it & lastDocCode
            freqUpto = p.freqUpto & BYTE_BLOCK_MASK;
            freq = postingsPool.buffers[p.freqUpto >> BYTE_BLOCK_SHIFT];
            if (1 == p.docFreq)
              writeFreqVInt(p.lastDocCode|1);
            else {
              writeFreqVInt(p.lastDocCode);
              writeFreqVInt(p.docFreq);
            }
            p.freqUpto = freqUpto + (p.freqUpto & BYTE_BLOCK_NOT_MASK);

            if (doVectors) {
              vector = addNewVector();
              if (doVectorOffsets) {
                offsetStartCode = offsetStart = offset + token.startOffset();
                offsetEnd = offset + token.endOffset();
              }
            }

            proxCode = position;

            p.docFreq = 1;

            // Store code so we can write this after we're
            // done with this new doc
            p.lastDocCode = (docID-p.lastDocID) << 1;
            p.lastDocID = docID;

          } else {                                // term already seen in this doc
            // System.out.println("    seen before (same docID=" + docID + ") proxUpto=" + p.proxUpto);
            p.docFreq++;

            proxCode = position-p.lastPosition;

            if (doVectors) {
              vector = p.vector;
              if (vector == null)
                vector = addNewVector();
              if (doVectorOffsets) {
                offsetStart = offset + token.startOffset();
                offsetEnd = offset + token.endOffset();
                offsetStartCode = offsetStart-vector.lastOffset;
              }
            }
          }
        } else {					  // term not seen before
          // System.out.println("    never seen docID=" + docID);

          // Refill?
          if (0 == postingsFreeCount) {
            postingsFreeCount = postingsFreeList.length;
            getPostings(postingsFreeList);
          }

          // Pull next free Posting from free list
          p = postingsFreeList[--postingsFreeCount];

          final int textLen1 = 1+tokenTextLen;
          if (textLen1 + charPool.byteUpto > CHAR_BLOCK_SIZE) {
            if (textLen1 > CHAR_BLOCK_SIZE)
              throw new IllegalArgumentException("term length " + tokenTextLen + " exceeds max term length " + (CHAR_BLOCK_SIZE-1));
            charPool.nextBuffer();
          }
          final char[] text = charPool.buffer;
          final int textUpto = charPool.byteUpto;
          p.textStart = textUpto + charPool.byteOffset;
          charPool.byteUpto += textLen1;

          System.arraycopy(tokenText, 0, text, textUpto, tokenTextLen);

          text[textUpto+tokenTextLen] = 0xffff;
          
          assert postingsHash[hashPos] == null;

          postingsHash[hashPos] = p;
          numPostings++;

          if (numPostings == postingsHashHalfSize)
            rehashPostings(2*postingsHashSize);

          // Init first slice for freq & prox streams
          final int firstSize = levelSizeArray[0];

          final int upto1 = postingsPool.newSlice(firstSize);
          p.freqStart = p.freqUpto = postingsPool.byteOffset + upto1;

          final int upto2 = postingsPool.newSlice(firstSize);
          p.proxStart = p.proxUpto = postingsPool.byteOffset + upto2;

          p.lastDocCode = docID << 1;
          p.lastDocID = docID;
          p.docFreq = 1;

          if (doVectors) {
            vector = addNewVector();
            if (doVectorOffsets) {
              offsetStart = offsetStartCode = offset + token.startOffset();
              offsetEnd = offset + token.endOffset();
            }
          }

          proxCode = position;
        }

        proxUpto = p.proxUpto & BYTE_BLOCK_MASK;
        prox = postingsPool.buffers[p.proxUpto >> BYTE_BLOCK_SHIFT];
        assert prox != null;

        if (payload != null && payload.length > 0) {
          writeProxVInt((proxCode<<1)|1);
          writeProxVInt(payload.length);
          writeProxBytes(payload.data, payload.offset, payload.length);
          fieldInfo.storePayloads = true;
        } else
          writeProxVInt(proxCode<<1);

        p.proxUpto = proxUpto + (p.proxUpto & BYTE_BLOCK_NOT_MASK);

        p.lastPosition = position++;

        if (doVectorPositions) {
          posUpto = vector.posUpto & BYTE_BLOCK_MASK;
          pos = vectorsPool.buffers[vector.posUpto >> BYTE_BLOCK_SHIFT];
          writePosVInt(proxCode);
          vector.posUpto = posUpto + (vector.posUpto & BYTE_BLOCK_NOT_MASK);
        }

        if (doVectorOffsets) {
          offsetUpto = vector.offsetUpto & BYTE_BLOCK_MASK;
          offsets = vectorsPool.buffers[vector.offsetUpto >> BYTE_BLOCK_SHIFT];
          writeOffsetVInt(offsetStartCode);
          writeOffsetVInt(offsetEnd-offsetStart);
          vector.lastOffset = offsetEnd;
          vector.offsetUpto = offsetUpto + (vector.offsetUpto & BYTE_BLOCK_NOT_MASK);
        }
      }

      /** Called when postings hash is too small (> 50%
       *  occupied) or too large (< 20% occupied). */
      void rehashPostings(final int newSize) {

        postingsHashMask = newSize-1;

        Posting[] newHash = new Posting[newSize];
        for(int i=0;i<postingsHashSize;i++) {
          Posting p0 = postingsHash[i];
          if (p0 != null) {
            final int start = p0.textStart & CHAR_BLOCK_MASK;
            final char[] text = charPool.buffers[p0.textStart >> CHAR_BLOCK_SHIFT];
            int pos = start;
            while(text[pos] != 0xffff)
              pos++;
            int code = 0;
            while (pos > start)
              code = (code*31) + text[--pos];

            int hashPos = code & postingsHashMask;
            assert hashPos >= 0;
            if (newHash[hashPos] != null) {
              final int inc = ((code>>8)+code)|1;
              do {
                code += inc;
                hashPos = code & postingsHashMask;
              } while (newHash[hashPos] != null);
            }
            newHash[hashPos] = p0;
          }
        }

        postingsHash = newHash;
        postingsHashSize = newSize;
        postingsHashHalfSize = newSize >> 1;
      }
      
      final ByteSliceReader vectorSliceReader = new ByteSliceReader();

      /** Called once per field per document if term vectors
       *  are enabled, to write the vectors to *
       *  RAMOutputStream, which is then quickly flushed to
       *  * the real term vectors files in the Directory. */
      void writeVectors(FieldInfo fieldInfo) throws IOException {

        assert fieldInfo.storeTermVector;

        vectorFieldNumbers[numVectorFields] = fieldInfo.number;
        vectorFieldPointers[numVectorFields] = tvfLocal.getFilePointer();
        numVectorFields++;

        final int numPostingsVectors = postingsVectorsUpto;

        tvfLocal.writeVInt(numPostingsVectors);
        byte bits = 0x0;
        if (doVectorPositions)
          bits |= TermVectorsReader.STORE_POSITIONS_WITH_TERMVECTOR;
        if (doVectorOffsets) 
          bits |= TermVectorsReader.STORE_OFFSET_WITH_TERMVECTOR;
        tvfLocal.writeByte(bits);

        doVectorSort(postingsVectors, numPostingsVectors);

        Posting lastPosting = null;

        final ByteSliceReader reader = vectorSliceReader;

        for(int j=0;j<numPostingsVectors;j++) {
          PostingVector vector = postingsVectors[j];
          Posting posting = vector.p;
          final int freq = posting.docFreq;
          
          final int prefix;
          final char[] text2 = charPool.buffers[posting.textStart >> CHAR_BLOCK_SHIFT];
          final int start2 = posting.textStart & CHAR_BLOCK_MASK;
          int pos2 = start2;

          // Compute common prefix between last term and
          // this term
          if (lastPosting == null)
            prefix = 0;
          else {
            final char[] text1 = charPool.buffers[lastPosting.textStart >> CHAR_BLOCK_SHIFT];
            final int start1 = lastPosting.textStart & CHAR_BLOCK_MASK;
            int pos1 = start1;
            while(true) {
              final char c1 = text1[pos1];
              final char c2 = text2[pos2];
              if (c1 != c2 || c1 == 0xffff) {
                prefix = pos1-start1;
                break;
              }
              pos1++;
              pos2++;
            }
          }
          lastPosting = posting;

          // Compute length
          while(text2[pos2] != 0xffff)
            pos2++;

          final int suffix = pos2 - start2 - prefix;
          tvfLocal.writeVInt(prefix);
          tvfLocal.writeVInt(suffix);
          tvfLocal.writeChars(text2, start2 + prefix, suffix);
          tvfLocal.writeVInt(freq);

          if (doVectorPositions) {
            reader.init(vectorsPool, vector.posStart, vector.posUpto);
            reader.writeTo(tvfLocal);
          }

          if (doVectorOffsets) {
            reader.init(vectorsPool, vector.offsetStart, vector.offsetUpto);
            reader.writeTo(tvfLocal);
          }
        }
      }
    }
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
      ThreadState state = threadStates[i];
      state.trimFields();
      final int numFields = state.numAllFieldData;
      for(int j=0;j<numFields;j++) {
        ThreadState.FieldData fp = state.allFieldDataArray[j];
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

      final String fieldName = ((ThreadState.FieldData) allFields.get(start)).fieldInfo.name;

      int end = start+1;
      while(end < numAllFields && ((ThreadState.FieldData) allFields.get(end)).fieldInfo.name.equals(fieldName))
        end++;
      
      ThreadState.FieldData[] fields = new ThreadState.FieldData[end-start];
      for(int i=start;i<end;i++)
        fields[i-start] = (ThreadState.FieldData) allFields.get(i);

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
      infoStream.println(message);
    }

    resetPostingsData();

    nextDocID = 0;
    nextWriteDocID = 0;
    numDocsInRAM = 0;
    files = null;

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

  /** Returns the name of the file with this extension, on
   *  the current segment we are working on. */
  private String segmentFileName(String extension) {
    return segment + "." + extension;
  }

  private final TermInfo termInfo = new TermInfo(); // minimize consing

  /** Used to merge the postings from multiple ThreadStates
   * when creating a segment */
  final static class FieldMergeState {

    private ThreadState.FieldData field;

    private Posting[] postings;

    private Posting p;
    private  char[] text;
    private int textOffset;

    private int postingUpto = -1;

    private ByteSliceReader freq = new ByteSliceReader();
    private ByteSliceReader prox = new ByteSliceReader();

    private int lastDocID;
    private int docID;
    private int termFreq;

    boolean nextTerm() throws IOException {
      postingUpto++;
      if (postingUpto == field.numPostings)
        return false;

      p = postings[postingUpto];
      docID = 0;

      text = field.threadState.charPool.buffers[p.textStart >> CHAR_BLOCK_SHIFT];
      textOffset = p.textStart & CHAR_BLOCK_MASK;

      if (p.freqUpto > p.freqStart)
        freq.init(field.threadState.postingsPool, p.freqStart, p.freqUpto);
      else
        freq.bufferOffset = freq.upto = freq.endIndex = 0;

      prox.init(field.threadState.postingsPool, p.proxStart, p.proxUpto);

      // Should always be true
      boolean result = nextDoc();
      assert result;

      return true;
    }

    public boolean nextDoc() throws IOException {
      if (freq.bufferOffset + freq.upto == freq.endIndex) {
        if (p.lastDocCode != -1) {
          // Return last doc
          docID = p.lastDocID;
          termFreq = p.docFreq;
          p.lastDocCode = -1;
          return true;
        } else 
          // EOF
          return false;
      }

      final int code = freq.readVInt();
      docID += code >>> 1;
      if ((code & 1) != 0)
        termFreq = 1;
      else
        termFreq = freq.readVInt();

      return true;
    }
  }

  int compareText(final char[] text1, int pos1, final char[] text2, int pos2) {
    while(true) {
      final char c1 = text1[pos1++];
      final char c2 = text2[pos2++];
      if (c1 < c2)
        if (0xffff == c2)
          return 1;
        else
          return -1;
      else if (c2 < c1)
        if (0xffff == c1)
          return -1;
        else
          return 1;
      else if (0xffff == c1)
        return 0;
    }
  }

  /* Walk through all unique text tokens (Posting
   * instances) found in this field and serialize them
   * into a single RAM segment. */
  void appendPostings(ThreadState.FieldData[] fields,
                      TermInfosWriter termsOut,
                      IndexOutput freqOut,
                      IndexOutput proxOut)
    throws CorruptIndexException, IOException {

    final String fieldName = fields[0].fieldInfo.name;
    int numFields = fields.length;

    final FieldMergeState[] mergeStates = new FieldMergeState[numFields];

    for(int i=0;i<numFields;i++) {
      FieldMergeState fms = mergeStates[i] = new FieldMergeState();
      fms.field = fields[i];
      fms.postings = fms.field.sortPostings();

      assert fms.field.fieldInfo == fields[0].fieldInfo;

      // Should always be true
      boolean result = fms.nextTerm();
      assert result;
    }

    final int skipInterval = termsOut.skipInterval;
    currentFieldStorePayloads = fields[0].fieldInfo.storePayloads;

    FieldMergeState[] termStates = new FieldMergeState[numFields];

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
      int pos = start;
      while(text[pos] != 0xffff)
        pos++;

      // TODO: can we avoid 2 new objects here?
      Term term = new Term(fieldName, new String(text, start, pos-start));

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

        FieldMergeState minState = termStates[0];
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
      termsOut.add(term, termInfo);
    }
  }

  /** Returns a free (idle) ThreadState that may be used for
   * indexing this one document.  This call also pauses if a
   * flush is pending.  If delTerm is non-null then we
   * buffer this deleted term after the thread state has
   * been acquired. */
  synchronized ThreadState getThreadState(Document doc, Term delTerm) throws IOException {

    // First, find a thread state.  If this thread already
    // has affinity to a specific ThreadState, use that one
    // again.
    ThreadState state = (ThreadState) threadBindings.get(Thread.currentThread());
    if (state == null) {
      // First time this thread has called us since last flush
      ThreadState minThreadState = null;
      for(int i=0;i<threadStates.length;i++) {
        ThreadState ts = threadStates[i];
        if (minThreadState == null || ts.numThreads < minThreadState.numThreads)
          minThreadState = ts;
      }
      if (minThreadState != null && (minThreadState.numThreads == 0 || threadStates.length == MAX_THREAD_STATE)) {
        state = minThreadState;
        state.numThreads++;
      } else {
        // Just create a new "private" thread state
        ThreadState[] newArray = new ThreadState[1+threadStates.length];
        if (threadStates.length > 0)
          System.arraycopy(threadStates, 0, newArray, 0, threadStates.length);
        state = newArray[threadStates.length] = new ThreadState();
        threadStates = newArray;
      }
      threadBindings.put(Thread.currentThread(), state);
    }

    // Next, wait until my thread state is idle (in case
    // it's shared with other threads) and for threads to
    // not be paused nor a flush pending:
    while(!state.isIdle || pauseThreads != 0 || flushPending)
      try {
        wait();
      } catch (InterruptedException e) {}

    if (segment == null)
      segment = writer.newSegmentName();

    numDocsInRAM++;
    numDocsInStore++;

    // We must at this point commit to flushing to ensure we
    // always get N docs when we flush by doc count, even if
    // > 1 thread is adding documents:
    if (!flushPending && maxBufferedDocs != IndexWriter.DISABLE_AUTO_FLUSH
        && numDocsInRAM >= maxBufferedDocs) {
      flushPending = true;
      state.doFlushAfter = true;
    } else
      state.doFlushAfter = false;

    state.isIdle = false;

    boolean success = false;
    try {
      state.init(doc, nextDocID++);
      success = true;
    } finally {
      if (!success) {
        state.isIdle = true;
        if (state.doFlushAfter) {
          state.doFlushAfter = false;
          flushPending = false;
        }
        abort();
      }
    }

    if (delTerm != null) {
      addDeleteTerm(delTerm, state.docID);
      if (!state.doFlushAfter) {
        state.doFlushAfter = timeToFlushDeletes();
      }
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
    final ThreadState state = getThreadState(doc, delTerm);
    boolean success = false;
    try {
      // This call is not synchronized and does all the work
      state.processDocument(analyzer);
      // This call synchronized but fast
      finishDocument(state);
      success = true;
    } finally {
      if (!success) {
        state.isIdle = true;
        abort();
      }
    }
    return state.doFlushAfter || timeToFlushDeletes();
  }

  synchronized int getNumBufferedDeleteTerms() {
    return numBufferedDeleteTerms;
  }

  synchronized HashMap getBufferedDeleteTerms() {
    return bufferedDeleteTerms;
  }

  // Reset buffered deletes.
  synchronized void clearBufferedDeleteTerms() throws IOException {
    bufferedDeleteTerms.clear();
    numBufferedDeleteTerms = 0;
    if (numBytesUsed > 0)
      resetPostingsData();
  }

  synchronized boolean bufferDeleteTerms(Term[] terms) throws IOException {
    while(pauseThreads != 0 || flushPending)
      try {
        wait();
      } catch (InterruptedException e) {}
      for (int i = 0; i < terms.length; i++)
        addDeleteTerm(terms[i], numDocsInRAM);
    return timeToFlushDeletes();
  }

  synchronized boolean bufferDeleteTerm(Term term) throws IOException {
    while(pauseThreads != 0 || flushPending)
      try {
        wait();
      } catch (InterruptedException e) {}
    addDeleteTerm(term, numDocsInRAM);
    return timeToFlushDeletes();
  }

  synchronized private boolean timeToFlushDeletes() {
    return (bufferIsFull
            || (maxBufferedDeleteTerms != IndexWriter.DISABLE_AUTO_FLUSH
                && numBufferedDeleteTerms >= maxBufferedDeleteTerms))
           && setFlushPending();
  }

  void setMaxBufferedDeleteTerms(int maxBufferedDeleteTerms) {
    this.maxBufferedDeleteTerms = maxBufferedDeleteTerms;
  }

  int getMaxBufferedDeleteTerms() {
    return maxBufferedDeleteTerms;
  }

  synchronized boolean hasDeletes() {
    return bufferedDeleteTerms.size() > 0;
  }

  // Number of documents a delete term applies to.
  static class Num {
    private int num;

    Num(int num) {
      this.num = num;
    }

    int getNum() {
      return num;
    }

    void setNum(int num) {
      // Only record the new number if it's greater than the
      // current one.  This is important because if multiple
      // threads are replacing the same doc at nearly the
      // same time, it's possible that one thread that got a
      // higher docID is scheduled before the other
      // threads.
      if (num > this.num)
        this.num = num;
    }
  }

  // Buffer a term in bufferedDeleteTerms, which records the
  // current number of documents buffered in ram so that the
  // delete term will be applied to those documents as well
  // as the disk segments.
  synchronized private void addDeleteTerm(Term term, int docCount) {
    Num num = (Num) bufferedDeleteTerms.get(term);
    if (num == null) {
      bufferedDeleteTerms.put(term, new Num(docCount));
      // This is coarse approximation of actual bytes used:
      numBytesUsed += (term.field().length() + term.text().length()) * BYTES_PER_CHAR
          + 4 + 5 * OBJECT_HEADER_BYTES + 5 * OBJECT_POINTER_BYTES;
      if (ramBufferSize != IndexWriter.DISABLE_AUTO_FLUSH
          && numBytesUsed > ramBufferSize) {
        bufferIsFull = true;
      }
    } else {
      num.setNum(docCount);
    }
    numBufferedDeleteTerms++;
  }

  /** Does the synchronized work to finish/flush the
   * inverted document. */
  private synchronized void finishDocument(ThreadState state) throws IOException {

    // Now write the indexed document to the real files.

    if (nextWriteDocID == state.docID) {
      // It's my turn, so write everything now:
      state.isIdle = true;
      nextWriteDocID++;
      state.writeDocument();

      // If any states were waiting on me, sweep through and
      // flush those that are enabled by my write.
      if (numWaiting > 0) {
        while(true) {
          int upto = 0;
          for(int i=0;i<numWaiting;i++) {
            ThreadState s = waitingThreadStates[i];
            if (s.docID == nextWriteDocID) {
              s.isIdle = true;
              nextWriteDocID++;
              s.writeDocument();
            } else
              // Compact as we go
              waitingThreadStates[upto++] = waitingThreadStates[i];
          }
          if (upto == numWaiting) 
            break;
          numWaiting = upto;
        }
      }

      // Now notify any incoming calls to addDocument
      // (above) that are waiting on our line to
      // shrink
      notifyAll();

    } else {
      // Another thread got a docID before me, but, it
      // hasn't finished its processing.  So add myself to
      // the line but don't hold up this thread.
      if (numWaiting == waitingThreadStates.length) {
        ThreadState[] newWaiting = new ThreadState[2*waitingThreadStates.length];
        System.arraycopy(waitingThreadStates, 0, newWaiting, 0, numWaiting);
        waitingThreadStates = newWaiting;
      }
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

  byte[] copyByteBuffer = new byte[4096];

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

  /* Stores norms, buffered in RAM, until they are flushed
   * to a partial segment. */
  private static class BufferedNorms {

    RAMOutputStream out;
    int upto;

    BufferedNorms() {
      out = new RAMOutputStream();
    }

    void add(float norm) throws IOException {
      byte b = Similarity.encodeNorm(norm);
      out.writeByte(b);
      upto++;
    }

    void reset() {
      out.reset();
      upto = 0;
    }

    void fill(int docID) throws IOException {
      // Must now fill in docs that didn't have this
      // field.  Note that this is how norms can consume
      // tremendous storage when the docs have widely
      // varying different fields, because we are not
      // storing the norms sparsely (see LUCENE-830)
      if (upto < docID) {
        fillBytes(out, defaultNorm, docID-upto);
        upto = docID;
      }
    }
  }

  /* Simple StringReader that can be reset to a new string;
   * we use this when tokenizing the string value from a
   * Field. */
  private final static class ReusableStringReader extends Reader {
    int upto;
    int left;
    String s;
    void init(String s) {
      this.s = s;
      left = s.length();
      this.upto = 0;
    }
    public int read(char[] c) {
      return read(c, 0, c.length);
    }
    public int read(char[] c, int off, int len) {
      if (left > len) {
        s.getChars(upto, upto+len, c, off);
        upto += len;
        left -= len;
        return len;
      } else if (0 == left) {
        return -1;
      } else {
        s.getChars(upto, upto+left, c, off);
        int r = left;
        left = 0;
        upto = s.length();
        return r;
      }
    }
    public void close() {};
  }

  /* IndexInput that knows how to read the byte slices written
   * by Posting and PostingVector.  We read the bytes in
   * each slice until we hit the end of that slice at which
   * point we read the forwarding address of the next slice
   * and then jump to it.*/
  private final static class ByteSliceReader extends IndexInput {
    ByteBlockPool pool;
    int bufferUpto;
    byte[] buffer;
    public int upto;
    int limit;
    int level;
    public int bufferOffset;

    public int endIndex;

    public void init(ByteBlockPool pool, int startIndex, int endIndex) {

      assert endIndex-startIndex > 0;

      this.pool = pool;
      this.endIndex = endIndex;

      level = 0;
      bufferUpto = startIndex / BYTE_BLOCK_SIZE;
      bufferOffset = bufferUpto * BYTE_BLOCK_SIZE;
      buffer = pool.buffers[bufferUpto];
      upto = startIndex & BYTE_BLOCK_MASK;

      final int firstSize = levelSizeArray[0];

      if (startIndex+firstSize >= endIndex) {
        // There is only this one slice to read
        limit = endIndex & BYTE_BLOCK_MASK;
      } else
        limit = upto+firstSize-4;
    }

    public byte readByte() {
      // Assert that we are not @ EOF
      assert upto + bufferOffset < endIndex;
      if (upto == limit)
        nextSlice();
      return buffer[upto++];
    }

    public long writeTo(IndexOutput out) throws IOException {
      long size = 0;
      while(true) {
        if (limit + bufferOffset == endIndex) {
          assert endIndex - bufferOffset >= upto;
          out.writeBytes(buffer, upto, limit-upto);
          size += limit-upto;
          break;
        } else {
          out.writeBytes(buffer, upto, limit-upto);
          size += limit-upto;
          nextSlice();
        }
      }

      return size;
    }

    public void nextSlice() {

      // Skip to our next slice
      final int nextIndex = ((buffer[limit]&0xff)<<24) + ((buffer[1+limit]&0xff)<<16) + ((buffer[2+limit]&0xff)<<8) + (buffer[3+limit]&0xff);

      level = nextLevelArray[level];
      final int newSize = levelSizeArray[level];

      bufferUpto = nextIndex / BYTE_BLOCK_SIZE;
      bufferOffset = bufferUpto * BYTE_BLOCK_SIZE;

      buffer = pool.buffers[bufferUpto];
      upto = nextIndex & BYTE_BLOCK_MASK;

      if (nextIndex + newSize >= endIndex) {
        // We are advancing to the final slice
        assert endIndex - nextIndex > 0;
        limit = endIndex - bufferOffset;
      } else {
        // This is not the final slice (subtract 4 for the
        // forwarding address at the end of this new slice)
        limit = upto+newSize-4;
      }
    }

    public void readBytes(byte[] b, int offset, int len) {
      while(len > 0) {
        final int numLeft = limit-upto;
        if (numLeft < len) {
          // Read entire slice
          System.arraycopy(buffer, upto, b, offset, numLeft);
          offset += numLeft;
          len -= numLeft;
          nextSlice();
        } else {
          // This slice is the last one
          System.arraycopy(buffer, upto, b, offset, len);
          upto += len;
          break;
        }
      }
    }

    public long getFilePointer() {throw new RuntimeException("not implemented");}
    public long length() {throw new RuntimeException("not implemented");}
    public void seek(long pos) {throw new RuntimeException("not implemented");}
    public void close() {throw new RuntimeException("not implemented");}
  }

  // Size of each slice.  These arrays should be at most 16
  // elements.  First array is just a compact way to encode
  // X+1 with a max.  Second array is the length of each
  // slice, ie first slice is 5 bytes, next slice is 14
  // bytes, etc.
  final static int[] nextLevelArray = {1, 2, 3, 4, 5, 6, 7, 8, 9, 9};
  final static int[] levelSizeArray = {5, 14, 20, 30, 40, 40, 80, 80, 120, 200};

  /* Class that Posting and PostingVector use to write byte
   * streams into shared fixed-size byte[] arrays.  The idea
   * is to allocate slices of increasing lengths For
   * example, the first slice is 5 bytes, the next slice is
   * 14, etc.  We start by writing our bytes into the first
   * 5 bytes.  When we hit the end of the slice, we allocate
   * the next slice and then write the address of the new
   * slice into the last 4 bytes of the previous slice (the
   * "forwarding address").
   *
   * Each slice is filled with 0's initially, and we mark
   * the end with a non-zero byte.  This way the methods
   * that are writing into the slice don't need to record
   * its length and instead allocate a new slice once they
   * hit a non-zero byte. */
  private final class ByteBlockPool {

    public byte[][] buffers = new byte[10][];

    int bufferUpto = -1;                        // Which buffer we are upto
    public int byteUpto = BYTE_BLOCK_SIZE;             // Where we are in head buffer

    public byte[] buffer;                              // Current head buffer
    public int byteOffset = -BYTE_BLOCK_SIZE;          // Current head offset

    public void reset() {
      recycleByteBlocks(buffers, 1+bufferUpto);
      bufferUpto = -1;
      byteUpto = BYTE_BLOCK_SIZE;
      byteOffset = -BYTE_BLOCK_SIZE;
    }

    public void nextBuffer() {
      bufferUpto++;
      if (bufferUpto == buffers.length) {
        byte[][] newBuffers = new byte[(int) (bufferUpto*1.5)][];
        System.arraycopy(buffers, 0, newBuffers, 0, bufferUpto);
        buffers = newBuffers;
      }
      buffer = buffers[bufferUpto] = getByteBlock();
      Arrays.fill(buffer, (byte) 0);

      byteUpto = 0;
      byteOffset += BYTE_BLOCK_SIZE;
    }

    public int newSlice(final int size) {
      if (byteUpto > BYTE_BLOCK_SIZE-size)
        nextBuffer();
      final int upto = byteUpto;
      byteUpto += size;
      buffer[byteUpto-1] = 16;
      return upto;
    }

    public int allocSlice(final byte[] slice, final int upto) {

      final int level = slice[upto] & 15;
      final int newLevel = nextLevelArray[level];
      final int newSize = levelSizeArray[newLevel];

      // Maybe allocate another block
      if (byteUpto > BYTE_BLOCK_SIZE-newSize)
        nextBuffer();

      final int newUpto = byteUpto;
      final int offset = newUpto + byteOffset;
      byteUpto += newSize;

      // Copy forward the past 3 bytes (which we are about
      // to overwrite with the forwarding address):
      buffer[newUpto] = slice[upto-3];
      buffer[newUpto+1] = slice[upto-2];
      buffer[newUpto+2] = slice[upto-1];

      // Write forwarding address at end of last slice:
      slice[upto-3] = (byte) (offset >>> 24);
      slice[upto-2] = (byte) (offset >>> 16);
      slice[upto-1] = (byte) (offset >>> 8);
      slice[upto] = (byte) offset;
        
      // Write new level:
      buffer[byteUpto-1] = (byte) (16|newLevel);

      return newUpto+3;
    }
  }

  private final class CharBlockPool {

    public char[][] buffers = new char[10][];
    int numBuffer;

    int bufferUpto = -1;                        // Which buffer we are upto
    public int byteUpto = CHAR_BLOCK_SIZE;             // Where we are in head buffer

    public char[] buffer;                              // Current head buffer
    public int byteOffset = -CHAR_BLOCK_SIZE;          // Current head offset

    public void reset() {
      recycleCharBlocks(buffers, 1+bufferUpto);
      bufferUpto = -1;
      byteUpto = CHAR_BLOCK_SIZE;
      byteOffset = -CHAR_BLOCK_SIZE;
    }

    public void nextBuffer() {
      bufferUpto++;
      if (bufferUpto == buffers.length) {
        char[][] newBuffers = new char[(int) (bufferUpto*1.5)][];
        System.arraycopy(buffers, 0, newBuffers, 0, bufferUpto);
        buffers = newBuffers;
      }
      buffer = buffers[bufferUpto] = getCharBlock();

      byteUpto = 0;
      byteOffset += CHAR_BLOCK_SIZE;
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
  final private static int OBJECT_HEADER_NUM_BYTE = 8;

  final static int POSTING_NUM_BYTE = OBJECT_HEADER_NUM_BYTE + 9*INT_NUM_BYTE + POINTER_NUM_BYTE;

  // Holds free pool of Posting instances
  private Posting[] postingsFreeList;
  private int postingsFreeCount;

  /* Allocate more Postings from shared pool */
  private synchronized void getPostings(Posting[] postings) {
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
      numBytesAlloc += (postings.length - numToCopy) * POSTING_NUM_BYTE;
      balanceRAM();
      for(int i=numToCopy;i<postings.length;i++)
        postings[i] = new Posting();
    }
  }

  private synchronized void recyclePostings(Posting[] postings, int numPostings) {
    // Move all Postings from this ThreadState back to our
    // free list
    if (postingsFreeCount + numPostings > postingsFreeList.length) {
      final int newSize = (int) (1.25 * (postingsFreeCount + numPostings));
      Posting[] newArray = new Posting[newSize];
      System.arraycopy(postingsFreeList, 0, newArray, 0, postingsFreeCount);
      postingsFreeList = newArray;
    }
    System.arraycopy(postings, 0, postingsFreeList, postingsFreeCount, numPostings);
    postingsFreeCount += numPostings;
    numBytesUsed -= numPostings * POSTING_NUM_BYTE;
  }

  /* Initial chunks size of the shared byte[] blocks used to
     store postings data */
  final static int BYTE_BLOCK_SHIFT = 15;
  final static int BYTE_BLOCK_SIZE = (int) Math.pow(2.0, BYTE_BLOCK_SHIFT);
  final static int BYTE_BLOCK_MASK = BYTE_BLOCK_SIZE - 1;
  final static int BYTE_BLOCK_NOT_MASK = ~BYTE_BLOCK_MASK;

  private ArrayList freeByteBlocks = new ArrayList();

  /* Allocate another byte[] from the shared pool */
  synchronized byte[] getByteBlock() {
    final int size = freeByteBlocks.size();
    final byte[] b;
    if (0 == size) {
      numBytesAlloc += BYTE_BLOCK_SIZE;
      balanceRAM();
      b = new byte[BYTE_BLOCK_SIZE];
    } else
      b = (byte[]) freeByteBlocks.remove(size-1);
    numBytesUsed += BYTE_BLOCK_SIZE;
    return b;
  }

  /* Return a byte[] to the pool */
  synchronized void recycleByteBlocks(byte[][] blocks, int numBlocks) {
    for(int i=0;i<numBlocks;i++)
      freeByteBlocks.add(blocks[i]);
    numBytesUsed -= numBlocks * BYTE_BLOCK_SIZE;
  }

  /* Initial chunk size of the shared char[] blocks used to
     store term text */
  final static int CHAR_BLOCK_SHIFT = 14;
  final static int CHAR_BLOCK_SIZE = (int) Math.pow(2.0, CHAR_BLOCK_SHIFT);
  final static int CHAR_BLOCK_MASK = CHAR_BLOCK_SIZE - 1;

  private ArrayList freeCharBlocks = new ArrayList();

  /* Allocate another char[] from the shared pool */
  synchronized char[] getCharBlock() {
    final int size = freeCharBlocks.size();
    final char[] c;
    if (0 == size) {
      numBytesAlloc += CHAR_BLOCK_SIZE * CHAR_NUM_BYTE;
      balanceRAM();
      c = new char[BYTE_BLOCK_SIZE];
    } else
      c = (char[]) freeCharBlocks.remove(size-1);
    numBytesUsed += CHAR_BLOCK_SIZE * CHAR_NUM_BYTE;
    return c;
  }

  /* Return a char[] to the pool */
  synchronized void recycleCharBlocks(char[][] blocks, int numBlocks) {
    for(int i=0;i<numBlocks;i++)
      freeCharBlocks.add(blocks[i]);
    numBytesUsed -= numBlocks * CHAR_BLOCK_SIZE * CHAR_NUM_BYTE;
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
  private synchronized void balanceRAM() {

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
        infoStream.println("  RAM: now balance allocations: usedMB=" + toMB(numBytesUsed) +
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
            infoStream.println("    nothing to free; now set bufferIsFull");
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
          numBytesAlloc -= numToFree * POSTING_NUM_BYTE;
        }

        iter++;
      }
      
      if (infoStream != null)
        infoStream.println("    after free: freedMB=" + nf.format((startBytesAlloc-numBytesAlloc)/1024./1024.) + " usedMB=" + nf.format(numBytesUsed/1024./1024.) + " allocMB=" + nf.format(numBytesAlloc/1024./1024.));
      
    } else {
      // If we have not crossed the 100% mark, but have
      // crossed the 95% mark of RAM we are actually
      // using, go ahead and flush.  This prevents
      // over-allocating and then freeing, with every
      // flush.
      if (numBytesUsed > flushTrigger) {
        if (infoStream != null)
          infoStream.println("  RAM: now flush @ usedMB=" + nf.format(numBytesUsed/1024./1024.) +
                             " allocMB=" + nf.format(numBytesAlloc/1024./1024.) +
                             " triggerMB=" + nf.format(flushTrigger/1024./1024.));

        bufferIsFull = true;
      }
    }
  }

  /* Used to track postings for a single term.  One of these
   * exists per unique term seen since the last flush. */
  private final static class Posting {
    int textStart;                                  // Address into char[] blocks where our text is stored
    int docFreq;                                    // # times this term occurs in the current doc
    int freqStart;                                  // Address of first byte[] slice for freq
    int freqUpto;                                   // Next write address for freq
    int proxStart;                                  // Address of first byte[] slice
    int proxUpto;                                   // Next write address for prox
    int lastDocID;                                  // Last docID where this term occurred
    int lastDocCode;                                // Code for prior doc
    int lastPosition;                               // Last position where this term occurred
    PostingVector vector;                           // Corresponding PostingVector instance
  }

  /* Used to track data for term vectors.  One of these
   * exists per unique term seen in each field in the
   * document. */
  private final static class PostingVector {
    Posting p;                                      // Corresponding Posting instance for this term
    int lastOffset;                                 // Last offset we saw
    int offsetStart;                                // Address of first slice for offsets
    int offsetUpto;                                 // Next write address for offsets
    int posStart;                                   // Address of first slice for positions
    int posUpto;                                    // Next write address for positions
  }
}
