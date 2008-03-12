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
import java.util.List;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.analysis.Analyzer;

/** Used by DocumentsWriter to maintain per-thread state.
 *  We keep a separate Posting hash and other state for each
 *  thread and then merge postings hashes from all threads
 *  when writing the segment. */
final class DocumentsWriterThreadState {

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

  DocumentsWriterFieldData[] fieldDataArray;           // Fields touched by current doc
  int numFieldData;                     // How many fields in current doc
  int numVectorFields;                  // How many vector fields in current doc

  DocumentsWriterFieldData[] allFieldDataArray = new DocumentsWriterFieldData[10]; // All FieldData instances
  int numAllFieldData;
  DocumentsWriterFieldData[] fieldDataHash;            // Hash FieldData instances by field name
  int fieldDataHashMask;
  String maxTermPrefix;                 // Non-null prefix of a too-large term if this
  // doc has one

  boolean doFlushAfter;

  final DocumentsWriter docWriter;

  final ByteBlockPool postingsPool;
  final ByteBlockPool vectorsPool;
  final CharBlockPool charPool;

  public DocumentsWriterThreadState(DocumentsWriter docWriter) {
    this.docWriter = docWriter;
    fieldDataArray = new DocumentsWriterFieldData[8];

    fieldDataHash = new DocumentsWriterFieldData[16];
    fieldDataHashMask = 15;

    vectorFieldPointers = new long[10];
    vectorFieldNumbers = new int[10];
    postingsFreeList = new Posting[256];
    postingsFreeCount = 0;

    postingsPool = new ByteBlockPool(docWriter ,true);
    vectorsPool = new ByteBlockPool(docWriter, false);
    charPool = new CharBlockPool(docWriter);
  }

  /** Clear the postings hash and return objects back to
   *  shared pool */
  public void resetPostings() throws IOException {
    fieldGen = 0;
    maxPostingsVectors = 0;
    doFlushAfter = false;
    if (localFieldsWriter != null) {
      localFieldsWriter.close();
      localFieldsWriter = null;
    }
    postingsPool.reset();
    charPool.reset();
    docWriter.recyclePostings(postingsFreeList, postingsFreeCount);
    postingsFreeCount = 0;
    for(int i=0;i<numAllFieldData;i++) {
      DocumentsWriterFieldData fp = allFieldDataArray[i];
      fp.lastGen = -1;
      if (fp.numPostings > 0)
        fp.resetPostingArrays();
    }
  }

  /** Move all per-document state that was accumulated in
   *  the ThreadState into the "real" stores. */
  public void writeDocument() throws IOException, AbortException {

    // If we hit an exception while appending to the
    // stored fields or term vectors files, we have to
    // abort all documents since we last flushed because
    // it means those files are possibly inconsistent.
    try {

      docWriter.numDocsInStore++;

      // Append stored fields to the real FieldsWriter:
      docWriter.fieldsWriter.flushDocument(numStoredFields, fdtLocal);
      fdtLocal.reset();

      // Append term vectors to the real outputs:
      final IndexOutput tvx = docWriter.tvx;
      final IndexOutput tvd = docWriter.tvd;
      final IndexOutput tvf = docWriter.tvf;
      if (tvx != null) {
        tvx.writeLong(tvd.getFilePointer());
        tvx.writeLong(tvf.getFilePointer());
        tvd.writeVInt(numVectorFields);
        if (numVectorFields > 0) {
          for(int i=0;i<numVectorFields;i++)
            tvd.writeVInt(vectorFieldNumbers[i]);
          assert 0 == vectorFieldPointers[0];
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
        DocumentsWriterFieldData fp = fieldDataArray[i];
        if (fp.doNorms) {
          BufferedNorms bn = docWriter.norms[fp.fieldInfo.number];
          assert bn != null;
          assert bn.upto <= docID;
          bn.fill(docID);
          float norm = fp.boost * docWriter.writer.getSimilarity().lengthNorm(fp.fieldInfo.name, fp.length);
          bn.add(norm);
        }
      }
    } catch (Throwable t) {
      // Forcefully idle this threadstate -- its state will
      // be reset by abort()
      isIdle = true;
      throw new AbortException(t, docWriter);
    }

    if (docWriter.bufferIsFull && !docWriter.flushPending) {
      docWriter.flushPending = true;
      doFlushAfter = true;
    }
  }

  int fieldGen;

  /** Initializes shared state for this new document */
  void init(Document doc, int docID) throws IOException, AbortException {

    assert !isIdle;
    assert docWriter.writer.testPoint("DocumentsWriter.ThreadState.init start");

    this.docID = docID;
    docBoost = doc.getBoost();
    numStoredFields = 0;
    numFieldData = 0;
    numVectorFields = 0;
    maxTermPrefix = null;

    assert 0 == fdtLocal.length();
    assert 0 == fdtLocal.getFilePointer();
    assert 0 == tvfLocal.length();
    assert 0 == tvfLocal.getFilePointer();
    final int thisFieldGen = fieldGen++;

    List docFields = doc.getFields();
    final int numDocFields = docFields.size();
    boolean docHasVectors = false;

    // Absorb any new fields first seen in this document.
    // Also absorb any changes to fields we had already
    // seen before (eg suddenly turning on norms or
    // vectors, etc.):

    for(int i=0;i<numDocFields;i++) {
      Fieldable field = (Fieldable) docFields.get(i);

      FieldInfo fi = docWriter.fieldInfos.add(field.name(), field.isIndexed(), field.isTermVectorStored(),
                                              field.isStorePositionWithTermVector(), field.isStoreOffsetWithTermVector(),
                                              field.getOmitNorms(), false);
      if (fi.isIndexed && !fi.omitNorms) {
        // Maybe grow our buffered norms
        if (docWriter.norms.length <= fi.number) {
          int newSize = (int) ((1+fi.number)*1.25);
          BufferedNorms[] newNorms = new BufferedNorms[newSize];
          System.arraycopy(docWriter.norms, 0, newNorms, 0, docWriter.norms.length);
          docWriter.norms = newNorms;
        }
          
        if (docWriter.norms[fi.number] == null)
          docWriter.norms[fi.number] = new BufferedNorms();

        docWriter.hasNorms = true;
      }

      // Make sure we have a FieldData allocated
      int hashPos = fi.name.hashCode() & fieldDataHashMask;
      DocumentsWriterFieldData fp = fieldDataHash[hashPos];
      while(fp != null && !fp.fieldInfo.name.equals(fi.name))
        fp = fp.next;

      if (fp == null) {

        fp = new DocumentsWriterFieldData(this, fi);
        fp.next = fieldDataHash[hashPos];
        fieldDataHash[hashPos] = fp;

        if (numAllFieldData == allFieldDataArray.length) {
          int newSize = (int) (allFieldDataArray.length*1.5);
          int newHashSize = fieldDataHash.length*2;

          DocumentsWriterFieldData newArray[] = new DocumentsWriterFieldData[newSize];
          DocumentsWriterFieldData newHashArray[] = new DocumentsWriterFieldData[newHashSize];
          System.arraycopy(allFieldDataArray, 0, newArray, 0, numAllFieldData);

          // Rehash
          fieldDataHashMask = newSize-1;
          for(int j=0;j<fieldDataHash.length;j++) {
            DocumentsWriterFieldData fp0 = fieldDataHash[j];
            while(fp0 != null) {
              hashPos = fp0.fieldInfo.name.hashCode() & fieldDataHashMask;
              DocumentsWriterFieldData nextFP0 = fp0.next;
              fp0.next = newHashArray[hashPos];
              newHashArray[hashPos] = fp0;
              fp0 = nextFP0;
            }
          }

          allFieldDataArray = newArray;
          fieldDataHash = newHashArray;
        }
        allFieldDataArray[numAllFieldData++] = fp;
      } else {
        assert fp.fieldInfo == fi;
      }

      if (thisFieldGen != fp.lastGen) {

        // First time we're seeing this field for this doc
        fp.lastGen = thisFieldGen;
        fp.fieldCount = 0;
        fp.doVectors = fp.doVectorPositions = fp.doVectorOffsets = false;
        fp.doNorms = fi.isIndexed && !fi.omitNorms;

        if (numFieldData == fieldDataArray.length) {
          int newSize = fieldDataArray.length*2;
          DocumentsWriterFieldData newArray[] = new DocumentsWriterFieldData[newSize];
          System.arraycopy(fieldDataArray, 0, newArray, 0, numFieldData);
          fieldDataArray = newArray;

        }
        fieldDataArray[numFieldData++] = fp;
      }

      if (field.isTermVectorStored()) {
        if (!fp.doVectors && numVectorFields++ == vectorFieldPointers.length) {
          final int newSize = (int) (numVectorFields*1.5);
          vectorFieldPointers = new long[newSize];
          vectorFieldNumbers = new int[newSize];
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
      if (docWriter.fieldsWriter == null) {
        assert docWriter.docStoreSegment == null;
        assert docWriter.segment != null;
        docWriter.docStoreSegment = docWriter.segment;
        // If we hit an exception while init'ing the
        // fieldsWriter, we must abort this segment
        // because those files will be in an unknown
        // state:
        try {
          docWriter.fieldsWriter = new FieldsWriter(docWriter.directory, docWriter.docStoreSegment, docWriter.fieldInfos);
        } catch (Throwable t) {
          throw new AbortException(t, docWriter);
        }
        docWriter.files = null;
      }
      localFieldsWriter = new FieldsWriter(null, fdtLocal, docWriter.fieldInfos);
    }

    // First time we see a doc that has field(s) with
    // stored vectors, we init our tvx writer
    if (docHasVectors) {
      if (docWriter.tvx == null) {
        assert docWriter.docStoreSegment != null;
        // If we hit an exception while init'ing the term
        // vector output files, we must abort this segment
        // because those files will be in an unknown
        // state:
        try {
          docWriter.tvx = docWriter.directory.createOutput(docWriter.docStoreSegment + "." + IndexFileNames.VECTORS_INDEX_EXTENSION);
          docWriter.tvx.writeInt(TermVectorsReader.FORMAT_VERSION2);
          docWriter.tvd = docWriter.directory.createOutput(docWriter.docStoreSegment +  "." + IndexFileNames.VECTORS_DOCUMENTS_EXTENSION);
          docWriter.tvd.writeInt(TermVectorsReader.FORMAT_VERSION2);
          docWriter.tvf = docWriter.directory.createOutput(docWriter.docStoreSegment +  "." + IndexFileNames.VECTORS_FIELDS_EXTENSION);
          docWriter.tvf.writeInt(TermVectorsReader.FORMAT_VERSION2);

          // We must "catch up" for all docs before us
          // that had no vectors:
          for(int i=0;i<docWriter.numDocsInStore;i++) {
            docWriter.tvx.writeLong(docWriter.tvd.getFilePointer());
            docWriter.tvd.writeVInt(0);
            docWriter.tvx.writeLong(0);
          }
        } catch (Throwable t) {
          throw new AbortException(t, docWriter);
        }
        docWriter.files = null;
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
    else if (hi == 1+lo) {
      if (comparePostings(postings[lo], postings[hi]) > 0) {
        final Posting tmp = postings[lo];
        postings[lo] = postings[hi];
        postings[hi] = tmp;
      }
      return;
    }

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
    else if (hi == 1+lo) {
      if (comparePostings(postings[lo].p, postings[hi].p) > 0) {
        final PostingVector tmp = postings[lo];
        postings[lo] = postings[hi];
        postings[hi] = tmp;
      }
      return;
    }

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

  void quickSort(DocumentsWriterFieldData[] array, int lo, int hi) {
    if (lo >= hi)
      return;
    else if (hi == 1+lo) {
      if (array[lo].compareTo(array[hi]) > 0) {
        final DocumentsWriterFieldData tmp = array[lo];
        array[lo] = array[hi];
        array[hi] = tmp;
      }
      return;
    }

    int mid = (lo + hi) >>> 1;

    if (array[lo].compareTo(array[mid]) > 0) {
      DocumentsWriterFieldData tmp = array[lo];
      array[lo] = array[mid];
      array[mid] = tmp;
    }

    if (array[mid].compareTo(array[hi]) > 0) {
      DocumentsWriterFieldData tmp = array[mid];
      array[mid] = array[hi];
      array[hi] = tmp;

      if (array[lo].compareTo(array[mid]) > 0) {
        DocumentsWriterFieldData tmp2 = array[lo];
        array[lo] = array[mid];
        array[mid] = tmp2;
      }
    }

    int left = lo + 1;
    int right = hi - 1;

    if (left >= right)
      return;

    DocumentsWriterFieldData partition = array[mid];

    for (; ;) {
      while (array[right].compareTo(partition) > 0)
        --right;

      while (left < right && array[left].compareTo(partition) <= 0)
        ++left;

      if (left < right) {
        DocumentsWriterFieldData tmp = array[left];
        array[left] = array[right];
        array[right] = tmp;
        --right;
      } else {
        break;
      }
    }

    quickSort(array, lo, left);
    quickSort(array, left + 1, hi);
  }

  /** If there are fields we've seen but did not see again
   *  in the last run, then free them up.  Also reduce
   *  postings hash size. */
  void trimFields() {

    int upto = 0;
    for(int i=0;i<numAllFieldData;i++) {
      DocumentsWriterFieldData fp = allFieldDataArray[i];
      if (fp.lastGen == -1) {
        // This field was not seen since the previous
        // flush, so, free up its resources now

        // Unhash
        final int hashPos = fp.fieldInfo.name.hashCode() & fieldDataHashMask;
        DocumentsWriterFieldData last = null;
        DocumentsWriterFieldData fp0 = fieldDataHash[hashPos];
        while(fp0 != fp) {
          last = fp0;
          fp0 = fp0.next;
        }
        assert fp0 != null;

        if (last == null)
          fieldDataHash[hashPos] = fp.next;
        else
          last.next = fp.next;

        if (docWriter.infoStream != null)
          docWriter.infoStream.println("  remove field=" + fp.fieldInfo.name);

      } else {
        // Reset
        fp.lastGen = -1;
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
    for(int i=0;i<docWriter.norms.length;i++) {
      BufferedNorms n = docWriter.norms[i];
      if (n != null && n.upto == 0)
        docWriter.norms[i] = null;
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
    throws IOException, AbortException {

    final int numFields = numFieldData;
    assert clearLastVectorFieldName();

    assert 0 == fdtLocal.length();

    if (docWriter.tvx != null)
      // If we are writing vectors then we must visit
      // fields in sorted order so they are written in
      // sorted order.  TODO: we actually only need to
      // sort the subset of fields that have vectors
      // enabled; we could save [small amount of] CPU
      // here.
      quickSort(fieldDataArray, 0, numFields-1);

    // We process the document one field at a time
    for(int i=0;i<numFields;i++)
      fieldDataArray[i].processField(analyzer);

    if (docWriter.infoStream != null && maxTermPrefix != null)
      docWriter.infoStream.println("WARNING: document contains at least one immense term (longer than the max length " + DocumentsWriter.MAX_TERM_LENGTH + "), all of which were skipped.  Please correct the analyzer to not produce such terms.  The prefix of the first immense term is: '" + maxTermPrefix + "...'"); 
  }

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

  /** Compares term text for two Posting instance and
   *  returns -1 if p1 < p2; 1 if p1 > p2; else 0.
   */
  int comparePostings(Posting p1, Posting p2) {
    if (p1 == p2)
      return 0;
    final char[] text1 = charPool.buffers[p1.textStart >> DocumentsWriter.CHAR_BLOCK_SHIFT];
    int pos1 = p1.textStart & DocumentsWriter.CHAR_BLOCK_MASK;
    final char[] text2 = charPool.buffers[p2.textStart >> DocumentsWriter.CHAR_BLOCK_SHIFT];
    int pos2 = p2.textStart & DocumentsWriter.CHAR_BLOCK_MASK;
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

  String lastVectorFieldName;

  // Called only by assert
  final boolean clearLastVectorFieldName() {
    lastVectorFieldName = null;
    return true;
  }

  // Called only by assert
  final boolean vectorFieldsInOrder(FieldInfo fi) {
    try {
      if (lastVectorFieldName != null)
        return lastVectorFieldName.compareTo(fi.name) < 0;
      else
        return true;
    } finally {
      lastVectorFieldName = fi.name;
    }
  }

  PostingVector[] postingsVectors = new PostingVector[1];
  int maxPostingsVectors;

  // Used to read a string value for a field
  ReusableStringReader stringReader = new ReusableStringReader();
}

