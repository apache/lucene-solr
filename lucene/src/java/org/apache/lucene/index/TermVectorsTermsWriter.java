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

import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.IOException;
import java.util.Collection;

import java.util.Map;

final class TermVectorsTermsWriter extends TermsHashConsumer {

  final DocumentsWriter docWriter;
  TermVectorsWriter termVectorsWriter;
  PerDoc[] docFreeList = new PerDoc[1];
  int freeCount;
  IndexOutput tvx;
  IndexOutput tvd;
  IndexOutput tvf;
  int lastDocID;

  public TermVectorsTermsWriter(DocumentsWriter docWriter) {
    this.docWriter = docWriter;
  }

  @Override
  public TermsHashConsumerPerThread addThread(TermsHashPerThread termsHashPerThread) {
    return new TermVectorsTermsWriterPerThread(termsHashPerThread, this);
  }

  @Override
  synchronized void flush(Map<TermsHashConsumerPerThread,Collection<TermsHashConsumerPerField>> threadsAndFields, final SegmentWriteState state) throws IOException {

    // NOTE: it's possible that all documents seen in this segment
    // hit non-aborting exceptions, in which case we will
    // not have yet init'd the TermVectorsWriter.  This is
    // actually OK (unlike in the stored fields case)
    // because, although FieldInfos.hasVectors() will return
    // true, the TermVectorsReader gracefully handles
    // non-existence of the term vectors files.

    if (tvx != null) {

      if (state.numDocsInStore > 0)
        // In case there are some final documents that we
        // didn't see (because they hit a non-aborting exception):
        fill(state.numDocsInStore - docWriter.getDocStoreOffset());

      tvx.flush();
      tvd.flush();
      tvf.flush();
    }

    for (Map.Entry<TermsHashConsumerPerThread,Collection<TermsHashConsumerPerField>> entry : threadsAndFields.entrySet()) {
      for (final TermsHashConsumerPerField field : entry.getValue() ) {
        TermVectorsTermsWriterPerField perField = (TermVectorsTermsWriterPerField) field;
        perField.termsHashPerField.reset();
        perField.shrinkHash();
      }

      TermVectorsTermsWriterPerThread perThread = (TermVectorsTermsWriterPerThread) entry.getKey();
      perThread.termsHashPerThread.reset(true);
    }
  }

  @Override
  synchronized void closeDocStore(final SegmentWriteState state) throws IOException {
    if (tvx != null) {
      // At least one doc in this run had term vectors
      // enabled
      fill(state.numDocsInStore - docWriter.getDocStoreOffset());
      tvx.close();
      tvf.close();
      tvd.close();
      tvx = null;
      assert state.docStoreSegmentName != null;
      String idxName = IndexFileNames.segmentFileName(state.docStoreSegmentName, IndexFileNames.VECTORS_INDEX_EXTENSION);
      if (4+((long) state.numDocsInStore)*16 != state.directory.fileLength(idxName))
        throw new RuntimeException("after flush: tvx size mismatch: " + state.numDocsInStore + " docs vs " + state.directory.fileLength(idxName) + " length in bytes of " + idxName + " file exists?=" + state.directory.fileExists(idxName));

      String fldName = IndexFileNames.segmentFileName(state.docStoreSegmentName, IndexFileNames.VECTORS_FIELDS_EXTENSION);
      String docName = IndexFileNames.segmentFileName(state.docStoreSegmentName, IndexFileNames.VECTORS_DOCUMENTS_EXTENSION);
      state.flushedFiles.add(idxName);
      state.flushedFiles.add(fldName);
      state.flushedFiles.add(docName);

      docWriter.removeOpenFile(idxName);
      docWriter.removeOpenFile(fldName);
      docWriter.removeOpenFile(docName);

      lastDocID = 0;
    }    
  }

  int allocCount;

  synchronized PerDoc getPerDoc() {
    if (freeCount == 0) {
      allocCount++;
      if (allocCount > docFreeList.length) {
        // Grow our free list up front to make sure we have
        // enough space to recycle all outstanding PerDoc
        // instances
        assert allocCount == 1+docFreeList.length;
        docFreeList = new PerDoc[ArrayUtil.oversize(allocCount, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
      }
      return new PerDoc();
    } else
      return docFreeList[--freeCount];
  }

  /** Fills in no-term-vectors for all docs we haven't seen
   *  since the last doc that had term vectors. */
  void fill(int docID) throws IOException {
    final int docStoreOffset = docWriter.getDocStoreOffset();
    final int end = docID+docStoreOffset;
    if (lastDocID < end) {
      final long tvfPosition = tvf.getFilePointer();
      while(lastDocID < end) {
        tvx.writeLong(tvd.getFilePointer());
        tvd.writeVInt(0);
        tvx.writeLong(tvfPosition);
        lastDocID++;
      }
    }
  }

  synchronized void initTermVectorsWriter() throws IOException {        
    if (tvx == null) {
      
      final String docStoreSegment = docWriter.getDocStoreSegment();

      if (docStoreSegment == null)
        return;

      // If we hit an exception while init'ing the term
      // vector output files, we must abort this segment
      // because those files will be in an unknown
      // state:
      String idxName = IndexFileNames.segmentFileName(docStoreSegment, IndexFileNames.VECTORS_INDEX_EXTENSION);
      String docName = IndexFileNames.segmentFileName(docStoreSegment, IndexFileNames.VECTORS_DOCUMENTS_EXTENSION);
      String fldName = IndexFileNames.segmentFileName(docStoreSegment, IndexFileNames.VECTORS_FIELDS_EXTENSION);
      tvx = docWriter.directory.createOutput(idxName);
      tvd = docWriter.directory.createOutput(docName);
      tvf = docWriter.directory.createOutput(fldName);
      
      tvx.writeInt(TermVectorsReader.FORMAT_CURRENT);
      tvd.writeInt(TermVectorsReader.FORMAT_CURRENT);
      tvf.writeInt(TermVectorsReader.FORMAT_CURRENT);

      docWriter.addOpenFile(idxName);
      docWriter.addOpenFile(fldName);
      docWriter.addOpenFile(docName);

      lastDocID = 0;
    }
  }

  synchronized void finishDocument(PerDoc perDoc) throws IOException {

    assert docWriter.writer.testPoint("TermVectorsTermsWriter.finishDocument start");

    initTermVectorsWriter();

    fill(perDoc.docID);

    // Append term vectors to the real outputs:
    tvx.writeLong(tvd.getFilePointer());
    tvx.writeLong(tvf.getFilePointer());
    tvd.writeVInt(perDoc.numVectorFields);
    if (perDoc.numVectorFields > 0) {
      for(int i=0;i<perDoc.numVectorFields;i++)
        tvd.writeVInt(perDoc.fieldNumbers[i]);
      assert 0 == perDoc.fieldPointers[0];
      long lastPos = perDoc.fieldPointers[0];
      for(int i=1;i<perDoc.numVectorFields;i++) {
        long pos = perDoc.fieldPointers[i];
        tvd.writeVLong(pos-lastPos);
        lastPos = pos;
      }
      perDoc.perDocTvf.writeTo(tvf);
      perDoc.numVectorFields = 0;
    }

    assert lastDocID == perDoc.docID + docWriter.getDocStoreOffset();

    lastDocID++;

    perDoc.reset();
    free(perDoc);
    assert docWriter.writer.testPoint("TermVectorsTermsWriter.finishDocument end");
  }

  public boolean freeRAM() {
    // We don't hold any state beyond one doc, so we don't
    // free persistent RAM here
    return false;
  }

  @Override
  public void abort() {
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
    lastDocID = 0;
  }

  synchronized void free(PerDoc doc) {
    assert freeCount < docFreeList.length;
    docFreeList[freeCount++] = doc;
  }

  class PerDoc extends DocumentsWriter.DocWriter {

    final DocumentsWriter.PerDocBuffer buffer = docWriter.newPerDocBuffer();
    RAMOutputStream perDocTvf = new RAMOutputStream(buffer);

    int numVectorFields;

    int[] fieldNumbers = new int[1];
    long[] fieldPointers = new long[1];

    void reset() {
      perDocTvf.reset();
      buffer.recycle();
      numVectorFields = 0;
    }

    @Override
    void abort() {
      reset();
      free(this);
    }

    void addField(final int fieldNumber) {
      if (numVectorFields == fieldNumbers.length) {
        fieldNumbers = ArrayUtil.grow(fieldNumbers);
      }
      if (numVectorFields == fieldPointers.length) {
        fieldPointers = ArrayUtil.grow(fieldPointers);
      }
      fieldNumbers[numVectorFields] = fieldNumber;
      fieldPointers[numVectorFields] = perDocTvf.getFilePointer();
      numVectorFields++;
    }

    @Override
    public long sizeInBytes() {
      return buffer.getSizeInBytes();
    }

    @Override
    public void finish() throws IOException {
      finishDocument(this);
    }
  }
}
