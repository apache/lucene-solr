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

import java.util.Map;
import java.io.IOException;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.ArrayUtil;

/** This is a DocFieldConsumer that writes stored fields. */
final class StoredFieldsWriter extends DocFieldConsumer {

  FieldsWriter fieldsWriter;
  final DocumentsWriter docWriter;
  int lastDocID;

  PerDoc[] docFreeList = new PerDoc[1];
  int freeCount;

  public StoredFieldsWriter(DocumentsWriter docWriter) {
    this.docWriter = docWriter;
  }

  public DocFieldConsumerPerThread addThread(DocFieldProcessorPerThread docFieldProcessorPerThread) throws IOException {
    return new StoredFieldsWriterPerThread(docFieldProcessorPerThread, this);
  }

  synchronized public void flush(Map threadsAndFields, DocumentsWriter.FlushState state) throws IOException {

    if (state.numDocsInStore > 0) {
      // It's possible that all documents seen in this segment
      // hit non-aborting exceptions, in which case we will
      // not have yet init'd the FieldsWriter:
      initFieldsWriter();

      // Fill fdx file to include any final docs that we
      // skipped because they hit non-aborting exceptions
      fill(state.numDocsInStore - docWriter.getDocStoreOffset());
    }

    if (fieldsWriter != null)
      fieldsWriter.flush();
  }
  
  private void initFieldsWriter() throws IOException {
    if (fieldsWriter == null) {
      final String docStoreSegment = docWriter.getDocStoreSegment();
      if (docStoreSegment != null) {
        assert docStoreSegment != null;
        fieldsWriter = new FieldsWriter(docWriter.directory,
                                        docStoreSegment,
                                        fieldInfos);
        docWriter.addOpenFile(docStoreSegment + "." + IndexFileNames.FIELDS_EXTENSION);
        docWriter.addOpenFile(docStoreSegment + "." + IndexFileNames.FIELDS_INDEX_EXTENSION);
        lastDocID = 0;
      }
    }
  }

  synchronized public void closeDocStore(DocumentsWriter.FlushState state) throws IOException {
    final int inc = state.numDocsInStore - lastDocID;
    if (inc > 0) {
      initFieldsWriter();
      fill(state.numDocsInStore - docWriter.getDocStoreOffset());
    }

    if (fieldsWriter != null) {
      fieldsWriter.close();
      fieldsWriter = null;
      lastDocID = 0;
      assert state.docStoreSegmentName != null;
      state.flushedFiles.add(state.docStoreSegmentName + "." + IndexFileNames.FIELDS_EXTENSION);
      state.flushedFiles.add(state.docStoreSegmentName + "." + IndexFileNames.FIELDS_INDEX_EXTENSION);

      state.docWriter.removeOpenFile(state.docStoreSegmentName + "." + IndexFileNames.FIELDS_EXTENSION);
      state.docWriter.removeOpenFile(state.docStoreSegmentName + "." + IndexFileNames.FIELDS_INDEX_EXTENSION);

      if (4+((long) state.numDocsInStore)*8 != state.directory.fileLength(state.docStoreSegmentName + "." + IndexFileNames.FIELDS_INDEX_EXTENSION))
        throw new RuntimeException("after flush: fdx size mismatch: " + state.numDocsInStore + " docs vs " + state.directory.fileLength(state.docStoreSegmentName + "." + IndexFileNames.FIELDS_INDEX_EXTENSION) + " length in bytes of " + state.docStoreSegmentName + "." + IndexFileNames.FIELDS_INDEX_EXTENSION);
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
        docFreeList = new PerDoc[ArrayUtil.getNextSize(allocCount)];
      }
      return new PerDoc();
    } else
      return docFreeList[--freeCount];
  }

  synchronized void abort() {
    if (fieldsWriter != null) {
      try {
        fieldsWriter.close();
      } catch (Throwable t) {
      }
      fieldsWriter = null;
      lastDocID = 0;
    }
  }

  /** Fills in any hole in the docIDs */
  void fill(int docID) throws IOException {
    final int docStoreOffset = docWriter.getDocStoreOffset();

    // We must "catch up" for all docs before us
    // that had no stored fields:
    final int end = docID+docStoreOffset;
    while(lastDocID < end) {
      fieldsWriter.skipDocument();
      lastDocID++;
    }
  }

  synchronized void finishDocument(PerDoc perDoc) throws IOException {
    assert docWriter.writer.testPoint("StoredFieldsWriter.finishDocument start");
    initFieldsWriter();

    fill(perDoc.docID);

    // Append stored fields to the real FieldsWriter:
    fieldsWriter.flushDocument(perDoc.numStoredFields, perDoc.fdt);
    lastDocID++;
    perDoc.reset();
    free(perDoc);
    assert docWriter.writer.testPoint("StoredFieldsWriter.finishDocument end");
  }

  public boolean freeRAM() {
    return false;
  }

  synchronized void free(PerDoc perDoc) {
    assert freeCount < docFreeList.length;
    assert 0 == perDoc.numStoredFields;
    assert 0 == perDoc.fdt.length();
    assert 0 == perDoc.fdt.getFilePointer();
    docFreeList[freeCount++] = perDoc;
  }

  class PerDoc extends DocumentsWriter.DocWriter {

    // TODO: use something more memory efficient; for small
    // docs the 1024 buffer size of RAMOutputStream wastes alot
    RAMOutputStream fdt = new RAMOutputStream();
    int numStoredFields;

    void reset() {
      fdt.reset();
      numStoredFields = 0;
    }

    void abort() {
      reset();
      free(this);
    }

    public long sizeInBytes() {
      return fdt.sizeInBytes();
    }

    public void finish() throws IOException {
      finishDocument(this);
    }
  }
}
