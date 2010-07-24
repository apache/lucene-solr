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

import org.apache.lucene.document.Fieldable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;

/** This is a DocFieldConsumer that writes stored fields. */
final class StoredFieldsWriter {

  FieldsWriter fieldsWriter;
  final DocumentsWriterPerThread docWriter;
  final FieldInfos fieldInfos;
  int lastDocID;
  private String segment;

  int freeCount;
  
  final DocumentsWriterPerThread.DocState docState;

  public StoredFieldsWriter(DocumentsWriterPerThread docWriter, FieldInfos fieldInfos) {
    this.docWriter = docWriter;
    this.fieldInfos = fieldInfos;
    this.docState = docWriter.docState;
  }
  
  private int numStoredFields;
  private Fieldable[] storedFields;
  private int[] fieldNumbers;

  public void reset() {
    numStoredFields = 0;
    storedFields = new Fieldable[1];
    fieldNumbers = new int[1];
  }

  public void startDocument() {
    reset();
  }
  
  public void flush(SegmentWriteState state) throws IOException {

    if (state.numDocs > 0) {
      // It's possible that all documents seen in this segment
      // hit non-aborting exceptions, in which case we will
      // not have yet init'd the FieldsWriter:
      initFieldsWriter();

      // Fill fdx file to include any final docs that we
      // skipped because they hit non-aborting exceptions
      fill(state.numDocs);
    }

    if (fieldsWriter != null) {
      fieldsWriter.flush();
      fieldsWriter.close();
      fieldsWriter = null;
      lastDocID = 0;
      String fieldsName = IndexFileNames.segmentFileName(state.segmentName, "", IndexFileNames.FIELDS_EXTENSION);
      String fieldsIdxName = IndexFileNames.segmentFileName(state.segmentName, "", IndexFileNames.FIELDS_INDEX_EXTENSION);
      state.flushedFiles.add(fieldsName);
      state.flushedFiles.add(fieldsIdxName);

      docWriter.removeOpenFile(fieldsName);
      docWriter.removeOpenFile(fieldsIdxName);

      if (4+((long) state.numDocs)*8 != state.directory.fileLength(fieldsIdxName)) {
        throw new RuntimeException("after flush: fdx size mismatch: " + state.numDocs + " docs vs " + state.directory.fileLength(fieldsIdxName) + " length in bytes of " + fieldsIdxName + " file exists?=" + state.directory.fileExists(fieldsIdxName));
      }
    }

  }

  private void initFieldsWriter() throws IOException {
    if (fieldsWriter == null) {
      segment = docWriter.getSegment();
      if (segment != null) {
        fieldsWriter = new FieldsWriter(docWriter.directory,
                                        segment,
                                        fieldInfos);
        docWriter.addOpenFile(IndexFileNames.segmentFileName(segment, "", IndexFileNames.FIELDS_EXTENSION));
        docWriter.addOpenFile(IndexFileNames.segmentFileName(segment, "", IndexFileNames.FIELDS_INDEX_EXTENSION));
        lastDocID = 0;
      }
    }
  }

  int allocCount;

  void abort() {
    reset();

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
    // We must "catch up" for all docs before us
    // that had no stored fields:
    final int end = docID;
    while(lastDocID < end) {
      fieldsWriter.skipDocument();
      lastDocID++;
    }
  }

  void finishDocument() throws IOException {
    assert docWriter.writer.testPoint("StoredFieldsWriter.finishDocument start");

    initFieldsWriter();
    fill(docState.docID);
  
    if (fieldsWriter != null && numStoredFields > 0) {  
      fieldsWriter.startDocument(numStoredFields);
      for (int i = 0; i < numStoredFields; i++) {
        fieldsWriter.writeField(fieldNumbers[i], storedFields[i]);
      }
      lastDocID++;
    }
    
    reset();
    assert docWriter.writer.testPoint("StoredFieldsWriter.finishDocument end");
  }

  
  public void addField(Fieldable field, FieldInfo fieldInfo) throws IOException {
    if (numStoredFields == storedFields.length) {
      int newSize = ArrayUtil.oversize(numStoredFields + 1, RamUsageEstimator.NUM_BYTES_OBJ_REF);
      Fieldable[] newArray = new Fieldable[newSize];
      System.arraycopy(storedFields, 0, newArray, 0, numStoredFields);
      storedFields = newArray;
    }
    
    if (numStoredFields == fieldNumbers.length) {
      fieldNumbers = ArrayUtil.grow(fieldNumbers);
    }
    
    storedFields[numStoredFields] = field;
    fieldNumbers[numStoredFields] = fieldInfo.number;
    numStoredFields++;
    
    assert docState.testPoint("StoredFieldsWriterPerThread.processFields.writeField");
  }
}
