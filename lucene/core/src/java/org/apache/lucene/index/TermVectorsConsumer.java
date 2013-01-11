package org.apache.lucene.index;

/*
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
import java.util.Map;

import org.apache.lucene.codecs.TermVectorsWriter;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;

final class TermVectorsConsumer extends TermsHashConsumer {

  TermVectorsWriter writer;
  final DocumentsWriterPerThread docWriter;
  int freeCount;
  int lastDocID;

  final DocumentsWriterPerThread.DocState docState;
  final BytesRef flushTerm = new BytesRef();

  // Used by perField when serializing the term vectors
  final ByteSliceReader vectorSliceReaderPos = new ByteSliceReader();
  final ByteSliceReader vectorSliceReaderOff = new ByteSliceReader();
  boolean hasVectors;

  public TermVectorsConsumer(DocumentsWriterPerThread docWriter) {
    this.docWriter = docWriter;
    docState = docWriter.docState;
  }

  @Override
  void flush(Map<String, TermsHashConsumerPerField> fieldsToFlush, final SegmentWriteState state) throws IOException {
    if (writer != null) {
      int numDocs = state.segmentInfo.getDocCount();
      // At least one doc in this run had term vectors enabled
      try {
        fill(numDocs);
        assert state.segmentInfo != null;
        writer.finish(state.fieldInfos, numDocs);
      } finally {
        IOUtils.close(writer);
        writer = null;

        lastDocID = 0;
        hasVectors = false;
      }
    }

    for (final TermsHashConsumerPerField field : fieldsToFlush.values() ) {
      TermVectorsConsumerPerField perField = (TermVectorsConsumerPerField) field;
      perField.termsHashPerField.reset();
      perField.shrinkHash();
    }
  }

  /** Fills in no-term-vectors for all docs we haven't seen
   *  since the last doc that had term vectors. */
  void fill(int docID) throws IOException {
    while(lastDocID < docID) {
      writer.startDocument(0);
      writer.finishDocument();
      lastDocID++;
    }
  }

  private final void initTermVectorsWriter() throws IOException {
    if (writer == null) {
      IOContext context = new IOContext(new FlushInfo(docWriter.getNumDocsInRAM(), docWriter.bytesUsed()));
      writer = docWriter.codec.termVectorsFormat().vectorsWriter(docWriter.directory, docWriter.getSegmentInfo(), context);
      lastDocID = 0;
    }
  }

  @Override
  void finishDocument(TermsHash termsHash) throws IOException {

    assert docWriter.writer.testPoint("TermVectorsTermsWriter.finishDocument start");

    if (!hasVectors) {
      return;
    }

    initTermVectorsWriter();

    fill(docState.docID);

    // Append term vectors to the real outputs:
    writer.startDocument(numVectorFields);
    for (int i = 0; i < numVectorFields; i++) {
      perFields[i].finishDocument();
    }
    writer.finishDocument();

    assert lastDocID == docState.docID: "lastDocID=" + lastDocID + " docState.docID=" + docState.docID;

    lastDocID++;

    termsHash.reset();
    reset();
    assert docWriter.writer.testPoint("TermVectorsTermsWriter.finishDocument end");
  }

  @Override
  public void abort() {
    hasVectors = false;

    if (writer != null) {
      writer.abort();
      writer = null;
    }

    lastDocID = 0;

    reset();
  }

  int numVectorFields;

  TermVectorsConsumerPerField[] perFields;

  void reset() {
    numVectorFields = 0;
    perFields = new TermVectorsConsumerPerField[1];
  }

  @Override
  public TermsHashConsumerPerField addField(TermsHashPerField termsHashPerField, FieldInfo fieldInfo) {
    return new TermVectorsConsumerPerField(termsHashPerField, this, fieldInfo);
  }

  void addFieldToFlush(TermVectorsConsumerPerField fieldToFlush) {
    if (numVectorFields == perFields.length) {
      int newSize = ArrayUtil.oversize(numVectorFields + 1, RamUsageEstimator.NUM_BYTES_OBJECT_REF);
      TermVectorsConsumerPerField[] newArray = new TermVectorsConsumerPerField[newSize];
      System.arraycopy(perFields, 0, newArray, 0, numVectorFields);
      perFields = newArray;
    }

    perFields[numVectorFields++] = fieldToFlush;
  }

  @Override
  void startDocument() {
    assert clearLastVectorFieldName();
    reset();
  }

  // Called only by assert
  final boolean clearLastVectorFieldName() {
    lastVectorFieldName = null;
    return true;
  }

  // Called only by assert
  String lastVectorFieldName;
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

}
