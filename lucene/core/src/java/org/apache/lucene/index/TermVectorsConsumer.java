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
package org.apache.lucene.index;


import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.TermVectorsWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntBlockPool;
import org.apache.lucene.util.RamUsageEstimator;

class TermVectorsConsumer extends TermsHash {
  protected final Directory directory;
  protected final SegmentInfo info;
  protected final Codec codec;
  TermVectorsWriter writer;

  /** Scratch term used by TermVectorsConsumerPerField.finishDocument. */
  final BytesRef flushTerm = new BytesRef();


  /** Used by TermVectorsConsumerPerField when serializing
   *  the term vectors. */
  final ByteSliceReader vectorSliceReaderPos = new ByteSliceReader();
  final ByteSliceReader vectorSliceReaderOff = new ByteSliceReader();

  boolean hasVectors;
  private int numVectorFields;
  int lastDocID;
  private TermVectorsConsumerPerField[] perFields = new TermVectorsConsumerPerField[1];
  // this accountable either holds the writer or one that returns null.
  // it's cleaner than checking if the writer is null all over the place
  Accountable accountable = Accountable.NULL_ACCOUNTABLE;

  TermVectorsConsumer(final IntBlockPool.Allocator intBlockAllocator, final ByteBlockPool.Allocator byteBlockAllocator, Directory directory, SegmentInfo info, Codec codec) {
    super(intBlockAllocator, byteBlockAllocator, Counter.newCounter(), null);
    this.directory = directory;
    this.info = info;
    this.codec = codec;
  }

  @Override
  void flush(Map<String, TermsHashPerField> fieldsToFlush, final SegmentWriteState state, Sorter.DocMap sortMap, NormsProducer norms) throws IOException {
    if (writer != null) {
      int numDocs = state.segmentInfo.maxDoc();
      assert numDocs > 0;
      // At least one doc in this run had term vectors enabled
      try {
        fill(numDocs);
        assert state.segmentInfo != null;
        writer.finish(state.fieldInfos, numDocs);
      } finally {
        IOUtils.close(writer);
      }
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

  void initTermVectorsWriter() throws IOException {
    if (writer == null) {
      IOContext context = new IOContext(new FlushInfo(lastDocID, bytesUsed.get()));
      writer = codec.termVectorsFormat().vectorsWriter(directory, info, context);
      lastDocID = 0;
      accountable = writer;
    }
  }

  @Override
  void finishDocument(int docID) throws IOException {

    if (!hasVectors) {
      return;
    }

    // Fields in term vectors are UTF16 sorted:
    ArrayUtil.introSort(perFields, 0, numVectorFields);

    initTermVectorsWriter();

    fill(docID);

    // Append term vectors to the real outputs:
    writer.startDocument(numVectorFields);
    for (int i = 0; i < numVectorFields; i++) {
      perFields[i].finishDocument();
    }
    writer.finishDocument();

    assert lastDocID == docID: "lastDocID=" + lastDocID + " docID=" + docID;

    lastDocID++;

    super.reset();
    resetFields();
  }

  @Override
  public void abort() {
    try {
      super.abort();
    } finally {
      IOUtils.closeWhileHandlingException(writer);
      reset();
    }
  }

  void resetFields() {
    Arrays.fill(perFields, null); // don't hang onto stuff from previous doc
    numVectorFields = 0;
  }

  @Override
  public TermsHashPerField addField(FieldInvertState invertState, FieldInfo fieldInfo) {
    return new TermVectorsConsumerPerField(invertState, this, fieldInfo);
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
    resetFields();
    numVectorFields = 0;
  }

}
