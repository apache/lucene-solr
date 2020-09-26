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

package org.apache.lucene.codecs.lucene90;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.FloatBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.VectorReader;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

/**
 * Reads vectors from the index segments.
 */
public final class Lucene90VectorReader extends VectorReader {

  private final FieldInfos fieldInfos;
  private final Map<String, FieldEntry> fields = new HashMap<>();
  private final IndexInput vectorData;
  private final int maxDoc;

  Lucene90VectorReader(SegmentReadState state) throws IOException {
    this.fieldInfos = state.fieldInfos;
    this.maxDoc = state.segmentInfo.maxDoc();

    String metaFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, Lucene90VectorFormat.META_EXTENSION);
    int versionMeta = -1;
    try (ChecksumIndexInput meta = state.directory.openChecksumInput(metaFileName, state.context)) {
      Throwable priorE = null;
      try {
        versionMeta = CodecUtil.checkIndexHeader(meta,
            Lucene90VectorFormat.META_CODEC_NAME,
            Lucene90VectorFormat.VERSION_START,
            Lucene90VectorFormat.VERSION_CURRENT,
            state.segmentInfo.getId(),
            state.segmentSuffix);
        readFields(meta, state.fieldInfos);
      } catch (Throwable exception) {
        priorE = exception;
      } finally {
        CodecUtil.checkFooter(meta, priorE);
      }
    }

    boolean success = false;

    String vectorDataFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, Lucene90VectorFormat.VECTOR_DATA_EXTENSION);
    this.vectorData = state.directory.openInput(vectorDataFileName, state.context);
    try {
      int versionVectorData = CodecUtil.checkIndexHeader(vectorData,
          Lucene90VectorFormat.VECTOR_DATA_CODEC_NAME,
          Lucene90VectorFormat.VERSION_START,
          Lucene90VectorFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      if (versionMeta != versionVectorData) {
        throw new CorruptIndexException("Format versions mismatch: meta=" + versionMeta + ", vector data=" + versionVectorData, vectorData);
      }
      CodecUtil.retrieveChecksum(vectorData);

      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(this.vectorData);
      }
    }
  }

  private void readFields(ChecksumIndexInput meta, FieldInfos infos) throws IOException {
    for (int fieldNumber = meta.readInt(); fieldNumber != -1; fieldNumber = meta.readInt()) {
      FieldInfo info = infos.fieldInfo(fieldNumber);
      if (info == null) {
        throw new CorruptIndexException("Invalid field number: " + fieldNumber, meta);
      }
      VectorValues.ScoreFunction scoreFunction = VectorValues.ScoreFunction.fromId(meta.readInt());
      long vectorDataOffset = meta.readVLong();
      long vectorDataLength = meta.readVLong();
      int dimension = meta.readInt();
      int size = meta.readInt();
      // TODO: don't box all these integers
      final Map<Integer, Integer> docToOrd = new HashMap<>();
      int[] ordToDoc = new int[size];
      for (int i = 0; i < size; i++) {
        int doc = meta.readVInt();
        docToOrd.put(doc, i);
        ordToDoc[i] = doc;
      }
      FieldEntry fieldEntry = new FieldEntry(dimension, scoreFunction, maxDoc, vectorDataOffset, vectorDataLength,
                                              docToOrd, ordToDoc);
      fields.put(info.name, fieldEntry);
    }
  }


  @Override
  public void checkIntegrity() throws IOException {
    // TODO
  }

  @Override
  public VectorValues getVectorValues(String field) throws IOException {

    FieldInfo info = fieldInfos.fieldInfo(field);
    if (info != null) {

      int numDims = info.getVectorDimension();
      if (numDims > 0) {

        final FieldEntry fieldEntry = fields.get(field);
        if (fieldEntry != null) {
          assert fieldEntry.dimension == numDims;
          long numBytes = (long) fieldEntry.size() * numDims * Float.BYTES;
          if (numBytes != fieldEntry.vectorDataLength) {
            throw new IllegalStateException("Vector data length " + fieldEntry.vectorDataLength +
                    " not matching size=" + fieldEntry.size() + " * dim=" + numDims + " * 4 = " +
                    numBytes);
          }
          IndexInput bytesSlice = vectorData.slice("vector-data", fieldEntry.vectorDataOffset, fieldEntry.vectorDataLength);
          return new RandomAccessVectorValues(fieldEntry, bytesSlice);
        }
      }
    }
    return VectorValues.EMPTY;
  }

  @Override
  public void close() throws IOException {
    vectorData.close();
  }

  @Override
  public long ramBytesUsed() {
    return 0;
  }

  private static class FieldEntry {

    final int dimension;
    final VectorValues.ScoreFunction scoreFunction;
    final int maxDoc;

    final long vectorDataOffset;
    final long vectorDataLength;
    final Map<Integer, Integer> docToOrd;
    final int[] ordToDoc;

    FieldEntry(int dimension, VectorValues.ScoreFunction scoreFunction, int maxDoc,
               long vectorDataOffset, long vectorDataLength,
               Map<Integer, Integer> docToOrd, int[] ordToDoc) {
      this.dimension = dimension;
      this.scoreFunction = scoreFunction;
      this.maxDoc = maxDoc;
      this.vectorDataOffset = vectorDataOffset;
      this.vectorDataLength = vectorDataLength;
      this.docToOrd = docToOrd;
      this.ordToDoc = ordToDoc;
    }

    int size() {
      return ordToDoc.length;
    }
  }

  /** Read the vector values from the index input. This allows random access to the underlying vectors data. */
  private final static class RandomAccessVectorValues extends VectorValues {

    final int byteSize;
    final FieldEntry fieldEntry;
    final IndexInput dataIn;
    final BytesRef binaryValue;
    final ByteBuffer byteBuffer;
    final FloatBuffer floatBuffer;
    final float[] value;

    int doc = -1;

    RandomAccessVectorValues(FieldEntry fieldEntry, IndexInput dataIn) {
      this.fieldEntry = fieldEntry;
      this.dataIn = dataIn;
      byteSize = Float.BYTES * fieldEntry.dimension;
      byteBuffer = ByteBuffer.allocate(byteSize);
      floatBuffer = byteBuffer.asFloatBuffer();
      value = new float[fieldEntry.dimension];
      binaryValue = new BytesRef(byteBuffer.array(), byteBuffer.arrayOffset(), byteSize);
    }

    @Override
    public VectorValues copy() {
      return new RandomAccessVectorValues(fieldEntry, dataIn.clone());
    }

    @Override
    public int dimension() {
      return fieldEntry.dimension;
    }

    @Override
    public int size() {
      return fieldEntry.size();
    }

    @Override
    public ScoreFunction scoreFunction() {
      return fieldEntry.scoreFunction;
    }

    @Override
    public float[] vectorValue() throws IOException {
      if (doc == NO_MORE_DOCS) {
        return null;
      }
      dataIn.readBytes(byteBuffer.array(), byteBuffer.arrayOffset(), byteSize);
      floatBuffer.position(0);
      floatBuffer.get(value, 0, fieldEntry.dimension);
      return value;
    }

    @Override
    public BytesRef binaryValue() throws IOException {
      dataIn.readBytes(byteBuffer.array(), byteBuffer.arrayOffset(), byteSize);
      return binaryValue;
    }

    @Override
    public float[] vectorValue(int targetOrd) throws IOException {
      long offset = targetOrd * byteSize;
      dataIn.seek(offset);
      dataIn.readBytes(byteBuffer.array(), byteBuffer.arrayOffset(), byteSize);
      floatBuffer.position(0);
      floatBuffer.get(value);
      return value;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() throws IOException {
      // TODO we can make this better
      return advance(doc + 1);
    }

    @Override
    public int advance(int target) throws IOException {
      // TODO use log-binary search to scan ahead in ordToDoc
      while (target <= fieldEntry.maxDoc) {
        int ord = fieldEntry.docToOrd.getOrDefault(target, -1);
        if (ord != -1) {
          dataIn.seek((long) ord * byteSize);
          doc = target;
          return doc;
        }
        ++target;
      }
      return doc = NO_MORE_DOCS;
    }

    @Override
    public long cost() {
      return fieldEntry.size();
    }

    @Override
    public TopDocs search(float[] vector, int topK, int fanout) throws IOException {
      throw new UnsupportedOperationException();
    }
  }

}
