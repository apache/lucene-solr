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
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Reads vectors from the index segments.
 * @lucene.experimental
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
      int searchStrategyId = meta.readInt();
      if (searchStrategyId < 0 || searchStrategyId >= VectorValues.SearchStrategy.values().length) {
        throw new CorruptIndexException("Invalid search strategy id: " + searchStrategyId, meta);
      }
      VectorValues.SearchStrategy searchStrategy = VectorValues.SearchStrategy.values()[searchStrategyId];
      long vectorDataOffset = meta.readVLong();
      long vectorDataLength = meta.readVLong();
      int dimension = meta.readInt();
      int size = meta.readInt();
      int[] ordToDoc = new int[size];
      for (int i = 0; i < size; i++) {
        int doc = meta.readVInt();
        ordToDoc[i] = doc;
      }
      FieldEntry fieldEntry = new FieldEntry(dimension, searchStrategy, maxDoc, vectorDataOffset, vectorDataLength,
                                              ordToDoc);
      fields.put(info.name, fieldEntry);
    }
  }

  @Override
  public long ramBytesUsed() {
    long totalBytes = RamUsageEstimator.shallowSizeOfInstance(Lucene90VectorReader.class);
    totalBytes += RamUsageEstimator.sizeOfMap(fields, RamUsageEstimator.shallowSizeOfInstance(FieldEntry.class));
    for (FieldEntry entry : fields.values()) {
      totalBytes += RamUsageEstimator.sizeOf(entry.ordToDoc);
    }
    return totalBytes;
  }

  @Override
  public void checkIntegrity() throws IOException {
    CodecUtil.checksumEntireFile(vectorData);
  }

  @Override
  public VectorValues getVectorValues(String field) throws IOException {
    FieldInfo info = fieldInfos.fieldInfo(field);
    if (info == null) {
      return null;
    }
    int dimension = info.getVectorDimension();
    if (dimension == 0) {
      return VectorValues.EMPTY;
    }
    FieldEntry fieldEntry = fields.get(field);
    if (fieldEntry == null) {
      // There is a FieldInfo, but no vectors. Should we have deleted the FieldInfo?
      return null;
    }
    if (dimension != fieldEntry.dimension) {
      throw new IllegalStateException("Inconsistent vector dimension for field=\"" + field + "\"; " + dimension + " != " + fieldEntry.dimension);
    }
    long numBytes = (long) fieldEntry.size() * dimension * Float.BYTES;
    if (numBytes != fieldEntry.vectorDataLength) {
      throw new IllegalStateException("Vector data length " + fieldEntry.vectorDataLength +
          " not matching size=" + fieldEntry.size() + " * dim=" + dimension + " * 4 = " +
          numBytes);
    }
    IndexInput bytesSlice = vectorData.slice("vector-data", fieldEntry.vectorDataOffset, fieldEntry.vectorDataLength);
    return new OffHeapVectorValues(fieldEntry, bytesSlice);
  }

  @Override
  public void close() throws IOException {
    vectorData.close();
  }

  private static class FieldEntry {

    final int dimension;
    final VectorValues.SearchStrategy searchStrategy;
    final int maxDoc;

    final long vectorDataOffset;
    final long vectorDataLength;
    final int[] ordToDoc;

    FieldEntry(int dimension, VectorValues.SearchStrategy searchStrategy, int maxDoc,
               long vectorDataOffset, long vectorDataLength, int[] ordToDoc) {
      this.dimension = dimension;
      this.searchStrategy = searchStrategy;
      this.maxDoc = maxDoc;
      this.vectorDataOffset = vectorDataOffset;
      this.vectorDataLength = vectorDataLength;
      this.ordToDoc = ordToDoc;
    }

    int size() {
      return ordToDoc.length;
    }
  }

  /** Read the vector values from the index input. This supports both iterated and random access. */
  private final static class OffHeapVectorValues extends VectorValues {

    final FieldEntry fieldEntry;
    final IndexInput dataIn;

    final BytesRef binaryValue;
    final ByteBuffer byteBuffer;
    final FloatBuffer floatBuffer;
    final int byteSize;
    final float[] value;

    int ord = -1;
    int doc = -1;

    OffHeapVectorValues(FieldEntry fieldEntry, IndexInput dataIn) {
      this.fieldEntry = fieldEntry;
      this.dataIn = dataIn;
      byteSize = Float.BYTES * fieldEntry.dimension;
      byteBuffer = ByteBuffer.allocate(byteSize);
      floatBuffer = byteBuffer.asFloatBuffer();
      value = new float[fieldEntry.dimension];
      binaryValue = new BytesRef(byteBuffer.array(), byteBuffer.arrayOffset(), byteSize);
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
    public SearchStrategy searchStrategy() {
      return fieldEntry.searchStrategy;
    }

    @Override
    public float[] vectorValue() throws IOException {
      binaryValue();
      floatBuffer.position(0);
      floatBuffer.get(value, 0, fieldEntry.dimension);
      return value;
    }

    @Override
    public BytesRef binaryValue() throws IOException {
      dataIn.seek(ord * byteSize);
      dataIn.readBytes(byteBuffer.array(), byteBuffer.arrayOffset(), byteSize);
      return binaryValue;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() {
      if (++ord >= size()) {
        doc = NO_MORE_DOCS;
      } else {
        doc = fieldEntry.ordToDoc[ord];
      }
      return doc;
    }

    @Override
    public int advance(int target) throws IOException {
      // We could do better by log-binary search in ordToDoc, but this is never used
      return slowAdvance(target);
    }

    @Override
    public long cost() {
      return fieldEntry.size();
    }

    @Override
    public RandomAccess randomAccess() {
      return new OffHeapRandomAccess(dataIn.clone());
    }


    class OffHeapRandomAccess implements VectorValues.RandomAccess {

      final IndexInput dataIn;

      final BytesRef binaryValue;
      final ByteBuffer byteBuffer;
      final FloatBuffer floatBuffer;
      final int byteSize;
      final float[] value;

      OffHeapRandomAccess(IndexInput dataIn) {
        this.dataIn = dataIn;
        byteSize = Float.BYTES * dimension();
        byteBuffer = ByteBuffer.allocate(byteSize);
        floatBuffer = byteBuffer.asFloatBuffer();
        value = new float[dimension()];
        binaryValue = new BytesRef(byteBuffer.array(), byteBuffer.arrayOffset(), byteSize);
      }

      @Override
      public int size() {
        return fieldEntry.size();
      }

      @Override
      public int dimension() {
        return fieldEntry.dimension;
      }

      @Override
      public SearchStrategy searchStrategy() {
        return fieldEntry.searchStrategy;
      }

      @Override
      public float[] vectorValue(int targetOrd) throws IOException {
        readValue(targetOrd);
        floatBuffer.position(0);
        floatBuffer.get(value);
        return value;
      }

      @Override
      public BytesRef binaryValue(int targetOrd) throws IOException {
        readValue(targetOrd);
        return binaryValue;
      }

      private void readValue(int targetOrd) throws IOException {
        long offset = targetOrd * byteSize;
        dataIn.seek(offset);
        dataIn.readBytes(byteBuffer.array(), byteBuffer.arrayOffset(), byteSize);
      }

      @Override
      public TopDocs search(float[] vector, int topK, int fanout) throws IOException {
        throw new UnsupportedOperationException();
      }
    }
  }
}
