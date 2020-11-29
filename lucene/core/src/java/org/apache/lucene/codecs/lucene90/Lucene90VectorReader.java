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
import java.util.Random;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.VectorReader;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.KnnGraphValues;
import org.apache.lucene.index.RandomAccessVectorValues;
import org.apache.lucene.index.RandomAccessVectorValuesProducer;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.apache.lucene.util.hnsw.Neighbors;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Reads vectors from the index segments along with index data structures supporting KNN search.
 * @lucene.experimental
 */
public final class Lucene90VectorReader extends VectorReader {

  private final FieldInfos fieldInfos;
  private final Map<String, FieldEntry> fields = new HashMap<>();
  private final IndexInput vectorData;
  private final IndexInput vectorIndex;
  private final long checksumSeed;

  Lucene90VectorReader(SegmentReadState state) throws IOException {
    this.fieldInfos = state.fieldInfos;

    int versionMeta = readMetadata(state, Lucene90VectorFormat.META_EXTENSION);
    long[] checksumRef = new long[1];
    vectorData = openDataInput(state, versionMeta, Lucene90VectorFormat.VECTOR_DATA_EXTENSION, Lucene90VectorFormat.VECTOR_DATA_CODEC_NAME, checksumRef);
    vectorIndex = openDataInput(state, versionMeta, Lucene90VectorFormat.VECTOR_INDEX_EXTENSION, Lucene90VectorFormat.VECTOR_INDEX_CODEC_NAME, checksumRef);
    checksumSeed = checksumRef[0];
  }

  private int readMetadata(SegmentReadState state, String fileExtension) throws IOException {
    String metaFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, fileExtension);
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
    return versionMeta;
  }

  private static IndexInput openDataInput(SegmentReadState state, int versionMeta, String fileExtension, String codecName, long[] checksumRef) throws IOException {
    boolean success = false;

    String fileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, fileExtension);
    IndexInput in = state.directory.openInput(fileName, state.context);
    try {
      int versionVectorData = CodecUtil.checkIndexHeader(in,
          codecName,
          Lucene90VectorFormat.VERSION_START,
          Lucene90VectorFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      if (versionMeta != versionVectorData) {
        throw new CorruptIndexException("Format versions mismatch: meta=" + versionMeta + ", " + codecName + "=" + versionVectorData, in);
      }
      checksumRef[0] = CodecUtil.retrieveChecksum(in);
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(in);
      }
    }
    return in;
  }

  private void readFields(ChecksumIndexInput meta, FieldInfos infos) throws IOException {
    for (int fieldNumber = meta.readInt(); fieldNumber != -1; fieldNumber = meta.readInt()) {
      FieldInfo info = infos.fieldInfo(fieldNumber);
      if (info == null) {
        throw new CorruptIndexException("Invalid field number: " + fieldNumber, meta);
      }
      fields.put(info.name, readField(meta));
    }
  }

  private VectorValues.SearchStrategy readSearchStrategy(DataInput input) throws IOException {
    int searchStrategyId = input.readInt();
    if (searchStrategyId < 0 || searchStrategyId >= VectorValues.SearchStrategy.values().length) {
      throw new CorruptIndexException("Invalid search strategy id: " + searchStrategyId, input);
    }
    return VectorValues.SearchStrategy.values()[searchStrategyId];
  }

  private FieldEntry readField(DataInput input) throws IOException {
    VectorValues.SearchStrategy searchStrategy = readSearchStrategy(input);
    switch(searchStrategy) {
      case NONE:
        return new FieldEntry(input, searchStrategy);
      case DOT_PRODUCT_HNSW:
      case EUCLIDEAN_HNSW:
        return new HnswGraphFieldEntry(input, searchStrategy);
      default:
        throw new CorruptIndexException("Unknown vector search strategy: " + searchStrategy, input);
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
    CodecUtil.checksumEntireFile(vectorIndex);
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

  public KnnGraphValues getGraphValues(String field) throws IOException {
    FieldInfo info = fieldInfos.fieldInfo(field);
    if (info == null) {
      throw new IllegalArgumentException("No such field '" + field + "'");
    }
    FieldEntry entry = fields.get(field);
    if (entry != null && entry.indexDataLength > 0) {
      return getGraphValues(entry);
    } else {
      return KnnGraphValues.EMPTY;
    }
  }

  private KnnGraphValues getGraphValues(FieldEntry entry) throws IOException {
    if (entry.searchStrategy.isHnsw()) {
      HnswGraphFieldEntry graphEntry = (HnswGraphFieldEntry) entry;
      IndexInput bytesSlice = vectorIndex.slice("graph-data", entry.indexDataOffset, entry.indexDataLength);
      return new IndexedKnnGraphReader(graphEntry, bytesSlice);
    } else {
      return KnnGraphValues.EMPTY;
    }
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(vectorData, vectorIndex);
  }

  private static class FieldEntry {

    final int dimension;
    final VectorValues.SearchStrategy searchStrategy;

    final long vectorDataOffset;
    final long vectorDataLength;
    final long indexDataOffset;
    final long indexDataLength;
    final int[] ordToDoc;

    FieldEntry(DataInput input, VectorValues.SearchStrategy searchStrategy) throws IOException {
      this.searchStrategy = searchStrategy;
      vectorDataOffset = input.readVLong();
      vectorDataLength = input.readVLong();
      indexDataOffset = input.readVLong();
      indexDataLength = input.readVLong();
      dimension = input.readInt();
      int size = input.readInt();
      ordToDoc = new int[size];
      for (int i = 0; i < size; i++) {
        int doc = input.readVInt();
        ordToDoc[i] = doc;
      }
    }

    int size() {
      return ordToDoc.length;
    }
  }

  private static class HnswGraphFieldEntry extends FieldEntry {

    final long[] ordOffsets;

    HnswGraphFieldEntry(DataInput input, VectorValues.SearchStrategy searchStrategy) throws IOException {
      super(input, searchStrategy);
      ordOffsets = new long[size()];
      long offset = 0;
      for (int i = 0; i < ordOffsets.length; i++) {
        offset += input.readVLong();
        ordOffsets[i] = offset;
      }
    }
  }

  /** Read the vector values from the index input. This supports both iterated and random access. */
  private final class OffHeapVectorValues extends VectorValues implements RandomAccessVectorValuesProducer {

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
    public RandomAccessVectorValues randomAccess() {
      return new OffHeapRandomAccess(dataIn.clone());
    }

    @Override
    public TopDocs search(float[] vector, int topK, int fanout) throws IOException {
      // use a seed that is fixed for the index so we get reproducible results for the same query
      final Random random = new Random(checksumSeed);
      Neighbors results = HnswGraph.search(vector, topK + fanout, topK + fanout, randomAccess(), getGraphValues(fieldEntry), random);
      while (results.size() > topK) {
        results.pop();
      }
      int i = 0;
      ScoreDoc[] scoreDocs = new ScoreDoc[Math.min(results.size(), topK)];
      boolean reversed = searchStrategy().reversed;
      while (results.size() > 0) {
        int node = results.topNode();
        float score = results.topScore();
        results.pop();
        if (reversed) {
          score = (float) Math.exp(-score / vector.length);
        }
        scoreDocs[scoreDocs.length - ++i] = new ScoreDoc(fieldEntry.ordToDoc[node], score);
      }
      // always return >= the case where we can assert == is only when there are fewer than topK vectors in the index
      return new TopDocs(new TotalHits(results.visitedCount(), TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), scoreDocs);
    }

    class OffHeapRandomAccess implements RandomAccessVectorValues {

      final IndexInput dataIn;

      final BytesRef binaryValue;
      final ByteBuffer byteBuffer;
      final FloatBuffer floatBuffer;
      final float[] value;

      OffHeapRandomAccess(IndexInput dataIn) {
        this.dataIn = dataIn;
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
    }
  }

  /** Read the nearest-neighbors graph from the index input */
  private final class IndexedKnnGraphReader extends KnnGraphValues {

    final HnswGraphFieldEntry entry;
    final IndexInput dataIn;

    int arcCount;
    int arcUpTo;
    int arc;

    IndexedKnnGraphReader(HnswGraphFieldEntry entry, IndexInput dataIn) {
      this.entry = entry;
      this.dataIn = dataIn;
    }

    @Override
    public void seek(int targetOrd) throws IOException {
      // unsafe; no bounds checking
      dataIn.seek(entry.ordOffsets[targetOrd]);
      arcCount = dataIn.readInt();
      arc = -1;
      arcUpTo = 0;
    }

    @Override
    public int nextNeighbor() throws IOException {
      if (arcUpTo >= arcCount) {
        return NO_MORE_DOCS;
      }
      ++arcUpTo;
      arc += dataIn.readVInt();
      return arc;
    }
  }
}
