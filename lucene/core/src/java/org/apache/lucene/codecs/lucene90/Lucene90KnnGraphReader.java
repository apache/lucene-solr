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
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnGraphReader;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.KnnGraphValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Reads vector values and knn graphs from the index segments.
 */
public final class Lucene90KnnGraphReader extends KnnGraphReader {

  private final FieldInfos fieldInfos;
  private final Map<String, KnnGraphEntry> graphs = new HashMap<>();
  private final IndexInput vectorData, graphData;
  private final int maxDoc;
  private long ramBytesUsed;

  public Lucene90KnnGraphReader(SegmentReadState state) throws IOException {
    this.fieldInfos = state.fieldInfos;
    this.maxDoc = state.segmentInfo.maxDoc();
    this.ramBytesUsed = RamUsageEstimator.shallowSizeOfInstance(getClass());

    String metaFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, Lucene90KnnGraphFormat.META_EXTENSION);
    int versionMeta = -1;
    try (ChecksumIndexInput meta = state.directory.openChecksumInput(metaFileName, state.context)) {
      Throwable priorE = null;
      try {
        versionMeta = CodecUtil.checkIndexHeader(meta,
            Lucene90KnnGraphFormat.META_CODEC_NAME,
            Lucene90KnnGraphFormat.VERSION_START,
            Lucene90KnnGraphFormat.VERSION_CURRENT,
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

    String vectorDataFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, Lucene90KnnGraphFormat.VECTOR_DATA_EXTENSION);
    this.vectorData = state.directory.openInput(vectorDataFileName, state.context);
    try {
      int versionVectorData = CodecUtil.checkIndexHeader(vectorData,
          Lucene90KnnGraphFormat.VECTOR_DATA_CODEC_NAME,
          Lucene90KnnGraphFormat.VERSION_START,
          Lucene90KnnGraphFormat.VERSION_CURRENT,
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

    success = false;
    String graphDataFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, Lucene90KnnGraphFormat.GRAPH_DATA_EXTENSION);
    this.graphData = state.directory.openInput(graphDataFileName, state.context);
    try {
      int versionGraphData = CodecUtil.checkIndexHeader(graphData,
          Lucene90KnnGraphFormat.GRAPH_DATA_CODEC_NAME,
          Lucene90KnnGraphFormat.VERSION_START,
          Lucene90KnnGraphFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      if (versionMeta != versionGraphData) {
        throw new CorruptIndexException("Format versions mismatch: meta=" + versionMeta + ", graph data=" + versionGraphData, vectorData);
      }

      CodecUtil.retrieveChecksum(graphData);

      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(this.graphData);
      }
    }
  }

  private void readFields(ChecksumIndexInput meta, FieldInfos infos) throws IOException {
    for (int fieldNumber = meta.readInt(); fieldNumber != -1; fieldNumber = meta.readInt()) {
      FieldInfo info = infos.fieldInfo(fieldNumber);
      if (info == null) {
        throw new CorruptIndexException("Invalid field number: " + fieldNumber, meta);
      }

      long vectorDataOffset = meta.readVLong();
      long vectorDataLength = meta.readVLong();
      long graphDataOffset = meta.readVLong();
      long graphDataLength = meta.readVLong();
      int topLevel = meta.readInt();
      int epCount = meta.readInt();
      int[] entryPoints = new int[epCount];
      for (int i = 0; i < epCount; i++) {
        entryPoints[i] = meta.readVInt();
      }
      int offsetCount = meta.readInt();
      final Map<Integer, Integer> docToOrd = new HashMap<>();
      final Map<Integer, Long> docToOffsets = new HashMap<>();
      for (int i = 0; i < offsetCount; i++) {
        int doc = meta.readVInt();
        docToOrd.put(doc, i);
        docToOffsets.put(doc, meta.readVLong());
      }

      KnnGraphEntry entry = new KnnGraphEntry(vectorDataOffset, vectorDataLength, graphDataOffset, graphDataLength, topLevel, entryPoints, Map.copyOf(docToOrd), Map.copyOf(docToOffsets));
      ramBytesUsed += RamUsageEstimator.shallowSizeOfInstance(KnnGraphEntry.class);
      ramBytesUsed += RamUsageEstimator.shallowSizeOf(entry.docToVectorOrd);
      ramBytesUsed += RamUsageEstimator.shallowSizeOf(entry.docToGraphOffset);
      graphs.put(info.name, entry);
    }
  }


  @Override
  public void checkIntegrity() throws IOException {
    // TODO
  }

  @Override
  public VectorValues getVectorValues(String field) throws IOException {
    FieldInfo info = fieldInfos.fieldInfo(field);
    if (info == null) {
      return VectorValues.EMPTY;
    }
    int numDims = info.getVectorNumDimensions();
    if (numDims <= 0) {
      return VectorValues.EMPTY;
    }

    final KnnGraphEntry entry = graphs.get(field);
    if (entry == null) {
      return VectorValues.EMPTY;
    }

    final IndexInput bytesSlice = vectorData.slice("vector-data", entry.vectorDataOffset, entry.vectorDataLength);
    return new RandomAccessVectorValuesReader(maxDoc, numDims, entry, bytesSlice);
  }

  @Override
  public KnnGraphValues getGraphValues(String field) throws IOException {
    final KnnGraphEntry entry = graphs.get(field);
    if (entry == null) {
      return KnnGraphValues.EMPTY;
    }
    final IndexInput bytesSlice = graphData.slice("knn-graph-data", entry.graphDataOffset, entry.graphDataLength);
    return new IndexedKnnGraphReader(maxDoc, graphs.get(field), bytesSlice);
  }

  @Override
  public void close() throws IOException {
    vectorData.close();
    graphData.close();
  }

  @Override
  public long ramBytesUsed() {
    return 0;
  }

  private static class KnnGraphEntry {
    final long vectorDataOffset;
    final long vectorDataLength;
    final long graphDataOffset;
    final long graphDataLength;
    final int topLevel;
    final int[] enterPoints;
    final Map<Integer, Integer> docToVectorOrd;
    final Map<Integer, Long> docToGraphOffset;

    KnnGraphEntry(long vectorDataOffset, long vectorDataLength, long graphDataOffset, long graphDataLength, int topLevel, int[] enterPoints,
                  Map<Integer, Integer> docToVectorOrd, Map<Integer, Long> docToGraphOffset) {
      this.vectorDataOffset = vectorDataOffset;
      this.vectorDataLength = vectorDataLength;
      this.graphDataOffset = graphDataOffset;
      this.graphDataLength = graphDataLength;
      this.topLevel = topLevel;
      this.enterPoints = enterPoints;
      this.docToVectorOrd = docToVectorOrd;
      this.docToGraphOffset = docToGraphOffset;
    }
  }

  private static class FriendsRef implements Accountable {
    final int maxLevel;
    final int[][] friendsAtLevel;

    long ramBytesUsed;

    FriendsRef(int maxLevel) {
      if (maxLevel < 0) {
        this.maxLevel = -1;
        friendsAtLevel = new int[0][0];
      } else {
        this.maxLevel = maxLevel;
        this.friendsAtLevel = new int[maxLevel+1][];
      }
      this.ramBytesUsed = RamUsageEstimator.shallowSizeOfInstance(getClass());
    }

    void setFriendsAtLevel(int level, int[] friends) {
      assert level >= 0 && level <= this.maxLevel;
      friendsAtLevel[level] = friends;
      ramBytesUsed += RamUsageEstimator.sizeOf(friends);
    }

    @Override
    public long ramBytesUsed() {
      return ramBytesUsed;
    }
  }

  /** Read the vector values from the index input stream. This allows random access to the underlying vectors data. */
  private final static class RandomAccessVectorValuesReader extends VectorValues {

    final int maxDoc;
    final int numDims;
    final KnnGraphEntry entry;
    final IndexInput dataIn;

    int doc = -1;
    byte[] binaryValue;

    RandomAccessVectorValuesReader(int maxDoc, int numDims, KnnGraphEntry entry, IndexInput dataIn) {
      this.maxDoc = maxDoc;
      this.numDims = numDims;
      this.entry = entry;
      this.dataIn = dataIn;
      this.binaryValue = new byte[Float.BYTES * numDims];
    }

    @Override
    public float[] vectorValue() throws IOException {
      if (doc == NO_MORE_DOCS) {
        return null;
      }
      return VectorValues.decode(binaryValue, numDims);
    }

    @Override
    public boolean seek(int target) throws IOException {
      if (target > maxDoc) {
        doc = NO_MORE_DOCS;
        return false;
      }
      doc = target;
      Integer ord = entry.docToVectorOrd.get(doc);
      if (ord == null) {
        return false;
      }
      int offset = Float.BYTES * numDims * ord;
      assert offset >= 0;
      dataIn.seek(offset);
      dataIn.readBytes(binaryValue, 0, binaryValue.length);
      return true;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() throws IOException {
      return advance(doc + 1);
    }

    @Override
    public int advance(int target) throws IOException {
      int _target = target;
      boolean found;
      do {
        found = seek(_target++);
      } while (!found && doc != NO_MORE_DOCS);
      return doc;
    }

    @Override
    public long cost() {
      return maxDoc;
    }
  }

  /** Read the knn graph from the index input stream */
  private final static class IndexedKnnGraphReader extends KnnGraphValues {

    final int maxDoc;
    final KnnGraphEntry entry;
    final IndexInput dataIn;

    int doc = -1;
    int maxLevel = -1;
    FriendsRef friendsRef = null;

    IndexedKnnGraphReader(int maxDoc, KnnGraphEntry entry, IndexInput dataIn) {
      this.maxDoc = maxDoc;
      this.entry = entry;
      this.dataIn = dataIn;
    }

    @Override
    public int getTopLevel() {
      return entry.topLevel;
    }

    @Override
    public int getMaxLevel() {
      return maxLevel;
    }

    @Override
    public IntsRef getFriends(int level) {
      if (level < 0 || level > maxLevel) {
        throw new IllegalArgumentException("level must be >= 0 or <= maxLevel (=" + maxLevel + "); got " + level);
      }
      int[] friends = friendsRef.friendsAtLevel[level];
      return new IntsRef(friends, 0, friends.length);
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      advance(target);
      return doc == target;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() throws IOException {
      return advance(doc + 1);
    }

    @Override
    public int advance(int target) throws IOException {
      if (doc > target || target > maxDoc) {
        return doc = NO_MORE_DOCS;
      }
      doc = target - 1;
      Long offset;
      boolean found;
      do {
        offset = entry.docToGraphOffset.get(++doc);
        found = offset != null;
      } while (!found && doc < maxDoc);
      if (doc == maxDoc) {
        return doc = NO_MORE_DOCS;
      }
      dataIn.seek(offset);
      int maxLevel = dataIn.readInt();
      assert maxLevel > 0;
      this.maxLevel = maxLevel;
      this.friendsRef = readFriends(maxLevel);
      return doc;
    }

    private FriendsRef readFriends(int maxLevel) throws IOException {
      FriendsRef friendsRef = new FriendsRef(maxLevel);
      for (int l = maxLevel; l >= 0; l--) {
        int friendSize = dataIn.readInt();
        assert friendSize > 0;
        int[] frineds = new int[friendSize];
        int friendId = dataIn.readVInt();  // first friend id
        frineds[0] = friendId;
        for (int i = 1; i < friendSize; i++) {
          int delta = dataIn.readVInt();
          assert delta > 0;
          friendId += delta;
          frineds[i] = friendId;
        }
        friendsRef.setFriendsAtLevel(l, frineds);
      }
      return friendsRef;
    }


    @Override
    public long cost() {
      return maxDoc;
    }
  }
}
