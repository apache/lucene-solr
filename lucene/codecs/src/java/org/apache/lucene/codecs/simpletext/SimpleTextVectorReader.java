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

package org.apache.lucene.codecs.simpletext;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.codecs.VectorReader;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.RandomAccessVectorValues;
import org.apache.lucene.index.RandomAccessVectorValuesProducer;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.StringHelper;

import static org.apache.lucene.codecs.simpletext.SimpleTextVectorWriter.*;

/**
 * Reads vector values from a simple text format. All vectors are read up front and cached in RAM in order to support
 * random access.
 * <b>FOR RECREATIONAL USE ONLY</b>
 * @lucene.experimental
 */
public class SimpleTextVectorReader extends VectorReader {

  private static final BytesRef EMPTY = new BytesRef("");

  private final SegmentReadState readState;
  private final IndexInput dataIn;
  private final BytesRefBuilder scratch = new BytesRefBuilder();
  private final Map<String, FieldEntry> fieldEntries = new HashMap<>();

  SimpleTextVectorReader(SegmentReadState readState) throws IOException {
    this.readState = readState;
    String metaFileName = IndexFileNames.segmentFileName(readState.segmentInfo.name, readState.segmentSuffix, SimpleTextVectorFormat.META_EXTENSION);
    try (ChecksumIndexInput in = readState.directory.openChecksumInput(metaFileName, IOContext.DEFAULT)) {
      int fieldNumber = readInt(in, FIELD_NUMBER);
      while (fieldNumber != -1) {
        String fieldName = readString(in, FIELD_NAME);
        String scoreFunctionName = readString(in, SCORE_FUNCTION);
        VectorValues.SearchStrategy searchStrategy = VectorValues.SearchStrategy.valueOf(scoreFunctionName);
        long vectorDataOffset = readLong(in, VECTOR_DATA_OFFSET);
        long vectorDataLength = readLong(in, VECTOR_DATA_LENGTH);
        int dimension = readInt(in, VECTOR_DIMENSION);
        int size = readInt(in, SIZE);
        int[] docIds = new int[size];
        for (int i = 0; i < size; i++) {
          docIds[i] = readInt(in, EMPTY);
        }
        assert fieldEntries.containsKey(fieldName) == false;
        fieldEntries.put(fieldName, new FieldEntry(dimension, searchStrategy, vectorDataOffset, vectorDataLength, docIds));
        fieldNumber = readInt(in, FIELD_NUMBER);
      }
      SimpleTextUtil.checkFooter(in);
    }

    String vectorFileName = IndexFileNames.segmentFileName(readState.segmentInfo.name, readState.segmentSuffix, SimpleTextVectorFormat.VECTOR_EXTENSION);
    dataIn = readState.directory.openInput(vectorFileName, IOContext.DEFAULT);
  }

  @Override
  public VectorValues getVectorValues(String field) throws IOException {
    FieldInfo info = readState.fieldInfos.fieldInfo(field);
    if (info == null) {
      throw new IllegalStateException("No vectors indexed for field=\"" + field + "\"");
    }
    int dimension = info.getVectorDimension();
    if (dimension == 0) {
      return VectorValues.EMPTY;
    }
    FieldEntry fieldEntry = fieldEntries.get(field);
    if (fieldEntry == null) {
      throw new IllegalStateException("No entry found for vector field=\"" + field + "\"");
    }
    if (dimension != fieldEntry.dimension) {
      throw new IllegalStateException("Inconsistent vector dimension for field=\"" + field + "\"; " + dimension + " != " + fieldEntry.dimension);
    }
    IndexInput bytesSlice = dataIn.slice("vector-data", fieldEntry.vectorDataOffset, fieldEntry.vectorDataLength);
    return new SimpleTextVectorValues(fieldEntry, bytesSlice);
  }

  @Override
  public void checkIntegrity() throws IOException {
    IndexInput clone = dataIn.clone();
    clone.seek(0);

    // checksum is fixed-width encoded with 20 bytes, plus 1 byte for newline (the space is included in SimpleTextUtil.CHECKSUM):
    long footerStartPos = dataIn.length() - (SimpleTextUtil.CHECKSUM.length + 21);
    ChecksumIndexInput input = new BufferedChecksumIndexInput(clone);
    while (true) {
      SimpleTextUtil.readLine(input, scratch);
      if (input.getFilePointer() >= footerStartPos) {
        // Make sure we landed at precisely the right location:
        if (input.getFilePointer() != footerStartPos) {
          throw new CorruptIndexException("SimpleText failure: footer does not start at expected position current=" + input.getFilePointer() + " vs expected=" + footerStartPos, input);
        }
        SimpleTextUtil.checkFooter(input);
        break;
      }
    }
  }

  @Override
  public long ramBytesUsed() {
    return 0;
  }

  @Override
  public void close() throws IOException {
    dataIn.close();
  }

  private static class FieldEntry {

    final int dimension;
    final VectorValues.SearchStrategy searchStrategy;

    final long vectorDataOffset;
    final long vectorDataLength;
    final int[] ordToDoc;

    FieldEntry(int dimension, VectorValues.SearchStrategy searchStrategy,
               long vectorDataOffset, long vectorDataLength, int[] ordToDoc) {
      this.dimension = dimension;
      this.searchStrategy = searchStrategy;
      this.vectorDataOffset = vectorDataOffset;
      this.vectorDataLength = vectorDataLength;
      this.ordToDoc = ordToDoc;
    }

    int size() {
      return ordToDoc.length;
    }
  }

  private static class SimpleTextVectorValues extends VectorValues implements RandomAccessVectorValues, RandomAccessVectorValuesProducer {

    private final BytesRefBuilder scratch = new BytesRefBuilder();
    private final FieldEntry entry;
    private final IndexInput in;
    private final BytesRef binaryValue;
    private final float[][] values;

    int curOrd;

    SimpleTextVectorValues(FieldEntry entry, IndexInput in) throws IOException {
      this.entry = entry;
      this.in = in;
      values = new float[entry.size()][entry.dimension];
      binaryValue = new BytesRef(entry.dimension * Float.BYTES);
      binaryValue.length = binaryValue.bytes.length;
      curOrd = -1;
      readAllVectors();
    }

    @Override
    public int dimension() {
      return entry.dimension;
    }

    @Override
    public int size() {
      return entry.size();
    }

    @Override
    public SearchStrategy searchStrategy() {
      return entry.searchStrategy;
    }

    @Override
    public float[] vectorValue() {
      return values[curOrd];
    }

    @Override
    public BytesRef binaryValue() {
      ByteBuffer.wrap(binaryValue.bytes).asFloatBuffer().get(values[curOrd]);
      return binaryValue;
    }

    @Override
    public RandomAccessVectorValues randomAccess() {
      return this;
    }

    @Override
    public int docID() {
      if (curOrd == -1) {
        return -1;
      }
      return entry.ordToDoc[curOrd];
    }

    @Override
    public int nextDoc() throws IOException {
      if (++curOrd < entry.size()) {
        return docID();
      }
      return NO_MORE_DOCS;
    }

    @Override
    public int advance(int target) throws IOException {
      return slowAdvance(target);
    }

    @Override
    public long cost() {
      return size();
    }

    private void readAllVectors() throws IOException {
      for (float[] value : values) {
        readVector(value);
      }
    }

    private void readVector(float[] value) throws IOException {
      SimpleTextUtil.readLine(in, scratch);
      // skip leading "[" and strip trailing "]"
      String s = new BytesRef(scratch.bytes(), 1, scratch.length() - 2).utf8ToString();
      String[] floatStrings = s.split(",");
      assert floatStrings.length == value.length : " read " + s + " when expecting " + value.length + " floats";
      for (int i = 0; i < floatStrings.length; i++) {
        value[i] = Float.parseFloat(floatStrings[i]);
      }
    }

    @Override
    public float[] vectorValue(int targetOrd) throws IOException {
      return values[targetOrd];
    }

    @Override
    public BytesRef binaryValue(int targetOrd) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public TopDocs search(float[] target, int k, int fanout) throws IOException {
      throw new UnsupportedOperationException();
    }
  }

  private int readInt(IndexInput in, BytesRef field) throws IOException {
    SimpleTextUtil.readLine(in, scratch);
    return parseInt(field);
  }

  private long readLong(IndexInput in, BytesRef field) throws IOException {
    SimpleTextUtil.readLine(in, scratch);
    return parseLong(field);
  }

  private String readString(IndexInput in, BytesRef field) throws IOException {
    SimpleTextUtil.readLine(in, scratch);
    return stripPrefix(field);
  }

  private boolean startsWith(BytesRef prefix) {
    return StringHelper.startsWith(scratch.get(), prefix);
  }

  private int parseInt(BytesRef prefix) {
    assert startsWith(prefix);
    return Integer.parseInt(stripPrefix(prefix));
  }

  private long parseLong(BytesRef prefix) {
    assert startsWith(prefix);
    return Long.parseLong(stripPrefix(prefix));
  }

  private String stripPrefix(BytesRef prefix) {
    int prefixLen = prefix.length;
    return new String(scratch.bytes(), prefixLen, scratch.length() - prefixLen, StandardCharsets.UTF_8);
  }
}
