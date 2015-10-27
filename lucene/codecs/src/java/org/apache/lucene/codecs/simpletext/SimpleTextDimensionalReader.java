package org.apache.lucene.codecs.simpletext;

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
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.codecs.DimensionalReader;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.bkd.BKDReader;

import static org.apache.lucene.codecs.simpletext.SimpleTextDimensionalWriter.BLOCK_FP;
import static org.apache.lucene.codecs.simpletext.SimpleTextDimensionalWriter.BYTES_PER_DIM;
import static org.apache.lucene.codecs.simpletext.SimpleTextDimensionalWriter.FIELD_COUNT;
import static org.apache.lucene.codecs.simpletext.SimpleTextDimensionalWriter.FIELD_FP;
import static org.apache.lucene.codecs.simpletext.SimpleTextDimensionalWriter.FIELD_FP_NAME;
import static org.apache.lucene.codecs.simpletext.SimpleTextDimensionalWriter.INDEX_COUNT;
import static org.apache.lucene.codecs.simpletext.SimpleTextDimensionalWriter.MAX_LEAF_POINTS;
import static org.apache.lucene.codecs.simpletext.SimpleTextDimensionalWriter.NUM_DIMS;
import static org.apache.lucene.codecs.simpletext.SimpleTextDimensionalWriter.SPLIT_COUNT;
import static org.apache.lucene.codecs.simpletext.SimpleTextDimensionalWriter.SPLIT_DIM;
import static org.apache.lucene.codecs.simpletext.SimpleTextDimensionalWriter.SPLIT_VALUE;

class SimpleTextDimensionalReader extends DimensionalReader {

  private final IndexInput dataIn;
  final SegmentReadState readState;
  final Map<String,BKDReader> readers = new HashMap<>();
  final BytesRefBuilder scratch = new BytesRefBuilder();

  public SimpleTextDimensionalReader(SegmentReadState readState) throws IOException {
    // Initialize readers now:
    String fileName = IndexFileNames.segmentFileName(readState.segmentInfo.name, readState.segmentSuffix, SimpleTextDimensionalFormat.DIMENSIONAL_EXTENSION);
    dataIn = readState.directory.openInput(fileName, IOContext.DEFAULT);
    String indexFileName = IndexFileNames.segmentFileName(readState.segmentInfo.name, readState.segmentSuffix, SimpleTextDimensionalFormat.DIMENSIONAL_INDEX_EXTENSION);
    try (ChecksumIndexInput in = readState.directory.openChecksumInput(indexFileName, IOContext.DEFAULT)) {
      readLine(in);
      int count = parseInt(FIELD_COUNT);
      for(int i=0;i<count;i++) {
        readLine(in);
        String fieldName = stripPrefix(FIELD_FP_NAME);
        readLine(in);
        long fp = parseLong(FIELD_FP);
        readers.put(fieldName, initReader(fp));
      }
      SimpleTextUtil.checkFooter(in);
    }
    this.readState = readState;
  }

  private BKDReader initReader(long fp) throws IOException {
    // NOTE: matches what writeIndex does in SimpleTextDimensionalWriter
    dataIn.seek(fp);
    readLine(dataIn);
    int numDims = parseInt(NUM_DIMS);

    readLine(dataIn);
    int bytesPerDim = parseInt(BYTES_PER_DIM);

    readLine(dataIn);
    int maxPointsInLeafNode = parseInt(MAX_LEAF_POINTS);

    readLine(dataIn);
    int count = parseInt(INDEX_COUNT);
    long[] leafBlockFPs = new long[count];
    for(int i=0;i<count;i++) {
      readLine(dataIn);
      leafBlockFPs[i] = parseLong(BLOCK_FP);
    }
    readLine(dataIn);
    count = parseInt(SPLIT_COUNT);

    byte[] splitPackedValues = new byte[count * (1 + bytesPerDim)];
    for(int i=0;i<count;i++) {
      readLine(dataIn);
      splitPackedValues[(1 + bytesPerDim) * i] = (byte) parseInt(SPLIT_DIM);
      readLine(dataIn);
      assert startsWith(SPLIT_VALUE);
      BytesRef br = SimpleTextUtil.fromBytesRefString(stripPrefix(SPLIT_VALUE));
      assert br.length == bytesPerDim;
      System.arraycopy(br.bytes, br.offset, splitPackedValues, (1 + bytesPerDim) * i + 1, bytesPerDim);
    }

    return new SimpleTextBKDReader(dataIn, numDims, maxPointsInLeafNode, bytesPerDim, leafBlockFPs, splitPackedValues);
  }

  private void readLine(IndexInput in) throws IOException {
    SimpleTextUtil.readLine(in, scratch);
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
    return new String(scratch.bytes(), prefix.length, scratch.length() - prefix.length, StandardCharsets.UTF_8);
  }

  /** Finds all documents and points matching the provided visitor */
  @Override
  public void intersect(String field, IntersectVisitor visitor) throws IOException {
    BKDReader bkdReader = readers.get(field);
    if (bkdReader == null) {
      throw new IllegalArgumentException("field=\"" + field + "\" was not indexed with dimensional values");
    }
    bkdReader.intersect(visitor);
  }

  @Override
  public void checkIntegrity() throws IOException {
    BytesRefBuilder scratch = new BytesRefBuilder();
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
    return 0L;
  }

  @Override
  public void close() throws IOException {
    dataIn.close();
  }
}
