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

import org.apache.lucene.codecs.PointReader;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.bkd.BKDReader;

import static org.apache.lucene.codecs.simpletext.SimpleTextPointWriter.BLOCK_FP;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointWriter.BYTES_PER_DIM;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointWriter.FIELD_COUNT;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointWriter.FIELD_FP;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointWriter.FIELD_FP_NAME;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointWriter.INDEX_COUNT;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointWriter.MAX_LEAF_POINTS;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointWriter.MAX_VALUE;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointWriter.MIN_VALUE;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointWriter.NUM_DIMS;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointWriter.SPLIT_COUNT;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointWriter.SPLIT_DIM;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointWriter.SPLIT_VALUE;

class SimpleTextPointReader extends PointReader {

  private final IndexInput dataIn;
  final SegmentReadState readState;
  final Map<String,BKDReader> readers = new HashMap<>();
  final BytesRefBuilder scratch = new BytesRefBuilder();

  public SimpleTextPointReader(SegmentReadState readState) throws IOException {
    // Initialize readers now:

    // Read index:
    Map<String,Long> fieldToFileOffset = new HashMap<>();

    String indexFileName = IndexFileNames.segmentFileName(readState.segmentInfo.name, readState.segmentSuffix, SimpleTextPointFormat.POINT_INDEX_EXTENSION);
    try (ChecksumIndexInput in = readState.directory.openChecksumInput(indexFileName, IOContext.DEFAULT)) {
      readLine(in);
      int count = parseInt(FIELD_COUNT);
      for(int i=0;i<count;i++) {
        readLine(in);
        String fieldName = stripPrefix(FIELD_FP_NAME);
        readLine(in);
        long fp = parseLong(FIELD_FP);
        fieldToFileOffset.put(fieldName, fp);
      }
      SimpleTextUtil.checkFooter(in);
    }

    boolean success = false;
    String fileName = IndexFileNames.segmentFileName(readState.segmentInfo.name, readState.segmentSuffix, SimpleTextPointFormat.POINT_EXTENSION);
    dataIn = readState.directory.openInput(fileName, IOContext.DEFAULT);
    try {
      for(Map.Entry<String,Long> ent : fieldToFileOffset.entrySet()) {
        readers.put(ent.getKey(), initReader(ent.getValue()));
      }
      success = true;
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
        
    this.readState = readState;
  }

  private BKDReader initReader(long fp) throws IOException {
    // NOTE: matches what writeIndex does in SimpleTextPointWriter
    dataIn.seek(fp);
    readLine(dataIn);
    int numDims = parseInt(NUM_DIMS);

    readLine(dataIn);
    int bytesPerDim = parseInt(BYTES_PER_DIM);

    readLine(dataIn);
    int maxPointsInLeafNode = parseInt(MAX_LEAF_POINTS);

    readLine(dataIn);
    int count = parseInt(INDEX_COUNT);

    readLine(dataIn);
    assert startsWith(MIN_VALUE);
    BytesRef minValue = SimpleTextUtil.fromBytesRefString(stripPrefix(MIN_VALUE));
    assert minValue.length == numDims*bytesPerDim;

    readLine(dataIn);
    assert startsWith(MAX_VALUE);
    BytesRef maxValue = SimpleTextUtil.fromBytesRefString(stripPrefix(MAX_VALUE));
    assert maxValue.length == numDims*bytesPerDim;
    
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

    return new SimpleTextBKDReader(dataIn, numDims, maxPointsInLeafNode, bytesPerDim, leafBlockFPs, splitPackedValues, minValue.bytes, maxValue.bytes);
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

  private BKDReader getBKDReader(String fieldName) {
    FieldInfo fieldInfo = readState.fieldInfos.fieldInfo(fieldName);
    if (fieldInfo == null) {
      throw new IllegalArgumentException("field=\"" + fieldName + "\" is unrecognized");
    }
    if (fieldInfo.getPointDimensionCount() == 0) {
      throw new IllegalArgumentException("field=\"" + fieldName + "\" did not index points");
    }
    return readers.get(fieldName);
  }

  /** Finds all documents and points matching the provided visitor */
  @Override
  public void intersect(String fieldName, IntersectVisitor visitor) throws IOException {
    BKDReader bkdReader = getBKDReader(fieldName);
    if (bkdReader == null) {
      // Schema ghost corner case!  This field did index points in the past, but
      // now all docs having this field were deleted in this segment:
      return;
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

  @Override
  public String toString() {
    return "SimpleTextPointReader(segment=" + readState.segmentInfo.name + " maxDoc=" + readState.segmentInfo.maxDoc() + ")";
  }

  @Override
  public byte[] getMinPackedValue(String fieldName) {
    BKDReader bkdReader = getBKDReader(fieldName);
    if (bkdReader == null) {
      // Schema ghost corner case!  This field did index points in the past, but
      // now all docs having this field were deleted in this segment:
      return null;
    }
    return bkdReader.getMinPackedValue();
  }

  @Override
  public byte[] getMaxPackedValue(String fieldName) {
    BKDReader bkdReader = getBKDReader(fieldName);
    if (bkdReader == null) {
      // Schema ghost corner case!  This field did index points in the past, but
      // now all docs having this field were deleted in this segment:
      return null;
    }
    return bkdReader.getMaxPackedValue();
  }

  @Override
  public int getNumDimensions(String fieldName) {
    BKDReader bkdReader = getBKDReader(fieldName);
    if (bkdReader == null) {
      // Schema ghost corner case!  This field did index points in the past, but
      // now all docs having this field were deleted in this segment:
      return 0;
    }
    return bkdReader.getNumDimensions();
  }

  @Override
  public int getBytesPerDimension(String fieldName) {
    BKDReader bkdReader = getBKDReader(fieldName);
    if (bkdReader == null) {
      // Schema ghost corner case!  This field did index points in the past, but
      // now all docs having this field were deleted in this segment:
      return 0;
    }
    return bkdReader.getBytesPerDimension();
  }
}
