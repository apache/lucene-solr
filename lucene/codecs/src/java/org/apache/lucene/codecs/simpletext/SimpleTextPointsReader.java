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
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.StringHelper;

import static org.apache.lucene.codecs.simpletext.SimpleTextPointsWriter.BLOCK_FP;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointsWriter.BYTES_PER_DIM;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointsWriter.DOC_COUNT;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointsWriter.FIELD_COUNT;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointsWriter.FIELD_FP;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointsWriter.FIELD_FP_NAME;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointsWriter.INDEX_COUNT;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointsWriter.MAX_LEAF_POINTS;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointsWriter.MAX_VALUE;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointsWriter.MIN_VALUE;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointsWriter.NUM_DATA_DIMS;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointsWriter.NUM_INDEX_DIMS;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointsWriter.POINT_COUNT;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointsWriter.SPLIT_COUNT;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointsWriter.SPLIT_DIM;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointsWriter.SPLIT_VALUE;

class SimpleTextPointsReader extends PointsReader {

  private final IndexInput dataIn;
  final SegmentReadState readState;
  final Map<String,SimpleTextBKDReader> readers = new HashMap<>();
  final BytesRefBuilder scratch = new BytesRefBuilder();

  public SimpleTextPointsReader(SegmentReadState readState) throws IOException {
    // Initialize readers now:

    // Read index:
    Map<String,Long> fieldToFileOffset = new HashMap<>();

    String indexFileName = IndexFileNames.segmentFileName(readState.segmentInfo.name, readState.segmentSuffix, SimpleTextPointsFormat.POINT_INDEX_EXTENSION);
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
    String fileName = IndexFileNames.segmentFileName(readState.segmentInfo.name, readState.segmentSuffix, SimpleTextPointsFormat.POINT_EXTENSION);
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

  private SimpleTextBKDReader initReader(long fp) throws IOException {
    // NOTE: matches what writeIndex does in SimpleTextPointsWriter
    dataIn.seek(fp);
    readLine(dataIn);
    int numDataDims = parseInt(NUM_DATA_DIMS);

    readLine(dataIn);
    int numIndexDims = parseInt(NUM_INDEX_DIMS);

    readLine(dataIn);
    int bytesPerDim = parseInt(BYTES_PER_DIM);

    readLine(dataIn);
    int maxPointsInLeafNode = parseInt(MAX_LEAF_POINTS);

    readLine(dataIn);
    int count = parseInt(INDEX_COUNT);

    readLine(dataIn);
    assert startsWith(MIN_VALUE);
    BytesRef minValue = SimpleTextUtil.fromBytesRefString(stripPrefix(MIN_VALUE));
    assert minValue.length == numIndexDims*bytesPerDim;

    readLine(dataIn);
    assert startsWith(MAX_VALUE);
    BytesRef maxValue = SimpleTextUtil.fromBytesRefString(stripPrefix(MAX_VALUE));
    assert maxValue.length == numIndexDims*bytesPerDim;

    readLine(dataIn);
    assert startsWith(POINT_COUNT);
    long pointCount = parseLong(POINT_COUNT);

    readLine(dataIn);
    assert startsWith(DOC_COUNT);
    int docCount = parseInt(DOC_COUNT);
    
    long[] leafBlockFPs = new long[count];
    for(int i=0;i<count;i++) {
      readLine(dataIn);
      leafBlockFPs[i] = parseLong(BLOCK_FP);
    }
    readLine(dataIn);
    count = parseInt(SPLIT_COUNT);

    byte[] splitPackedValues;
    int bytesPerIndexEntry;
    if (numIndexDims == 1) {
      bytesPerIndexEntry = bytesPerDim;
    } else {
      bytesPerIndexEntry = 1 + bytesPerDim;
    }
    splitPackedValues = new byte[count * bytesPerIndexEntry];
    for(int i=0;i<count;i++) {
      readLine(dataIn);
      int address = bytesPerIndexEntry * i;
      int splitDim = parseInt(SPLIT_DIM);
      if (numIndexDims != 1) {
        splitPackedValues[address++] = (byte) splitDim;
      }
      readLine(dataIn);
      assert startsWith(SPLIT_VALUE);
      BytesRef br = SimpleTextUtil.fromBytesRefString(stripPrefix(SPLIT_VALUE));
      assert br.length == bytesPerDim;
      System.arraycopy(br.bytes, br.offset, splitPackedValues, address, bytesPerDim);
    }

    return new SimpleTextBKDReader(dataIn, numDataDims, numIndexDims, maxPointsInLeafNode, bytesPerDim, leafBlockFPs, splitPackedValues, minValue.bytes, maxValue.bytes, pointCount, docCount);
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

  @Override
  public PointValues getValues(String fieldName) throws IOException {
    FieldInfo fieldInfo = readState.fieldInfos.fieldInfo(fieldName);
    if (fieldInfo == null) {
      throw new IllegalArgumentException("field=\"" + fieldName + "\" is unrecognized");
    }
    if (fieldInfo.getPointDimensionCount() == 0) {
      throw new IllegalArgumentException("field=\"" + fieldName + "\" did not index points");
    }
    return readers.get(fieldName);
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
    return "SimpleTextPointsReader(segment=" + readState.segmentInfo.name + " maxDoc=" + readState.segmentInfo.maxDoc() + ")";
  }

}
