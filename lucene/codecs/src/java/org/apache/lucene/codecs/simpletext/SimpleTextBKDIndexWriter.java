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

import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.bkd.BKDConfig;
import org.apache.lucene.util.bkd.BKDIndexWriter;

import static org.apache.lucene.codecs.simpletext.SimpleTextPointsWriter.BLOCK_COUNT;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointsWriter.BLOCK_DOC_ID;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointsWriter.BLOCK_FP;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointsWriter.BLOCK_VALUE;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointsWriter.BYTES_PER_DIM;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointsWriter.DOC_COUNT;
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

/** Simplified/specialized index writer for SimpleText's usage
 *
 * @lucene.experimental */
public class SimpleTextBKDIndexWriter implements BKDIndexWriter {

  public static final String CODEC_NAME = "BKD";
  public static final int VERSION_START = 0;
  public static final int VERSION_COMPRESSED_DOC_IDS = 1;
  public static final int VERSION_COMPRESSED_VALUES = 2;
  public static final int VERSION_IMPLICIT_SPLIT_DIM_1D = 3;
  public static final int VERSION_CURRENT = VERSION_IMPLICIT_SPLIT_DIM_1D;

  private final IndexOutput out;

  private final BytesRefBuilder scratch = new BytesRefBuilder();

  public SimpleTextBKDIndexWriter(IndexOutput out) {
    this.out = out;
  }

  @Override
  public void writeLeafBlock(BKDConfig config, BKDLeafBlock leafBlock, int[] commonPrefixes, int sortedDim, int leafCardinality) throws IOException {
    writeLeafBlockDocs(out, leafBlock);
    writeLeafBlockPackedValues(out, leafBlock);
  }

  @Override
  public void writeIndex(BKDConfig config, BKDTreeLeafNodes leafNodes, byte[] minPackedValue,
                         byte[] maxPackedValue, long pointCount, int numberDocs) throws IOException {
    write(out, NUM_DATA_DIMS);
    writeInt(out, config.numDims);
    newline(out);

    write(out, NUM_INDEX_DIMS);
    writeInt(out, config.numIndexDims);
    newline(out);

    write(out, BYTES_PER_DIM);
    writeInt(out, config.bytesPerDim);
    newline(out);

    write(out, MAX_LEAF_POINTS);
    writeInt(out, config.maxPointsInLeafNode);
    newline(out);

    write(out, INDEX_COUNT);
    writeInt(out, leafNodes.numLeaves());
    newline(out);

    write(out, MIN_VALUE);
    BytesRef br = new BytesRef(minPackedValue, 0, minPackedValue.length);
    write(out, br.toString());
    newline(out);

    write(out, MAX_VALUE);
    br = new BytesRef(maxPackedValue, 0, maxPackedValue.length);
    write(out, br.toString());
    newline(out);

    write(out, POINT_COUNT);
    writeLong(out, pointCount);
    newline(out);

    write(out, DOC_COUNT);
    writeInt(out, numberDocs);
    newline(out);

    for(int i=0;i<leafNodes.numLeaves();i++) {
      write(out, BLOCK_FP);
      writeLong(out, leafNodes.getLeafLP(i));
      newline(out);
    }

   // assert ((leafNodes.numLeaves() - 1) % (1 + config.bytesPerDim)) == 0;
    int count = (leafNodes.numLeaves() - 1);//  / (1 + config.bytesPerDim);
   // assert count == leafNodes.numLeaves();

    write(out, SPLIT_COUNT);
    writeInt(out, count);
    newline(out);

    for(int i=0;i<count;i++) {
      write(out, SPLIT_DIM);
      writeInt(out, leafNodes.getSplitDimension(i));
      newline(out);
      write(out, SPLIT_VALUE);
      br = leafNodes.getSplitValue(i);
      write(out, br.toString());
      newline(out);
    }
  }

  @Override
  public long getFilePointer() {
    return out.getFilePointer();
  }

  private void writeLeafBlockDocs(IndexOutput out, BKDLeafBlock leafBlock) throws IOException {
    write(out, BLOCK_COUNT);
    writeInt(out, leafBlock.count());
    newline(out);
    for(int i = 0; i < leafBlock.count(); i++) {
      write(out, BLOCK_DOC_ID);
      writeInt(out, leafBlock.docId(i));
      newline(out);
    }
  }

  private void writeLeafBlockPackedValues(IndexOutput out, BKDLeafBlock leafBlock) throws IOException {
    for (int i = 0; i < leafBlock.count(); ++i) {
      BytesRef packedValue = leafBlock.packedValue(i);
      // NOTE: we don't do prefix coding, so we ignore commonPrefixLengths
      write(out, BLOCK_VALUE);
      write(out, packedValue.toString());
      newline(out);
    }
  }

  private void write(IndexOutput out, String s) throws IOException {
    SimpleTextUtil.write(out, s, scratch);
  }

  private void writeInt(IndexOutput out, int x) throws IOException {
    SimpleTextUtil.write(out, Integer.toString(x), scratch);
  }

  private void writeLong(IndexOutput out, long x) throws IOException {
    SimpleTextUtil.write(out, Long.toString(x), scratch);
  }

  private void write(IndexOutput out, BytesRef b) throws IOException {
    SimpleTextUtil.write(out, b);
  }

  private void newline(IndexOutput out) throws IOException {
    SimpleTextUtil.writeNewline(out);
  }
}
