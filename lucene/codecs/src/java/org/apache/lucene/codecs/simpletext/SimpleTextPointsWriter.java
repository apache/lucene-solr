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
import java.util.HashMap;
import java.util.Map;
import java.util.function.IntFunction;

import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.bkd.BKDWriter;

class SimpleTextPointsWriter extends PointsWriter {

  final static BytesRef NUM_DIMS      = new BytesRef("num dims ");
  final static BytesRef BYTES_PER_DIM = new BytesRef("bytes per dim ");
  final static BytesRef MAX_LEAF_POINTS = new BytesRef("max leaf points ");
  final static BytesRef INDEX_COUNT = new BytesRef("index count ");
  final static BytesRef BLOCK_COUNT   = new BytesRef("block count ");
  final static BytesRef BLOCK_DOC_ID  = new BytesRef("  doc ");
  final static BytesRef BLOCK_FP      = new BytesRef("  block fp ");
  final static BytesRef BLOCK_VALUE   = new BytesRef("  block value ");
  final static BytesRef SPLIT_COUNT   = new BytesRef("split count ");
  final static BytesRef SPLIT_DIM     = new BytesRef("  split dim ");
  final static BytesRef SPLIT_VALUE   = new BytesRef("  split value ");
  final static BytesRef FIELD_COUNT   = new BytesRef("field count ");
  final static BytesRef FIELD_FP_NAME = new BytesRef("  field fp name ");
  final static BytesRef FIELD_FP      = new BytesRef("  field fp ");
  final static BytesRef MIN_VALUE     = new BytesRef("min value ");
  final static BytesRef MAX_VALUE     = new BytesRef("max value ");
  final static BytesRef POINT_COUNT   = new BytesRef("point count ");
  final static BytesRef DOC_COUNT     = new BytesRef("doc count ");
  final static BytesRef END           = new BytesRef("END");

  private IndexOutput dataOut;
  final BytesRefBuilder scratch = new BytesRefBuilder();
  final SegmentWriteState writeState;
  final Map<String,Long> indexFPs = new HashMap<>();

  public SimpleTextPointsWriter(SegmentWriteState writeState) throws IOException {
    String fileName = IndexFileNames.segmentFileName(writeState.segmentInfo.name, writeState.segmentSuffix, SimpleTextPointsFormat.POINT_EXTENSION);
    dataOut = writeState.directory.createOutput(fileName, writeState.context);
    this.writeState = writeState;
  }

  @Override
  public void writeField(FieldInfo fieldInfo, PointsReader values) throws IOException {

    boolean singleValuePerDoc = values.size(fieldInfo.name) == values.getDocCount(fieldInfo.name);

    // We use the normal BKDWriter, but subclass to customize how it writes the index and blocks to disk:
    try (BKDWriter writer = new BKDWriter(writeState.segmentInfo.maxDoc(),
                                          writeState.directory,
                                          writeState.segmentInfo.name,
                                          fieldInfo.getPointDimensionCount(),
                                          fieldInfo.getPointNumBytes(),
                                          BKDWriter.DEFAULT_MAX_POINTS_IN_LEAF_NODE,
                                          BKDWriter.DEFAULT_MAX_MB_SORT_IN_HEAP,
                                          values.size(fieldInfo.name),
                                          singleValuePerDoc) {

        @Override
        protected void writeIndex(IndexOutput out, long[] leafBlockFPs, byte[] splitPackedValues) throws IOException {
          write(out, NUM_DIMS);
          writeInt(out, numDims);
          newline(out);

          write(out, BYTES_PER_DIM);
          writeInt(out, bytesPerDim);
          newline(out);

          write(out, MAX_LEAF_POINTS);
          writeInt(out, maxPointsInLeafNode);
          newline(out);

          write(out, INDEX_COUNT);
          writeInt(out, leafBlockFPs.length);
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
          writeInt(out, docsSeen.cardinality());
          newline(out);

          for(int i=0;i<leafBlockFPs.length;i++) {
            write(out, BLOCK_FP);
            writeLong(out, leafBlockFPs[i]);
            newline(out);
          }

          assert (splitPackedValues.length % (1 + fieldInfo.getPointNumBytes())) == 0;
          int count = splitPackedValues.length / (1 + fieldInfo.getPointNumBytes());
          assert count == leafBlockFPs.length;

          write(out, SPLIT_COUNT);
          writeInt(out, count);
          newline(out);

          for(int i=0;i<count;i++) {
            write(out, SPLIT_DIM);
            writeInt(out, splitPackedValues[i * (1 + fieldInfo.getPointNumBytes())] & 0xff);
            newline(out);
            write(out, SPLIT_VALUE);
            br = new BytesRef(splitPackedValues, 1+(i * (1+fieldInfo.getPointNumBytes())), fieldInfo.getPointNumBytes());
            write(out, br.toString());
            newline(out);
          }
        }

        @Override
        protected void writeLeafBlockDocs(IndexOutput out, int[] docIDs, int start, int count) throws IOException {
          write(out, BLOCK_COUNT);
          writeInt(out, count);
          newline(out);
          for(int i=0;i<count;i++) {
            write(out, BLOCK_DOC_ID);
            writeInt(out, docIDs[start+i]);
            newline(out);
          }
        }

        @Override
        protected void writeCommonPrefixes(IndexOutput out, int[] commonPrefixLengths, byte[] packedValue) {
          // NOTE: we don't do prefix coding, so we ignore commonPrefixLengths
        }

        @Override
        protected void writeLeafBlockPackedValues(IndexOutput out, int[] commonPrefixLengths, int count, int sortedDim, IntFunction<BytesRef> packedValues) throws IOException {
          for (int i = 0; i < count; ++i) {
            BytesRef packedValue = packedValues.apply(i);
            // NOTE: we don't do prefix coding, so we ignore commonPrefixLengths
            write(out, BLOCK_VALUE);
            write(out, packedValue.toString());
            newline(out);
          }
        }
      }) {

      values.intersect(fieldInfo.name, new IntersectVisitor() {
          @Override
          public void visit(int docID) {
            throw new IllegalStateException();
          }

          public void visit(int docID, byte[] packedValue) throws IOException {
            writer.add(packedValue, docID);
          }

          @Override
          public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            return Relation.CELL_CROSSES_QUERY;
          }
        });

      // We could have 0 points on merge since all docs with points may be deleted:
      if (writer.getPointCount() > 0) {
        indexFPs.put(fieldInfo.name, writer.finish(dataOut));
      }
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

  @Override
  public void finish() throws IOException {
    SimpleTextUtil.write(dataOut, END);
    SimpleTextUtil.writeNewline(dataOut);
    SimpleTextUtil.writeChecksum(dataOut, scratch);
  }

  @Override
  public void close() throws IOException {
    if (dataOut != null) {
      dataOut.close();
      dataOut = null;

      // Write index file
      String fileName = IndexFileNames.segmentFileName(writeState.segmentInfo.name, writeState.segmentSuffix, SimpleTextPointsFormat.POINT_INDEX_EXTENSION);
      try (IndexOutput indexOut = writeState.directory.createOutput(fileName, writeState.context)) {
        int count = indexFPs.size();
        write(indexOut, FIELD_COUNT);
        write(indexOut, Integer.toString(count));
        newline(indexOut);
        for(Map.Entry<String,Long> ent : indexFPs.entrySet()) {
          write(indexOut, FIELD_FP_NAME);
          write(indexOut, ent.getKey());
          newline(indexOut);
          write(indexOut, FIELD_FP);
          write(indexOut, Long.toString(ent.getValue()));
          newline(indexOut);
        }
        SimpleTextUtil.writeChecksum(indexOut, scratch);
      }
    }
  }
}
