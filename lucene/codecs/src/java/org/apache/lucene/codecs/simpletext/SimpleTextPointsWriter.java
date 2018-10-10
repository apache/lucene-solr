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

import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

class SimpleTextPointsWriter extends PointsWriter {

  public final static BytesRef NUM_DIMS      = new BytesRef("num dims ");
  public final static BytesRef BYTES_PER_DIM = new BytesRef("bytes per dim ");
  public final static BytesRef MAX_LEAF_POINTS = new BytesRef("max leaf points ");
  public final static BytesRef INDEX_COUNT = new BytesRef("index count ");
  public final static BytesRef BLOCK_COUNT   = new BytesRef("block count ");
  public final static BytesRef BLOCK_DOC_ID  = new BytesRef("  doc ");
  public final static BytesRef BLOCK_FP      = new BytesRef("  block fp ");
  public final static BytesRef BLOCK_VALUE   = new BytesRef("  block value ");
  public final static BytesRef SPLIT_COUNT   = new BytesRef("split count ");
  public final static BytesRef SPLIT_DIM     = new BytesRef("  split dim ");
  public final static BytesRef SPLIT_VALUE   = new BytesRef("  split value ");
  public final static BytesRef FIELD_COUNT   = new BytesRef("field count ");
  public final static BytesRef FIELD_FP_NAME = new BytesRef("  field fp name ");
  public final static BytesRef FIELD_FP      = new BytesRef("  field fp ");
  public final static BytesRef MIN_VALUE     = new BytesRef("min value ");
  public final static BytesRef MAX_VALUE     = new BytesRef("max value ");
  public final static BytesRef POINT_COUNT   = new BytesRef("point count ");
  public final static BytesRef DOC_COUNT     = new BytesRef("doc count ");
  public final static BytesRef END           = new BytesRef("END");

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
  public void writeField(FieldInfo fieldInfo, PointsReader reader) throws IOException {

    PointValues values = reader.getValues(fieldInfo.name);
    boolean singleValuePerDoc = values.size() == values.getDocCount();

    // We use our own fork of the BKDWriter to customize how it writes the index and blocks to disk:
    try (SimpleTextBKDWriter writer = new SimpleTextBKDWriter(writeState.segmentInfo.maxDoc(),
                                                              writeState.directory,
                                                              writeState.segmentInfo.name,
                                                              fieldInfo.getPointDimensionCount(),
                                                              fieldInfo.getPointNumBytes(),
                                                              SimpleTextBKDWriter.DEFAULT_MAX_POINTS_IN_LEAF_NODE,
                                                              SimpleTextBKDWriter.DEFAULT_MAX_MB_SORT_IN_HEAP,
                                                              values.size(),
                                                              singleValuePerDoc)) {

      values.intersect(new IntersectVisitor() {
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
