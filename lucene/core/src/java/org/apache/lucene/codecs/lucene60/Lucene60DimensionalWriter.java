package org.apache.lucene.codecs.lucene60;


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

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DimensionalReader;
import org.apache.lucene.codecs.DimensionalWriter;
import org.apache.lucene.index.DimensionalValues.IntersectVisitor;
import org.apache.lucene.index.DimensionalValues.Relation;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.bkd.BKDWriter;

/** Writes dimensional values */
public class Lucene60DimensionalWriter extends DimensionalWriter implements Closeable {
  
  final IndexOutput dataOut;
  final Map<String,Long> indexFPs = new HashMap<>();
  final SegmentWriteState writeState;
  final int maxPointsInLeafNode;
  final double maxMBSortInHeap;
  private boolean closed;

  /** Full constructor */
  public Lucene60DimensionalWriter(SegmentWriteState writeState, int maxPointsInLeafNode, double maxMBSortInHeap) throws IOException {
    assert writeState.fieldInfos.hasDimensionalValues();
    this.writeState = writeState;
    this.maxPointsInLeafNode = maxPointsInLeafNode;
    this.maxMBSortInHeap = maxMBSortInHeap;
    String dataFileName = IndexFileNames.segmentFileName(writeState.segmentInfo.name,
                                                         writeState.segmentSuffix,
                                                         Lucene60DimensionalFormat.DATA_EXTENSION);
    dataOut = writeState.directory.createOutput(dataFileName, writeState.context);
    boolean success = false;
    try {
      CodecUtil.writeIndexHeader(dataOut,
                                 Lucene60DimensionalFormat.CODEC_NAME,
                                 Lucene60DimensionalFormat.DATA_VERSION_CURRENT,
                                 writeState.segmentInfo.getId(),
                                 writeState.segmentSuffix);
      success = true;
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(dataOut);
      }
    }
  }

  /** Uses the defaults values for {@code maxPointsInLeafNode} (1024) and {@code maxMBSortInHeap} (16.0) */
  public Lucene60DimensionalWriter(SegmentWriteState writeState) throws IOException {
    this(writeState, BKDWriter.DEFAULT_MAX_POINTS_IN_LEAF_NODE, BKDWriter.DEFAULT_MAX_MB_SORT_IN_HEAP);
  }

  @Override
  public void writeField(FieldInfo fieldInfo, DimensionalReader values) throws IOException {

    try (BKDWriter writer = new BKDWriter(writeState.directory,
                                          writeState.segmentInfo.name,
                                          fieldInfo.getDimensionCount(),
                                          fieldInfo.getDimensionNumBytes(),
                                          maxPointsInLeafNode,
                                          maxMBSortInHeap)) {

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
            return Relation.QUERY_CROSSES_CELL;
          }
        });

      indexFPs.put(fieldInfo.name, writer.finish(dataOut));
    }
  }


  @Override
  public void close() throws IOException {
    if (closed == false) {
      CodecUtil.writeFooter(dataOut);
      dataOut.close();
      closed = true;

      String indexFileName = IndexFileNames.segmentFileName(writeState.segmentInfo.name,
                                                            writeState.segmentSuffix,
                                                            Lucene60DimensionalFormat.INDEX_EXTENSION);
      // Write index file
      try (IndexOutput indexOut = writeState.directory.createOutput(indexFileName, writeState.context)) {
        CodecUtil.writeIndexHeader(indexOut,
                                   Lucene60DimensionalFormat.CODEC_NAME,
                                   Lucene60DimensionalFormat.INDEX_VERSION_CURRENT,
                                   writeState.segmentInfo.getId(),
                                   writeState.segmentSuffix);
        int count = indexFPs.size();
        indexOut.writeVInt(count);
        for(Map.Entry<String,Long> ent : indexFPs.entrySet()) {
          FieldInfo fieldInfo = writeState.fieldInfos.fieldInfo(ent.getKey());
          if (fieldInfo == null) {
            throw new IllegalStateException("wrote field=\"" + ent.getKey() + "\" but that field doesn't exist in FieldInfos");
          }
          indexOut.writeVInt(fieldInfo.number);
          indexOut.writeVLong(ent.getValue());
        }
        CodecUtil.writeFooter(indexOut);
      }
    }
  }
}
