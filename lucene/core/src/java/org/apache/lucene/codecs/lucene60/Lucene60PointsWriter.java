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
package org.apache.lucene.codecs.lucene60;


import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.bkd.BKDReader;
import org.apache.lucene.util.bkd.BKDWriter;

/** Writes dimensional values
 *
 * @lucene.experimental */
public class Lucene60PointsWriter extends PointsWriter implements Closeable {

  /** Output used to write the BKD tree data file */
  protected final IndexOutput dataOut;

  /** Maps field name to file pointer in the data file where the BKD index is located. */
  protected final Map<String,Long> indexFPs = new HashMap<>();

  final SegmentWriteState writeState;
  final int maxPointsInLeafNode;
  private boolean finished;

  /** Full constructor */
  public Lucene60PointsWriter(SegmentWriteState writeState, int maxPointsInLeafNode) throws IOException {
    assert writeState.fieldInfos.hasPointValues();
    this.writeState = writeState;
    this.maxPointsInLeafNode = maxPointsInLeafNode;
    String dataFileName = IndexFileNames.segmentFileName(writeState.segmentInfo.name,
                                                         writeState.segmentSuffix,
                                                         Lucene60PointsFormat.DATA_EXTENSION);
    dataOut = writeState.directory.createOutput(dataFileName, writeState.context);
    boolean success = false;
    try {
      CodecUtil.writeIndexHeader(dataOut,
                                 Lucene60PointsFormat.DATA_CODEC_NAME,
                                 Lucene60PointsFormat.DATA_VERSION_CURRENT,
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
  public Lucene60PointsWriter(SegmentWriteState writeState) throws IOException {
    this(writeState, BKDWriter.DEFAULT_MAX_POINTS_IN_LEAF_NODE);
  }

  @Override
  public void writeField(FieldInfo fieldInfo, PointsReader values, double maxMBSortInHeap) throws IOException {

    boolean singleValuePerDoc = values.size(fieldInfo.name) == values.getDocCount(fieldInfo.name);

    try (BKDWriter writer = new BKDWriter(writeState.segmentInfo.maxDoc(),
                                          writeState.directory,
                                          writeState.segmentInfo.name,
                                          fieldInfo.getPointDimensionCount(),
                                          fieldInfo.getPointNumBytes(),
                                          maxPointsInLeafNode,
                                          maxMBSortInHeap,
                                          values.size(fieldInfo.name),
                                          singleValuePerDoc)) {

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

      // We could have 0 points on merge since all docs with dimensional fields may be deleted:
      if (writer.getPointCount() > 0) {
        indexFPs.put(fieldInfo.name, writer.finish(dataOut));
      }
    }
  }

  @Override
  public void merge(MergeState mergeState) throws IOException {
    if (mergeState.segmentInfo.getIndexSort() != null) {
      // TODO: can we gain back some optos even if index is sorted?  E.g. if sort results in large chunks of contiguous docs from one sub
      // being copied over...?
      super.merge(mergeState);
      return;
    }

    for(PointsReader reader : mergeState.pointsReaders) {
      if (reader instanceof Lucene60PointsReader == false) {
        // We can only bulk merge when all to-be-merged segments use our format:
        super.merge(mergeState);
        return;
      }
    }
    for (PointsReader reader : mergeState.pointsReaders) {
      if (reader != null) {
        reader.checkIntegrity();
      }
    }

    for (FieldInfo fieldInfo : mergeState.mergeFieldInfos) {
      if (fieldInfo.getPointDimensionCount() != 0) {
        if (fieldInfo.getPointDimensionCount() == 1) {

          boolean singleValuePerDoc = true;

          // Worst case total maximum size (if none of the points are deleted):
          long totMaxSize = 0;
          for(int i=0;i<mergeState.pointsReaders.length;i++) {
            PointsReader reader = mergeState.pointsReaders[i];
            if (reader != null) {
              FieldInfos readerFieldInfos = mergeState.fieldInfos[i];
              FieldInfo readerFieldInfo = readerFieldInfos.fieldInfo(fieldInfo.name);
              if (readerFieldInfo != null) {
                totMaxSize += reader.size(fieldInfo.name);
                singleValuePerDoc &= reader.size(fieldInfo.name) == reader.getDocCount(fieldInfo.name);
              }
            }
          }

          //System.out.println("MERGE: field=" + fieldInfo.name);
          // Optimize the 1D case to use BKDWriter.merge, which does a single merge sort of the
          // already sorted incoming segments, instead of trying to sort all points again as if
          // we were simply reindexing them:
          try (BKDWriter writer = new BKDWriter(writeState.segmentInfo.maxDoc(),
                                                writeState.directory,
                                                writeState.segmentInfo.name,
                                                fieldInfo.getPointDimensionCount(),
                                                fieldInfo.getPointNumBytes(),
                                                maxPointsInLeafNode,
                                                // NOTE: not used, since BKDWriter.merge does a merge sort:
                                                BKDWriter.DEFAULT_MAX_MB_SORT_IN_HEAP,
                                                totMaxSize,
                                                singleValuePerDoc)) {
            List<BKDReader> bkdReaders = new ArrayList<>();
            List<MergeState.DocMap> docMaps = new ArrayList<>();
            for(int i=0;i<mergeState.pointsReaders.length;i++) {
              PointsReader reader = mergeState.pointsReaders[i];

              if (reader != null) {

                // we confirmed this up above
                assert reader instanceof Lucene60PointsReader;
                Lucene60PointsReader reader60 = (Lucene60PointsReader) reader;

                // NOTE: we cannot just use the merged fieldInfo.number (instead of resolving to this
                // reader's FieldInfo as we do below) because field numbers can easily be different
                // when addIndexes(Directory...) copies over segments from another index:


                FieldInfos readerFieldInfos = mergeState.fieldInfos[i];
                FieldInfo readerFieldInfo = readerFieldInfos.fieldInfo(fieldInfo.name);
                if (readerFieldInfo != null) {
                  BKDReader bkdReader = reader60.readers.get(readerFieldInfo.number);
                  if (bkdReader != null) {
                    bkdReaders.add(bkdReader);
                    docMaps.add(mergeState.docMaps[i]);
                  }
                }
              }
            }

            long fp = writer.merge(dataOut, docMaps, bkdReaders);
            if (fp != -1) {
              indexFPs.put(fieldInfo.name, fp);
            }
          }
        } else {
          mergeOneField(mergeState, fieldInfo);
        }
      }
    }

    finish();
  }

  @Override
  public void finish() throws IOException {
    if (finished) {
      throw new IllegalStateException("already finished");
    }
    finished = true;
    CodecUtil.writeFooter(dataOut);

    String indexFileName = IndexFileNames.segmentFileName(writeState.segmentInfo.name,
                                                          writeState.segmentSuffix,
                                                          Lucene60PointsFormat.INDEX_EXTENSION);
    // Write index file
    try (IndexOutput indexOut = writeState.directory.createOutput(indexFileName, writeState.context)) {
      CodecUtil.writeIndexHeader(indexOut,
                                 Lucene60PointsFormat.META_CODEC_NAME,
                                 Lucene60PointsFormat.INDEX_VERSION_CURRENT,
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

  @Override
  public void close() throws IOException {
    dataOut.close();
  }
}
