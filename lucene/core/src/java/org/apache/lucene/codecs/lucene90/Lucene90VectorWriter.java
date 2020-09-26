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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.VectorReader;
import org.apache.lucene.codecs.VectorWriter;
import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Writes vector values and knn graphs to index segments.
 */
public final class Lucene90VectorWriter extends VectorWriter {

  private final IndexOutput meta, vectorData;

  private boolean finished;

  Lucene90VectorWriter(SegmentWriteState state) throws IOException {
    assert state.fieldInfos.hasVectorValues();

    String metaFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, Lucene90VectorFormat.META_EXTENSION);
    meta = state.directory.createOutput(metaFileName, state.context);

    String vectorDataFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, Lucene90VectorFormat.VECTOR_DATA_EXTENSION);
    vectorData = state.directory.createOutput(vectorDataFileName, state.context);

    try {
      CodecUtil.writeIndexHeader(meta,
          Lucene90VectorFormat.META_CODEC_NAME,
          Lucene90VectorFormat.VERSION_CURRENT,
          state.segmentInfo.getId(), state.segmentSuffix);
      CodecUtil.writeIndexHeader(vectorData,
          Lucene90VectorFormat.VECTOR_DATA_CODEC_NAME,
          Lucene90VectorFormat.VERSION_CURRENT,
          state.segmentInfo.getId(), state.segmentSuffix);
    } catch (IOException e) {
      IOUtils.closeWhileHandlingException(this);
    }
  }

  @Override
  public void writeField(FieldInfo fieldInfo, VectorValues vectors) throws IOException {
    long vectorDataOffset = vectorData.getFilePointer();
    VectorValues v2 = vectors.copy();
    // TODO - use a better data structure; a bitset? DocsWithFieldSet is p.p. in o.a.l.index
    List<Integer> docIds = new ArrayList<>();
    int docV, ord = 0;
    for (docV = v2.nextDoc(); docV != NO_MORE_DOCS; docV = v2.nextDoc(), ord++) {
      writeVectorValue(v2);
      docIds.add(docV);
      // TODO: write knn graph value
    }
    long vectorDataLength = vectorData.getFilePointer() - vectorDataOffset;
    writeMeta(fieldInfo, vectorDataOffset, vectorDataLength, docIds);
  }

  private void writeVectorValue(VectorValues vectors) throws IOException {
    // write vector value
    BytesRef binaryValue = vectors.binaryValue();
    assert binaryValue.length == vectors.dimension() * Float.BYTES;
    vectorData.writeBytes(binaryValue.bytes, binaryValue.offset, binaryValue.length);
  }

  private void writeMeta(FieldInfo field, long vectorDataOffset, long vectorDataLength, List<Integer> docIds) throws IOException {
    meta.writeInt(field.number);
    meta.writeInt(field.getVectorScoreFunction().id);
    meta.writeVLong(vectorDataOffset);
    meta.writeVLong(vectorDataLength);
    meta.writeInt(field.getVectorDimension());
    meta.writeInt(docIds.size());
    for (Integer docId : docIds) {
      // TODO: delta-encode, or write as bitset
      meta.writeVInt(docId);
    }
  }

  @Override
  public void merge(MergeState mergeState) throws IOException {
    for (VectorReader reader : mergeState.vectorReaders) {
      if (reader != null) {
        reader.checkIntegrity();
      }
    }
    for (FieldInfo fieldInfo : mergeState.mergeFieldInfos) {
      if (fieldInfo.hasVectorValues()) {
        mergeVectors(fieldInfo, mergeState);
      }
    }
    finish();
  }

  /**
   * TODO: merge the segment HNSW graphs by constructing a new merged graph using HNSWGraph
   */
  private void mergeVectors(FieldInfo mergeFieldInfo, final MergeState mergeState) throws IOException {
    // We must compute the entire merged field in memory since each document's values depend on its neighbors
    if (mergeState.infoStream.isEnabled("KNN")) {
        mergeState.infoStream.message("KNN", "merging " + mergeState.segmentInfo);
    }
    List<VectorValuesSub> subs = new ArrayList<>();
    int dimension = -1;
    VectorValues.ScoreFunction scoreFunction = null;
    for (int i = 0; i < mergeState.vectorReaders.length; i++) {
      VectorReader graphReader = mergeState.vectorReaders[i];
      if (graphReader != null) {
        if (mergeFieldInfo != null && mergeFieldInfo.hasVectorValues()) {
          int segmentDimension = mergeFieldInfo.getVectorDimension();
          VectorValues.ScoreFunction segmentScoreFunction = mergeFieldInfo.getVectorScoreFunction();
          if (dimension == -1) {
            dimension = segmentDimension;
            scoreFunction = mergeFieldInfo.getVectorScoreFunction();
          } else if (dimension != segmentDimension) {
            throw new IllegalStateException("Varying dimensions for vector-valued field " + mergeFieldInfo.name
                + ": " + dimension + "!=" + segmentDimension);
          } else if (scoreFunction != segmentScoreFunction) {
            throw new IllegalStateException("Varying score functions for vector-valued field " + mergeFieldInfo.name
                + ": " + scoreFunction + "!=" + segmentScoreFunction);
          }
          VectorValues values = graphReader.getVectorValues(mergeFieldInfo.name);
          subs.add(new VectorValuesSub(i, mergeState.docMaps[i], values));
        }
      }
    }
    // Create a new VectorValues by iterating over the sub vectors, mapping the resulting
    // docids using docMaps in the mergeState.
    MultiVectorValues multiVectors = new MultiVectorValues(subs, mergeState.maxDocs, dimension, scoreFunction);
    // TODO: handle sorted index and test index with deletions
    writeField(mergeFieldInfo, multiVectors);
    if (mergeState.infoStream.isEnabled("KNN")) {
        mergeState.infoStream.message("KNN", "merge done " + mergeState.segmentInfo);
    }
  }

  /** Tracks state of one sub-reader that we are merging */
  private static class VectorValuesSub extends DocIDMerger.Sub {

    final MergeState.DocMap docMap;
    final VectorValues values;
    final int segmentIndex;

    VectorValuesSub(int segmentIndex, MergeState.DocMap docMap, VectorValues values) {
      super(docMap);
      this.values = values;
      this.segmentIndex = segmentIndex;
      this.docMap = docMap;
      assert values.docID() == -1;
    }

    @Override
    public int nextDoc() throws IOException {
      return values.nextDoc();
    }

  }

  // NOTE: we do two very different things with this right now:
  // 1) iterate over the documents in order, forward-only, tracking docId
  // 2) fetch values random-access style, by *ordinal*
  //
  // instead we should optimize for these with two separate VectorValues
  // we don't want the seeking to mess up the forward-only iteration by mixing these

  // provides a view over multiple VectorValues by concatenating their docid spaces
  private static class MultiVectorValues extends VectorValues {
    private final VectorValuesSub[] subValues;
    private final int[] maxDocs;
    private final int[] docBase;
    private final int[] segmentMaxDocs;
    private final int[] nonEmptySegment;
    private final int[] ordBase;
    private final int cost;
    private final int size;
    private final int dimension;
    private final ScoreFunction scoreFunction;

    private int docId;
    private int whichSub;

    MultiVectorValues(List<VectorValuesSub> subs, int[] maxDocs, int dimension, VectorValues.ScoreFunction scoreFunction) {
      this.subValues = new VectorValuesSub[subs.size()];
      this.maxDocs = maxDocs;
      this.dimension = dimension;
      this.scoreFunction = scoreFunction;
      // TODO: this complicated logic needs its own test
      // maxDocs actually says *how many* docs there are, not what the number of the max doc is
      int maxDoc = -1;
      int lastMaxDoc = -1;
      segmentMaxDocs = new int[subs.size() - 1];
      docBase = new int[subs.size()];
      for (int i = 0, j = 0; j < subs.size(); i++) {
        lastMaxDoc = maxDoc;
        maxDoc += maxDocs[i];
        if (i == subs.get(j).segmentIndex) {
          // we may skip some segments if they have no docs with values for this field
          if (j > 0) {
            segmentMaxDocs[j - 1] = lastMaxDoc;
          }
          docBase[j] = lastMaxDoc + 1;
          ++j;
        }
      }
      int i = 0;
      int totalCost = 0, totalSize = 0, nonEmptySegmentsCount = 0;
      for (VectorValuesSub sub : subs) {
        totalCost += sub.values.cost();
        int sz = sub.values.size();
        totalSize += sz;
        if (sz > 0) {
          nonEmptySegmentsCount++;
        }
        this.subValues[i++] = sub;
      }
      cost = totalCost;
      size = totalSize;
      nonEmptySegment = new int[nonEmptySegmentsCount];
      ordBase = new int[nonEmptySegmentsCount];
      int nonEmptySegmentOrd = 0, lastBase = 0;
      for (int k = 0; k < subs.size(); k++) {
        if (subs.get(k).values.size() > 0) {
          nonEmptySegment[nonEmptySegmentOrd] = k;
          int size = subs.get(k).values.size();
          ordBase[nonEmptySegmentOrd] = lastBase;
          lastBase += size;
          nonEmptySegmentOrd++;
        }
      }
      whichSub = 0;
      docId = -1;
    }

    public MultiVectorValues copy() {
       List<VectorValuesSub> subCopies= new ArrayList<>();
       for (VectorValuesSub sub : subValues) {
           subCopies.add(new VectorValuesSub(sub.segmentIndex, sub.docMap, sub.values.copy()));
       }
       return new MultiVectorValues(subCopies, maxDocs, dimension, scoreFunction);
    }

    @Override
    public int advance(int target) throws IOException {
      int rebased = unmapSettingWhich(target);
      if (rebased < 0) {
        rebased = 0;
      }
      int segmentDocId = subValues[whichSub].values.advance(rebased);
      if (segmentDocId == NO_MORE_DOCS) {
        if (++whichSub < subValues.length) {
          // Get the first document in the next segment; Note that all segments have values.
          segmentDocId = subValues[whichSub].values.advance(0);
        } else {
          return docId = NO_MORE_DOCS;
        }
      }
      docId = docBase[whichSub] + segmentDocId;
      return docId;
    }

    @Override
    public float[] vectorValue() throws IOException {
      return subValues[whichSub].values.vectorValue();
    }

    @Override
    public BytesRef binaryValue() throws IOException {
      return subValues[whichSub].values.binaryValue();
    }

    int unmap(int docid) {
      // map from global (merged) to segment-local (unmerged)
      // like mapDocid but no side effect - used for assertion
      return docid - docBase[findSegment(docid)];
    }

    private int unmapSettingWhich(int target) {
      whichSub = findSegment(target);
      return target - docBase[whichSub];
    }

    private int findSegment(int docid) {
      int segment = Arrays.binarySearch(segmentMaxDocs, docid);
      if (segment < 0) {
        return -1 - segment;
      } else {
        return segment;
      }
    }

    @Override
    public int dimension() {
      return dimension;
    }

    @Override
    public int size() {
      return size;
    }

    @Override
    public long cost() {
      return cost;
    }

    @Override
    public VectorValues.ScoreFunction scoreFunction() {
      return scoreFunction;
    }

    @Override
    public float[] vectorValue(int target) throws IOException {
      int segmentOrd = Arrays.binarySearch(ordBase, target);
      if (segmentOrd < 0) {
        // get the index of the greatest lower bound
        segmentOrd = -2 - segmentOrd;
      }
      int segment = nonEmptySegment[segmentOrd];
      return subValues[segment].values.vectorValue(target - ordBase[segmentOrd]);
    }

    @Override
    public TopDocs search(float[] target, int k, int fanout) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int docID() {
      return docId;
    }

    @Override
    public int nextDoc() throws IOException {
      // TODO: make this faster, avoiding binary search in advance (which supports random access)
      return advance(docId + 1);
    }
  }

  @Override
  public void finish() throws IOException {
    if (finished) {
      throw new IllegalStateException("already finished");
    }
    finished = true;

    if (meta != null) {
      // write end of fields marker
      meta.writeInt(-1);
      CodecUtil.writeFooter(meta);
    }
    if (vectorData != null) {
      CodecUtil.writeFooter(vectorData);
    }
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(meta, vectorData);
  }
}
