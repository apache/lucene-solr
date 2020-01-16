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
import java.util.Map;
import java.util.TreeMap;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnGraphReader;
import org.apache.lucene.codecs.KnnGraphWriter;
import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.KnnGraphValues;
import org.apache.lucene.index.KnnGraphValuesWriter;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntsRef;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Writes vector values and knn graphs to index segments.
 */
public final class Lucene90KnnGraphWriter extends KnnGraphWriter {

  private final IndexOutput meta, vectorData, graphData;

  private boolean finished;

  Lucene90KnnGraphWriter(SegmentWriteState state) throws IOException {
    assert state.fieldInfos.hasVectorValues();

    String metaFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, Lucene90KnnGraphFormat.META_EXTENSION);
    meta = state.directory.createOutput(metaFileName, state.context);

    String vectorDataFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, Lucene90KnnGraphFormat.VECTOR_DATA_EXTENSION);
    vectorData = state.directory.createOutput(vectorDataFileName, state.context);

    String graphDataFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, Lucene90KnnGraphFormat.GRAPH_DATA_EXTENSION);
    graphData = state.directory.createOutput(graphDataFileName, state.context);

    try {
      CodecUtil.writeIndexHeader(meta,
          Lucene90KnnGraphFormat.META_CODEC_NAME,
          Lucene90KnnGraphFormat.VERSION_CURRENT,
          state.segmentInfo.getId(), state.segmentSuffix);
      CodecUtil.writeIndexHeader(vectorData,
          Lucene90KnnGraphFormat.VECTOR_DATA_CODEC_NAME,
          Lucene90KnnGraphFormat.VERSION_CURRENT,
          state.segmentInfo.getId(), state.segmentSuffix);
      CodecUtil.writeIndexHeader(graphData,
          Lucene90KnnGraphFormat.GRAPH_DATA_CODEC_NAME,
          Lucene90KnnGraphFormat.VERSION_CURRENT,
          state.segmentInfo.getId(), state.segmentSuffix);
    } catch (IOException e) {
      IOUtils.closeWhileHandlingException(this);
    }
  }

  @Override
  public void writeField(FieldInfo fieldInfo, KnnGraphReader values) throws IOException {
    // we require a TreeMap so that we can iterate over docid keys in order. Instead we should move
    // to the IndexedDISI encoding used by BinaryDocValues
    Map<Integer, Long> docToOffset = new TreeMap<>();
    List<Integer> enterPoints = new ArrayList<>();
    long vectorDataOffset = vectorData.getFilePointer();
    long graphDataOffset = graphData.getFilePointer();

    int numDims = fieldInfo.getVectorNumDimensions();

    VectorValues vectors = values.getVectorValues(fieldInfo.name);
    KnnGraphValues graph = values.getGraphValues(fieldInfo.name);

    int docV, docG;
    for (docV = vectors.nextDoc(), docG = graph.nextDoc();
         docV != NO_MORE_DOCS && docG != NO_MORE_DOCS;
         docV = vectors.nextDoc(), docG = graph.nextDoc()) {
      assert docV == docG;  // must be same

      writeVectorValue(numDims, vectors);

      // write knn graph value
      docToOffset.put(docG, graphData.getFilePointer() - graphDataOffset);

      int maxLevel = graph.getMaxLevel();
      assert maxLevel >= 0;
      graphData.writeInt(maxLevel);
      for (int l = graph.getMaxLevel(); l >= 0; l--) {
        IntsRef friends = graph.getFriends(l);
        // assert friends.length > 0 : "doc " + docG + " has empty friends list at level=" + l;
        graphData.writeInt(friends.length);
        if (friends.length > 0) {
          int stop = friends.offset + friends.length;
          // sort friend ids
          Arrays.sort(friends.ints, friends.offset, stop);
          // write the smallest friend id
          graphData.writeVInt(friends.ints[friends.offset]);
          for (int i = friends.offset + 1; i < stop; i++) {
            // write delta
            int delta = friends.ints[i] - friends.ints[i - 1];
            assert delta > 0;
            graphData.writeVInt(delta);
          }
        }
      }
    }
    assert docV == NO_MORE_DOCS;  // must be exhausted
    assert docG == NO_MORE_DOCS;    // must be exhausted

    long vectorDataLength = vectorData.getFilePointer() - vectorDataOffset;
    long graphDataLength = graphData.getFilePointer() - graphDataOffset;

    writeMeta(fieldInfo, vectorDataOffset, vectorDataLength, graphDataOffset, graphDataLength, graph.getTopLevel(), graph.getEnterPoints(), docToOffset);
  }

  private void writeVectorValue(int numDims, VectorValues vectors) throws IOException {
    // write vector value
    BytesRef binaryValue = vectors.binaryValue();
    VectorValues.verifyNumDimensions(binaryValue.length, numDims);
    vectorData.writeBytes(binaryValue.bytes, binaryValue.offset, binaryValue.length);
  }

  private void writeMeta(FieldInfo field, long vectorDataOffset, long vectorDataLength, long graphDataOffset, long graphDataLength,
                         int topLevel, int[] eps, Map<Integer, Long> docToOffset) throws IOException {
    meta.writeInt(field.number);
    meta.writeVLong(vectorDataOffset);
    meta.writeVLong(vectorDataLength);
    meta.writeVLong(graphDataOffset);
    meta.writeVLong(graphDataLength);
    meta.writeInt(topLevel);
    meta.writeInt(eps.length);
    for (Integer ep : eps) {
      meta.writeVInt(ep);
    }
    meta.writeInt(docToOffset.size());
    for (Map.Entry<Integer, Long> entry : docToOffset.entrySet()) {
      // these are not in sorted order, yet we write the vectors in order by docid
      meta.writeVInt(entry.getKey());
      meta.writeVLong(entry.getValue());
    }
  }

  @Override
  public void merge(MergeState mergeState) throws IOException {
    for (KnnGraphReader reader : mergeState.knnGraphReaders) {
      if (reader != null) {
        reader.checkIntegrity();
      }
    }
    for (FieldInfo fieldInfo : mergeState.mergeFieldInfos) {
      if (fieldInfo.hasVectorValues()) {
        mergeKnnGraph(fieldInfo, mergeState);
      }
    }
    finish();
  }

  /**
   * Merges the segment HNSW graphs by constructing a new merged graph using HNSWGraph
   */
  private void mergeKnnGraph(FieldInfo mergeFieldInfo, final MergeState mergeState) throws IOException {
    // We must compute the entire merged field in memory since each document's values depend on its neighbors
    //mergeState.infoStream.message("ReferenceDocValues", "merging " + mergeState.segmentInfo);
    List<VectorValuesSub> subs = new ArrayList<>();
    int dimension = -1;
    for (int i = 0 ; i < mergeState.knnGraphReaders.length; i++) {
      KnnGraphReader graphReader = mergeState.knnGraphReaders[i];
      if (graphReader != null) {
        if (mergeFieldInfo != null && mergeFieldInfo.hasVectorValues()) {
          int segmentDimension = mergeFieldInfo.getVectorNumDimensions();
          if (dimension == -1) {
            dimension = segmentDimension;
          } else if (dimension != segmentDimension) {
            throw new IllegalStateException("Varying dimensions for vector-valued field " + mergeFieldInfo.name
                + ": " + dimension + "!=" + segmentDimension);
          }
          VectorValues values = graphReader.getVectorValues(mergeFieldInfo.name);
          subs.add(new VectorValuesSub(i, mergeState.docMaps[i], values));
        }
      }
    }
    // Create a new KnnGraphValues by iterating over the vectors, searching for
    // its nearest neighbor vectors in the newly merged segments' vectors, mapping the resulting
    // docids using docMaps in the mergeState.
    MultiVectorValues multiVectors = new MultiVectorValues(subs, mergeState.maxDocs);
    KnnGraphValuesWriter valuesWriter = new KnnGraphValuesWriter(mergeFieldInfo, Counter.newCounter());
    for (int i = 0; i < subs.size(); i++) {
      VectorValuesSub sub = subs.get(i);
      MergeState.DocMap docMap = mergeState.docMaps[sub.segmentIndex];
      // nocommit: test sorted index and test index with deletions
      int docid;
      while ((docid = sub.nextDoc()) != NO_MORE_DOCS) {
        int mappedDocId = docMap.get(docid);
        /// deleted document (not alive)
        if (mappedDocId == -1) {
          continue;
        }
        assert sub.values.docID() == docid;
        valuesWriter.addValue(mappedDocId, sub.values.binaryValue());
      }
    }
    valuesWriter.flush(null, this);
    //writeField(mergeFieldInfo, valuesWriter.getGraphValues(), valuesWriter.getVectorValues());
    //mergeState.infoStream.message("ReferenceDocValues", " mergeReferenceField done: " + mergeState.segmentInfo);
  }

  /** Tracks state of one binary sub-reader that we are merging */
  private static class VectorValuesSub extends DocIDMerger.Sub {

    final VectorValues values;
    final int segmentIndex;

    VectorValuesSub(int segmentIndex, MergeState.DocMap docMap, VectorValues values) {
      super(docMap);
      this.values = values;
      this.segmentIndex = segmentIndex;
      assert values.docID() == -1;
    }

    @Override
    public int nextDoc() throws IOException {
      return values.nextDoc();
    }
  }

  // provides a view over multiple VectorValues by concatenating their docid spaces
  private static class MultiVectorValues extends VectorValues {
    private final VectorValuesSub[] subValues;
    private final int[] docBase;
    private final int[] segmentMaxDocs;
    private final int cost;

    private int whichSub;

    MultiVectorValues(List<VectorValuesSub> subs, int[] maxDocs) throws IOException {
      this.subValues = new VectorValuesSub[subs.size()];
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
      int totalCost = 0;
      for (VectorValuesSub sub : subs) {
        totalCost += sub.values.cost();
        this.subValues[i++] = sub;
      }
      cost = totalCost;
      whichSub = 0;
    }

    @Override
    public long cost() {
      return cost;
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
          return NO_MORE_DOCS;
        }
      }
      return docBase[whichSub] + segmentDocId;
    }

    @Override
    public boolean seek(int target) {
      throw new UnsupportedOperationException();
    }

    @Override
    public float[] vectorValue() throws IOException {
      return subValues[whichSub].values.vectorValue();
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
    public int docID() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int nextDoc() throws IOException {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public void finish() throws IOException {
    if (finished) {
      throw new IllegalStateException("already finished");
    }
    finished = true;

    if (meta != null) {
      meta.writeInt(-1);
      CodecUtil.writeFooter(meta);
    }
    if (vectorData != null) {
      CodecUtil.writeFooter(vectorData);
    }
    if (graphData != null) {
      CodecUtil.writeFooter(graphData);
    }
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(meta, vectorData, graphData);
  }
}
