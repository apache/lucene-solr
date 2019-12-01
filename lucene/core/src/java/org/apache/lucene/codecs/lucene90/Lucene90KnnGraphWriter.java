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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnGraphReader;
import org.apache.lucene.codecs.KnnGraphWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.KnnGraphValues;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntsRef;

/**
 * Writes vector values and knn graphs to index segments.
 */
public final class Lucene90KnnGraphWriter extends KnnGraphWriter {

  private final IndexOutput meta, vectorData, graphData;

  private boolean finished;

  public Lucene90KnnGraphWriter(SegmentWriteState state) throws IOException {
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
    Map<Integer, Long> docToOffset = new HashMap<>();
    List<Integer> enterPoints = new ArrayList<>();
    long vectorDataOffset = vectorData.getFilePointer();
    long graphDataOffset = graphData.getFilePointer();

    int numDims = fieldInfo.getVectorNumDimensions();

    VectorValues vectors = values.getVectorValues(fieldInfo.name);
    KnnGraphValues graph = values.getGraphValues(fieldInfo.name);

    for (int docV = vectors.nextDoc(), docG = graph.nextDoc();
         docV != DocIdSetIterator.NO_MORE_DOCS && docG != DocIdSetIterator.NO_MORE_DOCS;
         docV = vectors.nextDoc(), docG = graph.nextDoc()) {
      assert docV == docG;  // must be same

      // write vector value
      byte[] binaryValue = vectors.binaryValue();
      VectorValues.verifyNumDimensions(binaryValue, numDims);
      vectorData.writeBytes(binaryValue, binaryValue.length);

      // write knn graph value
      docToOffset.put(docG, graphData.getFilePointer() - graphDataOffset);
      if (graph.isEnterPoint()) {
        enterPoints.add(docG);
      }
      graphData.writeInt(graph.getMaxLevel());
      for (int l = graph.getMaxLevel(); l >= 0; l--) {
        IntsRef friends = graph.getFriends(l);
        assert friends.length > 0;
        graphData.writeInt(friends.length);
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
    assert vectors.nextDoc() == DocIdSetIterator.NO_MORE_DOCS;  // must be exhausted
    assert graph.nextDoc() == DocIdSetIterator.NO_MORE_DOCS;    // must be exhausted

    long vectorDataLength = vectorData.getFilePointer() - vectorDataOffset;
    long graphDataLength = graphData.getFilePointer() - graphDataOffset;

    writeMeta(fieldInfo, vectorDataOffset, vectorDataLength, graphDataOffset, graphDataLength, graph.getTopLevel(), enterPoints, docToOffset);
  }

  private void writeMeta(FieldInfo field, long vectorDataOffset, long vectorDataLength, long graphDataOffset, long graphDataLength,
                         int topLevel, List<Integer> eps, Map<Integer, Long> docToOffset) throws IOException {
    meta.writeInt(field.number);
    meta.writeVLong(vectorDataOffset);
    meta.writeVLong(vectorDataLength);
    meta.writeVLong(graphDataOffset);
    meta.writeVLong(graphDataLength);
    meta.writeInt(topLevel);
    meta.writeInt(eps.size());
    for (Integer ep : eps) {
      meta.writeVInt(ep);
    }
    meta.writeInt(docToOffset.size());
    for (Map.Entry<Integer, Long> entry : docToOffset.entrySet()) {
      meta.writeVInt(entry.getKey());
      meta.writeVLong(entry.getValue());
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
