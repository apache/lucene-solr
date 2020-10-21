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

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.VectorWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.apache.lucene.util.hnsw.HnswGraphBuilder;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

// nocommit - separate out graph writer. switch based on score function. store in an extensible way so we can add vector index structures
/**
 * Writes vector values and knn graphs to index segments.
 * @lucene.experimental
 */
public final class Lucene90VectorWriter extends VectorWriter {

  private final SegmentWriteState state;
  private final String vectorDataFileName;
  private final IndexOutput meta, vectorData, graphData;

  private boolean finished;

  Lucene90VectorWriter(SegmentWriteState state) throws IOException {
    assert state.fieldInfos.hasVectorValues();
    this.state = state;

    String metaFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, Lucene90VectorFormat.META_EXTENSION);
    meta = state.directory.createOutput(metaFileName, state.context);

    vectorDataFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, Lucene90VectorFormat.VECTOR_DATA_EXTENSION);
    vectorData = state.directory.createOutput(vectorDataFileName, state.context);

    String graphDataFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, Lucene90VectorFormat.GRAPH_DATA_EXTENSION);
    graphData = state.directory.createOutput(graphDataFileName, state.context);

    try {
      CodecUtil.writeIndexHeader(meta,
          Lucene90VectorFormat.META_CODEC_NAME,
          Lucene90VectorFormat.VERSION_CURRENT,
          state.segmentInfo.getId(), state.segmentSuffix);
      CodecUtil.writeIndexHeader(vectorData,
          Lucene90VectorFormat.VECTOR_DATA_CODEC_NAME,
          Lucene90VectorFormat.VERSION_CURRENT,
          state.segmentInfo.getId(), state.segmentSuffix);
      CodecUtil.writeIndexHeader(graphData,
          Lucene90VectorFormat.GRAPH_DATA_CODEC_NAME,
          Lucene90VectorFormat.VERSION_CURRENT,
          state.segmentInfo.getId(), state.segmentSuffix);
    } catch (IOException e) {
      IOUtils.closeWhileHandlingException(this);
    }
  }

  @Override
  public void writeField(FieldInfo fieldInfo, VectorValues vectors) throws IOException {
    long vectorDataOffset = vectorData.getFilePointer();

    // TODO - use a better data structure; a bitset? DocsWithFieldSet is p.p. in o.a.l.index
    int[] docIds = new int[vectors.size()];
    int count = 0;
    for (int docV = vectors.nextDoc(); docV != NO_MORE_DOCS; docV = vectors.nextDoc(), count++) {
      // write vector
      writeVectorValue(vectors);
      docIds[count] = docV;
    }
    long[] offsets = new long[count];
    long vectorDataLength = vectorData.getFilePointer() - vectorDataOffset;
    long graphDataOffset = graphData.getFilePointer();
    if (vectors.searchStrategy() != VectorValues.SearchStrategy.NONE) {
      writeGraph(vectors, graphDataOffset, offsets, count);
    }
    long graphDataLength = graphData.getFilePointer() - graphDataOffset;
    if (vectorDataLength > 0) {
      writeMeta(fieldInfo, vectorDataOffset, vectorDataLength, graphDataOffset, graphDataLength, docIds, offsets);
    }
  }

  private void writeMeta(FieldInfo field, long vectorDataOffset, long vectorDataLength, long graphDataOffset, long graphDataLength, int[] docIds, long[] offsets) throws IOException {
    meta.writeInt(field.number);
    meta.writeInt(field.getVectorSearchStrategy().ordinal());
    meta.writeVLong(vectorDataOffset);
    meta.writeVLong(vectorDataLength);
    meta.writeVLong(graphDataOffset);
    meta.writeVLong(graphDataLength);
    meta.writeInt(field.getVectorDimension());
    int size = offsets.length;
    meta.writeInt(size);
    for (int i = 0; i < size; i ++) {
      // TODO: delta-encode, or write as bitset
      meta.writeVInt(docIds[i]);
    }
    int i = 0;
    for (long offset : offsets) {
      // TODO: delta-encode
      meta.writeVLong(offset);
    }
  }

  private void writeVectorValue(VectorValues vectors) throws IOException {
    // write vector value
    BytesRef binaryValue = vectors.binaryValue();
    assert binaryValue.length == vectors.dimension() * Float.BYTES;
    vectorData.writeBytes(binaryValue.bytes, binaryValue.offset, binaryValue.length);
  }

  private void writeGraph(VectorValues vectorValues, long graphDataOffset, long[] offsets, int count) throws IOException {
    HnswGraph graph = HnswGraphBuilder.build(vectorValues);
    for (int ord = 0; ord < count; ord++) {
      // write graph
      offsets[ord] = graphData.getFilePointer() - graphDataOffset;
      int[] arcs = graph.getFriends(ord);
      graphData.writeInt(arcs.length);
      int lastArc = -1;         // to make the assertion work?
      for (int arc : arcs) {
        assert arc > lastArc : "arcs out of order: " + lastArc + "," + arc;
        graphData.writeVInt(arc - lastArc);
        lastArc = arc;
      }
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
      CodecUtil.writeFooter(graphData);
    }
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(meta, vectorData, graphData);
  }
}
