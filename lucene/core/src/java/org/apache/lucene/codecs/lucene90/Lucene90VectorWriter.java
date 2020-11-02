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
import java.util.List;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.VectorWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Writes vector values and knn graphs to index segments.
 * @lucene.experimental
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
    // TODO - use a better data structure; a bitset? DocsWithFieldSet is p.p. in o.a.l.index
    List<Integer> docIds = new ArrayList<>();
    int docV, ord = 0;
    for (docV = vectors.nextDoc(); docV != NO_MORE_DOCS; docV = vectors.nextDoc(), ord++) {
      writeVectorValue(vectors);
      docIds.add(docV);
      // TODO: write knn graph value
    }
    long vectorDataLength = vectorData.getFilePointer() - vectorDataOffset;
    if (vectorDataLength > 0) {
      writeMeta(fieldInfo, vectorDataOffset, vectorDataLength, docIds);
    }
  }

  private void writeVectorValue(VectorValues vectors) throws IOException {
    // write vector value
    BytesRef binaryValue = vectors.binaryValue();
    assert binaryValue.length == vectors.dimension() * Float.BYTES;
    vectorData.writeBytes(binaryValue.bytes, binaryValue.offset, binaryValue.length);
  }

  private void writeMeta(FieldInfo field, long vectorDataOffset, long vectorDataLength, List<Integer> docIds) throws IOException {
    meta.writeInt(field.number);
    meta.writeInt(field.getVectorSearchStrategy().ordinal());
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
