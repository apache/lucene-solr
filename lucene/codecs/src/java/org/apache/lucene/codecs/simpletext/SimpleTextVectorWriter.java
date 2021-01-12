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

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.codecs.VectorWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IOUtils;

/** Writes vector-valued fields in a plain text format */
public class SimpleTextVectorWriter extends VectorWriter {

  static final BytesRef FIELD_NUMBER = new BytesRef("field-number ");
  static final BytesRef FIELD_NAME = new BytesRef("field-name ");
  static final BytesRef SCORE_FUNCTION = new BytesRef("score-function ");
  static final BytesRef VECTOR_DATA_OFFSET = new BytesRef("vector-data-offset ");
  static final BytesRef VECTOR_DATA_LENGTH = new BytesRef("vector-data-length ");
  static final BytesRef VECTOR_DIMENSION = new BytesRef("vector-dimension ");
  static final BytesRef SIZE = new BytesRef("size ");

  private final IndexOutput meta, vectorData;
  private final BytesRefBuilder scratch = new BytesRefBuilder();

  SimpleTextVectorWriter(SegmentWriteState state) throws IOException {
    assert state.fieldInfos.hasVectorValues();

    String metaFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, SimpleTextVectorFormat.META_EXTENSION);
    meta = state.directory.createOutput(metaFileName, state.context);

    String vectorDataFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, SimpleTextVectorFormat.VECTOR_EXTENSION);
    vectorData = state.directory.createOutput(vectorDataFileName, state.context);
  }

  @Override
  public void writeField(FieldInfo fieldInfo, VectorValues vectors) throws IOException {
    long vectorDataOffset = vectorData.getFilePointer();
    List<Integer> docIds = new ArrayList<>();
    int docV, ord = 0;
    for (docV = vectors.nextDoc(); docV != NO_MORE_DOCS; docV = vectors.nextDoc(), ord++) {
      writeVectorValue(vectors);
      docIds.add(docV);
    }
    long vectorDataLength = vectorData.getFilePointer() - vectorDataOffset;
    writeMeta(fieldInfo, vectorDataOffset, vectorDataLength, docIds);
  }

  private void writeVectorValue(VectorValues vectors) throws IOException {
    // write vector value
    float[] value = vectors.vectorValue();
    assert value.length == vectors.dimension();
    write(vectorData, Arrays.toString(value));
    newline(vectorData);
  }

  private void writeMeta(
      FieldInfo field, long vectorDataOffset, long vectorDataLength, List<Integer> docIds)
      throws IOException {
    writeField(meta, FIELD_NUMBER, field.number);
    writeField(meta, FIELD_NAME, field.name);
    writeField(meta, SCORE_FUNCTION, field.getVectorSearchStrategy().name());
    writeField(meta, VECTOR_DATA_OFFSET, vectorDataOffset);
    writeField(meta, VECTOR_DATA_LENGTH, vectorDataLength);
    writeField(meta, VECTOR_DIMENSION, field.getVectorDimension());
    writeField(meta, SIZE, docIds.size());
    for (Integer docId : docIds) {
      writeInt(meta, docId);
      newline(meta);
    }
  }

  @Override
  public void finish() throws IOException {
    writeField(meta, FIELD_NUMBER, -1);
    SimpleTextUtil.writeChecksum(meta, scratch);
    SimpleTextUtil.writeChecksum(vectorData, scratch);
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(vectorData, meta);
  }

  private void writeField(IndexOutput out, BytesRef fieldName, int value) throws IOException {
    write(out, fieldName);
    writeInt(out, value);
    newline(out);
  }

  private void writeField(IndexOutput out, BytesRef fieldName, long value) throws IOException {
    write(out, fieldName);
    writeLong(out, value);
    newline(out);
  }

  private void writeField(IndexOutput out, BytesRef fieldName, String value) throws IOException {
    write(out, fieldName);
    write(out, value);
    newline(out);
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
