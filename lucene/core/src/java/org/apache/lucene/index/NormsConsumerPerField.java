package org.apache.lucene.index;
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
import java.io.IOException;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.search.similarities.Similarity;

final class NormsConsumerPerField extends InvertedDocEndConsumerPerField implements Comparable<NormsConsumerPerField> {
  private final FieldInfo fieldInfo;
  private final DocumentsWriterPerThread.DocState docState;
  private final Similarity similarity;
  private final FieldInvertState fieldState;
  private NumericDocValuesWriter consumer;
  
  public NormsConsumerPerField(final DocInverterPerField docInverterPerField, final FieldInfo fieldInfo, NormsConsumer parent) {
    this.fieldInfo = fieldInfo;
    docState = docInverterPerField.docState;
    fieldState = docInverterPerField.fieldState;
    similarity = docState.similarity;
  }

  @Override
  public int compareTo(NormsConsumerPerField other) {
    return fieldInfo.name.compareTo(other.fieldInfo.name);
  }

  @Override
  void finish() throws IOException {
    if (fieldInfo.isIndexed() && !fieldInfo.omitsNorms()) {
      if (consumer == null) {
        fieldInfo.setNormValueType(FieldInfo.DocValuesType.NUMERIC);
        consumer = new NumericDocValuesWriter(fieldInfo, docState.docWriter.bytesUsed);
      }
      consumer.addValue(docState.docID, similarity.computeNorm(fieldState));
    }
  }
  
  void flush(SegmentWriteState state, DocValuesConsumer normsWriter) throws IOException {
    int docCount = state.segmentInfo.getDocCount();
    if (consumer == null) {
      return; // null type - not omitted but not written -
              // meaning the only docs that had
              // norms hit exceptions (but indexed=true is set...)
    }
    consumer.finish(docCount);
    consumer.flush(state, normsWriter);
  }
  
  boolean isEmpty() {
    return consumer == null;
  }

  @Override
  void abort() {
    //
  }
}
