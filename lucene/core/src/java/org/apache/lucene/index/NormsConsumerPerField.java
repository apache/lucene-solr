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
import org.apache.lucene.codecs.SimpleDVConsumer;
import org.apache.lucene.index.DocValues.Type;
import org.apache.lucene.search.similarities.Similarity;

final class NormsConsumerPerField extends InvertedDocEndConsumerPerField implements Comparable<NormsConsumerPerField> {
  private final FieldInfo fieldInfo;
  private final DocumentsWriterPerThread.DocState docState;
  private final Similarity similarity;
  private final FieldInvertState fieldState;
  private DocValuesConsumer consumer;
  private final Norm norm;
  private final NormsConsumer parent;
  private Type initType;
  private final NumberDVWriter simpleNormsWriter;
  
  public NormsConsumerPerField(final DocInverterPerField docInverterPerField, final FieldInfo fieldInfo, NormsConsumer parent) {
    this.fieldInfo = fieldInfo;
    this.parent = parent;
    docState = docInverterPerField.docState;
    fieldState = docInverterPerField.fieldState;
    similarity = docState.similarity;
    norm = new Norm();
    simpleNormsWriter = new NumberDVWriter(fieldInfo, docState.docWriter.bytesUsed);
  }

  @Override
  public int compareTo(NormsConsumerPerField other) {
    return fieldInfo.name.compareTo(other.fieldInfo.name);
  }

  @Override
  void finish() throws IOException {
    if (fieldInfo.isIndexed() && !fieldInfo.omitsNorms()) {
      similarity.computeNorm(fieldState, norm);
      
      if (norm.type() != null) {
        StorableField field = norm.field();
        // some similarity might not compute any norms
        DocValuesConsumer consumer = getConsumer(norm.type());
        consumer.add(docState.docID, field);
      }

      long norm = similarity.computeSimpleNorm(fieldState);
      if (norm != -1) {
        // nocommit is -1 really a safe "not set" value!?
        // nocommit shouldn't we require that it's either
        // all -1's or none?  a sim can't not compute norms
        // for only some docs?  hmm unless the field is
        // missing for this doc... but then finish() isn't
        // called?
        simpleNormsWriter.addValue(docState.docID, norm);
      }
    }    
  }
  
  Type flush(SegmentWriteState state, SimpleDVConsumer normsConsumer) throws IOException {
    int docCount = state.segmentInfo.getDocCount();
    if (!initialized()) {
      return null; // null type - not omitted but not written
    }
    consumer.finish(docCount);
    // nocommit change to assert normsConsumer != null
    if (normsConsumer != null) {
      // nocommit we need to change the suffix?  ie so norms
      // don't step on dvs? hmmm.... where does this happen
      // today ...
      simpleNormsWriter.finish(docCount);
      simpleNormsWriter.flush(state, normsConsumer);
    } else {
      // nocommit remove:
      simpleNormsWriter.reset();
    }
    return initType;
  }
  
  private DocValuesConsumer getConsumer(Type type) throws IOException {
    if (consumer == null) {
      assert fieldInfo.getNormType() == null || fieldInfo.getNormType() == type;
      fieldInfo.setNormValueType(type);
      consumer = parent.newConsumer(docState.docWriter.newPerDocWriteState(""), fieldInfo, type);
      this.initType = type;
    }
    if (initType != type) {
      throw new IllegalArgumentException("NormTypes for field: " + fieldInfo.name + " doesn't match " + initType + " != " + type);
    }
    return consumer;
  }
  
  boolean initialized() {
    return consumer != null;
  }

  @Override
  void abort() {
    //
  }

}
