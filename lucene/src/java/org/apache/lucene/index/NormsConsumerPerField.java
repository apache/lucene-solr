package org.apache.lucene.index;
/**
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
import org.apache.lucene.document.DocValuesField;
import org.apache.lucene.index.DocValues.Type;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BytesRef;

public class NormsConsumerPerField extends InvertedDocEndConsumerPerField implements Comparable<NormsConsumerPerField> {
  private final FieldInfo fieldInfo;
  private final DocumentsWriterPerThread.DocState docState;
  private final Similarity similarity;
  private final FieldInvertState fieldState;
  private DocValuesConsumer consumer;
  private final BytesRef spare = new BytesRef(1);
  private final DocValuesField value = new DocValuesField("", spare, Type.BYTES_FIXED_STRAIGHT);
  private final NormsConsumer parent;
  
  public NormsConsumerPerField(final DocInverterPerField docInverterPerField, final FieldInfo fieldInfo, NormsConsumer parent) {
    this.fieldInfo = fieldInfo;
    this.parent = parent;
    docState = docInverterPerField.docState;
    fieldState = docInverterPerField.fieldState;
    similarity = docState.similarityProvider.get(fieldInfo.name);
    spare.length = 1;
    spare.offset = 0;

  }
  @Override
  public int compareTo(NormsConsumerPerField other) {
    return fieldInfo.name.compareTo(other.fieldInfo.name);
  }

  @Override
  void finish() throws IOException {
    if (fieldInfo.isIndexed && !fieldInfo.omitNorms) {
      DocValuesConsumer consumer = getConsumer();
      spare.bytes[0] = similarity.computeNorm(fieldState);
      consumer.add(docState.docID, value);
    }    
  }
  
  void flush(int docCount) throws IOException {
    assert initialized();
    consumer.finish(docCount);
  }
  
  private DocValuesConsumer getConsumer() throws IOException {
    if (consumer == null) {
      consumer = parent.newConsumer(docState.docWriter.newPerDocWriteState(""), fieldInfo);
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
