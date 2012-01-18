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
import java.util.Collection;
import java.util.Map;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.PerDocConsumer;
import org.apache.lucene.index.DocValues.Type;
import org.apache.lucene.util.IOUtils;

// TODO FI: norms could actually be stored as doc store

/** Writes norms.  Each thread X field accumulates the norms
 *  for the doc/fields it saw, then the flush method below
 *  merges all of these together into a single _X.nrm file.
 */

final class NormsConsumer extends InvertedDocEndConsumer {
  private final NormsFormat normsFormat;
  private PerDocConsumer consumer;
  
  public NormsConsumer(DocumentsWriterPerThread dwpt) {
    normsFormat = dwpt.codec.normsFormat();
  }

  @Override
  public void abort(){
    if (consumer != null) {
      consumer.abort();
    }
  }

  // We only write the _X.nrm file at flush
  void files(Collection<String> files) {}

  /** Produce _X.nrm if any document had a field with norms
   *  not disabled */
  @Override
  public void flush(Map<FieldInfo,InvertedDocEndConsumerPerField> fieldsToFlush, SegmentWriteState state) throws IOException {
    boolean success = false;
    boolean anythingFlushed = false;
    try {
      if (state.fieldInfos.hasNorms()) {
        for (FieldInfo fi : state.fieldInfos) {
          final NormsConsumerPerField toWrite = (NormsConsumerPerField) fieldsToFlush.get(fi);
          // we must check the final value of omitNorms for the fieldinfo, it could have 
          // changed for this field since the first time we added it.
          if (!fi.omitNorms) {
            if (toWrite != null && toWrite.initialized()) {
              anythingFlushed = true;
              final Type type = toWrite.flush(state.numDocs);
              assert fi.getNormType() == type;
            } else if (fi.isIndexed) {
              anythingFlushed = true;
              assert fi.getNormType() == null;
              fi.setNormValueType(null, false);
            }
          }
        }
      } 
      
      success = true;
      if (!anythingFlushed && consumer != null) {
        consumer.abort();
      }
    } finally {
      if (success) {
        IOUtils.close(consumer);
      } else {
        IOUtils.closeWhileHandlingException(consumer);
      }
    }
  }

  @Override
  void finishDocument() throws IOException {}

  @Override
  void startDocument() throws IOException {}

  @Override
  InvertedDocEndConsumerPerField addField(DocInverterPerField docInverterPerField,
      FieldInfo fieldInfo) {
    return new NormsConsumerPerField(docInverterPerField, fieldInfo, this);
  }
  
  DocValuesConsumer newConsumer(PerDocWriteState perDocWriteState,
      FieldInfo fieldInfo, Type type) throws IOException {
    if (consumer == null) {
      consumer = normsFormat.docsConsumer(perDocWriteState);
    }
    DocValuesConsumer addValuesField = consumer.addValuesField(type, fieldInfo);
    return addValuesField;
  }
  
}
