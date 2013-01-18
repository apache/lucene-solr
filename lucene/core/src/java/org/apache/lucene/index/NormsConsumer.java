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
import java.util.Map;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.apache.lucene.util.IOUtils;

// TODO FI: norms could actually be stored as doc store

/** Writes norms.  Each thread X field accumulates the norms
 *  for the doc/fields it saw, then the flush method below
 *  merges all of these together into a single _X.nrm file.
 */

final class NormsConsumer extends InvertedDocEndConsumer {

  @Override
  void abort() {}
  
  @Override
  public void flush(Map<String,InvertedDocEndConsumerPerField> fieldsToFlush, SegmentWriteState state) throws IOException {
    boolean success = false;
    DocValuesConsumer normsConsumer = null;
    try {
      if (state.fieldInfos.hasNorms()) {
        NormsFormat normsFormat = state.segmentInfo.getCodec().normsFormat();
        assert normsFormat != null;
        normsConsumer = normsFormat.normsConsumer(state);

        for (FieldInfo fi : state.fieldInfos) {
          final NormsConsumerPerField toWrite = (NormsConsumerPerField) fieldsToFlush.get(fi.name);
          // we must check the final value of omitNorms for the fieldinfo, it could have 
          // changed for this field since the first time we added it.
          if (!fi.omitsNorms()) {
            if (toWrite != null && !toWrite.isEmpty()) {
              toWrite.flush(state, normsConsumer);
              assert fi.getNormType() == DocValuesType.NUMERIC;
            } else if (fi.isIndexed()) {
              assert fi.getNormType() == null: "got " + fi.getNormType() + "; field=" + fi.name;
            }
          }
        }
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(normsConsumer);
      } else {
        IOUtils.closeWhileHandlingException(normsConsumer);
      }
    }
  }

  @Override
  void finishDocument() {}

  @Override
  void startDocument() {}

  @Override
  InvertedDocEndConsumerPerField addField(DocInverterPerField docInverterPerField, FieldInfo fieldInfo) {
    return new NormsConsumerPerField(docInverterPerField, fieldInfo, this);
  }
}
