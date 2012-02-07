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

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Holds all per thread, per field state.
 */

final class DocFieldProcessorPerField {

  final DocFieldConsumerPerField consumer;
  final FieldInfo fieldInfo;

  DocFieldProcessorPerField next;
  int lastGen = -1;

  int fieldCount;
  IndexableField[] fields = new IndexableField[1];

  public DocFieldProcessorPerField(final DocFieldProcessor docFieldProcessor, final FieldInfo fieldInfo) {
    this.consumer = docFieldProcessor.consumer.addField(fieldInfo);
    this.fieldInfo = fieldInfo;
  }

  public void addField(IndexableField field) {
    if (fieldCount == fields.length) {
      int newSize = ArrayUtil.oversize(fieldCount + 1, RamUsageEstimator.NUM_BYTES_OBJECT_REF);
      IndexableField[] newArray = new IndexableField[newSize];
      System.arraycopy(fields, 0, newArray, 0, fieldCount);
      fields = newArray;
    }

    fields[fieldCount++] = field;
  }

  public void abort() {
    consumer.abort();
  }
}
