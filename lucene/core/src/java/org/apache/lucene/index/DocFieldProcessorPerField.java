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

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Holds all per thread, per field state.
 */

final class DocFieldProcessorPerField {

  final DocFieldConsumerPerField consumer;
  final FieldInfo fieldInfo;
  private final Counter bytesUsed;

  // nocommit after flush we should null these out?  then we
  // don't need reset() impl'd in each...
  BytesDVWriter bytesDVWriter;
  SortedBytesDVWriter sortedBytesDVWriter;
  NumberDVWriter numberDVWriter;

  DocFieldProcessorPerField next;
  int lastGen = -1;

  int fieldCount;
  IndexableField[] fields = new IndexableField[1];
  private final Map<FieldInfo,String> dvFields = new HashMap<FieldInfo,String>();

  public DocFieldProcessorPerField(final DocFieldProcessor docFieldProcessor, final FieldInfo fieldInfo) {
    this.consumer = docFieldProcessor.consumer.addField(fieldInfo);
    this.fieldInfo = fieldInfo;
    this.bytesUsed = docFieldProcessor.bytesUsed;
  }

  // nocommit make this generic chain through consumer?
  public void addBytesDVField(int docID, BytesRef value) {
    if (bytesDVWriter == null) {
      verifyField(fieldInfo, "binary");
      bytesDVWriter = new BytesDVWriter(fieldInfo, bytesUsed);
    }
    bytesDVWriter.addValue(docID, value);
  }

  // nocommit make this generic chain through consumer?
  public void addSortedBytesDVField(int docID, BytesRef value) {
    if (sortedBytesDVWriter == null) {
      verifyField(fieldInfo, "sorted");
      sortedBytesDVWriter = new SortedBytesDVWriter(fieldInfo, bytesUsed);
    }
    sortedBytesDVWriter.addValue(docID, value);
  }

  // nocommit make this generic chain through consumer?
  public void addNumberDVField(int docID, Number value) {
    if (numberDVWriter == null) {
      verifyField(fieldInfo, "numeric");
      numberDVWriter = new NumberDVWriter(fieldInfo, bytesUsed);
    }
    numberDVWriter.addValue(docID, value.longValue());
  }

  // nocommit make this generic chain through consumer?
  public void addFloatDVField(int docID, Number value) {
    if (numberDVWriter == null) {
      verifyField(fieldInfo, "numeric");
      numberDVWriter = new NumberDVWriter(fieldInfo, bytesUsed);
    }
    numberDVWriter.addValue(docID, Float.floatToRawIntBits(value.floatValue()));
  }

  // nocommit make this generic chain through consumer?
  public void addDoubleDVField(int docID, Number value) {
    if (numberDVWriter == null) {
      verifyField(fieldInfo, "numeric");
      numberDVWriter = new NumberDVWriter(fieldInfo, bytesUsed);
    }
    numberDVWriter.addValue(docID, Double.doubleToRawLongBits(value.doubleValue()));
  }

  private void verifyField(FieldInfo field, String type) {
    if (dvFields.containsKey(field)) {
      throw new IllegalArgumentException("Incompatible DocValues type: field \"" + field.name + "\" changed from " + dvFields.get(field) + " to " + type);
    }
    dvFields.put(field, type);
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
    if (bytesDVWriter != null) {
      bytesDVWriter.abort();
    }
    if (sortedBytesDVWriter != null) {
      sortedBytesDVWriter.abort();
    }
    if (numberDVWriter != null) {
      numberDVWriter.abort();
    }
  }
}
