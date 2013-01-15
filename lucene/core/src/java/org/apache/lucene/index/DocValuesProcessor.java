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
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.codecs.SimpleDVConsumer;
import org.apache.lucene.codecs.SimpleDocValuesFormat;
import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;

final class DocValuesProcessor extends StoredFieldsConsumer {

  // nocommit wasteful we also keep a map ... double the
  // hash lookups ... would be better if DFP had "the one map"?
  private final Map<String,DocValuesWriter> writers = new HashMap<String,DocValuesWriter>();
  private final Counter bytesUsed;

  public DocValuesProcessor(Counter bytesUsed) {
    this.bytesUsed = bytesUsed;
  }

  @Override
  void startDocument() {
  }

  @Override
  void finishDocument() {
  }

  @Override
  public void addField(int docID, StorableField field, FieldInfo fieldInfo) {
    // nocommit: these checks are duplicated everywhere
    final DocValuesType dvType = field.fieldType().docValueType();
    if (dvType != null) {
      DocValuesType currentDVType = fieldInfo.getDocValuesType();
      if (currentDVType == null) {
        fieldInfo.setDocValuesType(dvType);
      } else if (currentDVType != dvType) {
        throw new IllegalArgumentException("cannot change DocValues type from " + currentDVType + " to " + dvType + " for field \"" + fieldInfo.name + "\"");
      }
      if (dvType == DocValuesType.BINARY) {
        addBinaryField(fieldInfo, docID, field.binaryValue());
      } else if (dvType == DocValuesType.SORTED) {
        addSortedField(fieldInfo, docID, field.binaryValue());
        // nocommit: hack
      } else if (dvType == DocValuesType.NUMERIC && field.numericValue() instanceof Float) {
        addNumericField(fieldInfo, docID, field.numericValue().floatValue());
      } else if (dvType == DocValuesType.NUMERIC && field.numericValue() instanceof Double) {
        addNumericField(fieldInfo, docID, field.numericValue().doubleValue());
      } else if (dvType == DocValuesType.NUMERIC) {
        addNumericField(fieldInfo, docID, field.numericValue().longValue());
      } else {
        assert false: "unrecognized DocValues.Type: " + dvType;
      }
    }
  }

  @Override
  void flush(SegmentWriteState state) throws IOException {
    if (!writers.isEmpty()) {
      SimpleDocValuesFormat fmt = state.segmentInfo.getCodec().simpleDocValuesFormat();
      // nocommit once we make
      // Codec.simpleDocValuesFormat abstract, change
      // this to assert fmt != null!
      if (fmt == null) {
        return;
      }

      SimpleDVConsumer dvConsumer = fmt.fieldsConsumer(state);
      // nocommit change to assert != null:
      if (dvConsumer == null) {
        return;
      }

      for(DocValuesWriter writer : writers.values()) {
        writer.finish(state.segmentInfo.getDocCount());
        writer.flush(state, dvConsumer);
      }
      
      writers.clear();
      dvConsumer.close();
    }
  }

  void addBinaryField(FieldInfo fieldInfo, int docID, BytesRef value) {
    DocValuesWriter writer = writers.get(fieldInfo.name);
    BytesDVWriter binaryWriter;
    if (writer == null) {
      binaryWriter = new BytesDVWriter(fieldInfo, bytesUsed);
      writers.put(fieldInfo.name, binaryWriter);
    } else if (!(writer instanceof BytesDVWriter)) {
      throw new IllegalArgumentException("Incompatible DocValues type: field \"" + fieldInfo.name + "\" changed from " + getTypeDesc(writer) + " to binary");
    } else {
      binaryWriter = (BytesDVWriter) writer;
    }
    binaryWriter.addValue(docID, value);
  }

  void addSortedField(FieldInfo fieldInfo, int docID, BytesRef value) {
    DocValuesWriter writer = writers.get(fieldInfo.name);
    SortedBytesDVWriter sortedWriter;
    if (writer == null) {
      sortedWriter = new SortedBytesDVWriter(fieldInfo, bytesUsed);
      writers.put(fieldInfo.name, sortedWriter);
    } else if (!(writer instanceof SortedBytesDVWriter)) {
      throw new IllegalArgumentException("Incompatible DocValues type: field \"" + fieldInfo.name + "\" changed from " + getTypeDesc(writer) + " to sorted");
    } else {
      sortedWriter = (SortedBytesDVWriter) writer;
    }
    sortedWriter.addValue(docID, value);
  }

  void addNumericField(FieldInfo fieldInfo, int docID, long value) {
    DocValuesWriter writer = writers.get(fieldInfo.name);
    NumberDVWriter numericWriter;
    if (writer == null) {
      numericWriter = new NumberDVWriter(fieldInfo, bytesUsed);
      writers.put(fieldInfo.name, numericWriter);
    } else if (!(writer instanceof NumberDVWriter)) {
      throw new IllegalArgumentException("Incompatible DocValues type: field \"" + fieldInfo.name + "\" changed from " + getTypeDesc(writer) + " to numeric");
    } else {
      numericWriter = (NumberDVWriter) writer;
    }
    numericWriter.addValue(docID, value);
  }

  void addNumericField(FieldInfo fieldInfo, int docID, float value) {
    DocValuesWriter writer = writers.get(fieldInfo.name);
    NumberDVWriter numericWriter;
    if (writer == null) {
      numericWriter = new NumberDVWriter(fieldInfo, bytesUsed);
      writers.put(fieldInfo.name, numericWriter);
    } else if (!(writer instanceof NumberDVWriter)) {
      throw new IllegalArgumentException("Incompatible DocValues type: field \"" + fieldInfo.name + "\" changed from " + getTypeDesc(writer) + " to numeric");
    } else {
      numericWriter = (NumberDVWriter) writer;
    }
    numericWriter.addValue(docID, Float.floatToRawIntBits(value));
  }

  void addNumericField(FieldInfo fieldInfo, int docID, double value) {
    DocValuesWriter writer = writers.get(fieldInfo.name);
    NumberDVWriter numericWriter;
    if (writer == null) {
      numericWriter = new NumberDVWriter(fieldInfo, bytesUsed);
      writers.put(fieldInfo.name, numericWriter);
    } else if (!(writer instanceof NumberDVWriter)) {
      throw new IllegalArgumentException("Incompatible DocValues type: field \"" + fieldInfo.name + "\" changed from " + getTypeDesc(writer) + " to numeric");
    } else {
      numericWriter = (NumberDVWriter) writer;
    }
    numericWriter.addValue(docID, Double.doubleToRawLongBits(value));
  }

  private String getTypeDesc(DocValuesWriter obj) {
    if (obj instanceof BytesDVWriter) {
      return "binary";
    } else if (obj instanceof NumberDVWriter) {
      return "numeric";
    } else {
      assert obj instanceof SortedBytesDVWriter;
      return "sorted";
    }
  }

  @Override
  public void abort() throws IOException {
    for(DocValuesWriter writer : writers.values()) {
      try {
        writer.abort();
      } catch (Throwable t) {
      }
    }
    writers.clear();
  }
}
