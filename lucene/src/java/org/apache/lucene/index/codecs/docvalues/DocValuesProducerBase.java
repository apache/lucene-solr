package org.apache.lucene.index.codecs.docvalues;
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
import java.util.TreeMap;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.codecs.FieldsProducer;
import org.apache.lucene.index.values.Bytes;
import org.apache.lucene.index.values.DocValues;
import org.apache.lucene.index.values.Floats;
import org.apache.lucene.index.values.Ints;
import org.apache.lucene.index.values.Values;
import org.apache.lucene.index.values.Writer;
import org.apache.lucene.store.Directory;

public abstract class DocValuesProducerBase extends FieldsProducer{
  
  protected final TreeMap<String, DocValues> docValues = new TreeMap<String, DocValues>();

  protected DocValuesProducerBase(SegmentInfo si, Directory dir, FieldInfos fieldInfo, String codecId) throws IOException {
    load(fieldInfo, si.name, si.docCount, dir, codecId);
  }

  @Override
  public DocValues docValues(String field) throws IOException {
    return docValues.get(field);
  }

  // Only opens files... doesn't actually load any values
  protected void load(FieldInfos fieldInfos, String segment, int docCount,
      Directory dir, String codecId) throws IOException {
    final int numFields = fieldInfos.size();
    for (int i = 0; i < numFields; i++) {
      final FieldInfo fieldInfo = fieldInfos.fieldInfo(i);
      final Values v = fieldInfo.getDocValues();
      final String field = fieldInfo.name;
      //TODO can we have a compound file  per segment and codec for docvalues?
      final String id = IndexFileNames.segmentFileName(segment, codecId+"-"+fieldInfo.number, "");
      if (v != null && dir.fileExists(id + "." +  Writer.DATA_EXTENSION)) {
        docValues.put(field, loadDocValues(docCount, dir, id, v));
      } 
    }
  }

  protected DocValues loadDocValues(int docCount, Directory dir, String id,
      Values v) throws IOException {
    switch (v) {
    case PACKED_INTS:
      return Ints.getValues(dir, id, false);
    case PACKED_INTS_FIXED:
      return Ints.getValues(dir, id, true);
    case SIMPLE_FLOAT_4BYTE:
      return Floats.getValues(dir, id, docCount);
    case SIMPLE_FLOAT_8BYTE:
      return Floats.getValues(dir, id, docCount);
    case BYTES_FIXED_STRAIGHT:
      return Bytes.getValues(dir, id, Bytes.Mode.STRAIGHT, true, docCount);
    case BYTES_FIXED_DEREF:
      return Bytes.getValues(dir, id, Bytes.Mode.DEREF, true, docCount);
    case BYTES_FIXED_SORTED:
      return Bytes.getValues(dir, id, Bytes.Mode.SORTED, true, docCount);
    case BYTES_VAR_STRAIGHT:
      return Bytes.getValues(dir, id, Bytes.Mode.STRAIGHT, false, docCount);
    case BYTES_VAR_DEREF:
      return Bytes.getValues(dir, id, Bytes.Mode.DEREF, false, docCount);
    case BYTES_VAR_SORTED:
      return Bytes.getValues(dir, id, Bytes.Mode.SORTED, false, docCount);
    default:
      throw new IllegalStateException("unrecognized index values mode " + v);
    }
  }

  @Override
  public void close() throws IOException {
    Collection<DocValues> values = docValues.values();
    for (DocValues docValues : values) {
      docValues.close();
    }
  }
}
