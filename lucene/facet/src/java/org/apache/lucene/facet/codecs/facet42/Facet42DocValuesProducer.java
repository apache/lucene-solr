package org.apache.lucene.facet.codecs.facet42;

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

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.IOUtils;

class Facet42DocValuesProducer extends DocValuesProducer {

  private final Map<Integer,Facet42BinaryDocValues> fields = new HashMap<Integer,Facet42BinaryDocValues>();
  
  Facet42DocValuesProducer(SegmentReadState state) throws IOException {
    String fileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, Facet42DocValuesFormat.EXTENSION);
    IndexInput in = state.directory.openInput(fileName, state.context);
    boolean success = false;
    try {
      CodecUtil.checkHeader(in, Facet42DocValuesFormat.CODEC, 
                            Facet42DocValuesFormat.VERSION_START,
                            Facet42DocValuesFormat.VERSION_START);
      int fieldNumber = in.readVInt();
      while (fieldNumber != -1) {
        fields.put(fieldNumber, new Facet42BinaryDocValues(in));
        fieldNumber = in.readVInt();
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(in);
      } else {
        IOUtils.closeWhileHandlingException(in);
      }
    }
  }

  @Override
  public NumericDocValues getNumeric(FieldInfo field) throws IOException {
    throw new UnsupportedOperationException("FacetsDocValues only implements binary");
  }

  @Override
  public BinaryDocValues getBinary(FieldInfo field) throws IOException {
    return fields.get(field.number);
  }

  @Override
  public SortedDocValues getSorted(FieldInfo field) throws IOException {
    throw new UnsupportedOperationException("FacetsDocValues only implements binary");
  }

  @Override
  public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
    throw new UnsupportedOperationException("FacetsDocValues only implements binary");
  }

  @Override
  public void close() throws IOException {
  }
}
