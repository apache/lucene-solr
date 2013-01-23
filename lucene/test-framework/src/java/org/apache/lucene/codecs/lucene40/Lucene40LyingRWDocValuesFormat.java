package org.apache.lucene.codecs.lucene40;

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
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.lucene40.Lucene40FieldInfosReader.LegacyDocValuesType;
import org.apache.lucene.codecs.lucene42.Lucene42DocValuesFormat;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.BytesRef;

public class Lucene40LyingRWDocValuesFormat extends Lucene40LyingDocValuesFormat {
  private final DocValuesFormat lie = new Lucene42DocValuesFormat();

  // nocommit: a lie
  @Override
  public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    final DocValuesConsumer delegate = lie.fieldsConsumer(state);
    return new DocValuesConsumer() {

      @Override
      public void addNumericField(FieldInfo field, Iterable<Number> values) throws IOException {
        // hack: here we would examine the numerics and simulate in the impersonator the best we can
        // e.g. if they are all in byte/int range write fixed, otherwise write packed or whatever
        field.putAttribute(Lucene40FieldInfosReader.LEGACY_DV_TYPE_KEY, LegacyDocValuesType.VAR_INTS.name());
        delegate.addNumericField(field, values);
      }

      @Override
      public void addBinaryField(FieldInfo field, Iterable<BytesRef> values) throws IOException {
        field.putAttribute(Lucene40FieldInfosReader.LEGACY_DV_TYPE_KEY, LegacyDocValuesType.BYTES_VAR_STRAIGHT.name());
        delegate.addBinaryField(field, values);
      }

      @Override
      public void addSortedField(FieldInfo field, Iterable<BytesRef> values, Iterable<Number> docToOrd) throws IOException {
        field.putAttribute(Lucene40FieldInfosReader.LEGACY_DV_TYPE_KEY, LegacyDocValuesType.BYTES_VAR_SORTED.name());
        delegate.addSortedField(field, values, docToOrd);
      }
      
      @Override
      public void close() throws IOException {
        delegate.close();
      }
    };
  }
}
