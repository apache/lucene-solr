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
package org.apache.solr.schema;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;

/**
 * Custom field representing a {@link BinaryField} that's sortable.
 */
public class SortableBinaryField extends BinaryField {

  @Override
  protected void checkSupportsDocValues() { // we support DocValues
  }

  @Override
  public List<IndexableField> createFields(SchemaField field, Object value) {
    if (field.hasDocValues()) {
      List<IndexableField> fields = new ArrayList<>();
      IndexableField storedField = createField(field, value);
      fields.add(storedField);
      ByteBuffer byteBuffer = toObject(storedField);
      BytesRef bytes = new BytesRef
          (byteBuffer.array(), byteBuffer.arrayOffset() + byteBuffer.position(), byteBuffer.remaining());
      if (field.multiValued()) {
        fields.add(new SortedSetDocValuesField(field.getName(), bytes));
      } else {
        fields.add(new SortedDocValuesField(field.getName(), bytes));
      }
      return fields;
    } else {
      return Collections.singletonList(createField(field, value));
    }
  }

  @Override
  public SortField getSortField(final SchemaField field, final boolean reverse) {
    field.checkSortability();
    return new BinarySortField(field.getName(), reverse);
  }

  private static class BinarySortField extends SortField {
    public BinarySortField(final String field, final boolean reverse) {
      super(field, new FieldComparatorSource() {
        @Override
        public FieldComparator.TermOrdValComparator newComparator
            (final String fieldname, final int numHits, final int sortPos, final boolean reversed) {
          return new FieldComparator.TermOrdValComparator(numHits, fieldname);
        }}, reverse);
    }
  }
  
  @Override
  public Object marshalSortValue(Object value) {
    return marshalBase64SortValue(value);
  }
  
  @Override
  public Object unmarshalSortValue(Object value) {
    return unmarshalBase64SortValue(value);
  }
}
