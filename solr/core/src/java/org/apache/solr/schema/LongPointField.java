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

import java.util.Collection;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.LongFieldSource;
import org.apache.lucene.queries.function.valuesource.MultiValuedLongFieldSource;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.solr.search.QParser;
import org.apache.solr.uninverting.UninvertingReader.Type;

/**
 * {@code PointField} implementation for {@code Long} values.
 * @see PointField
 * @see LongPoint
 */
public class LongPointField extends PointField implements LongValueFieldType {

  public LongPointField() {
    type = NumberType.LONG;
  }

  @Override
  public Object toNativeType(Object val) {
    if (val == null) return null;
    if (val instanceof Number) return ((Number) val).longValue();
    try {
      if (val instanceof CharSequence) return Long.parseLong(val.toString());
    } catch (NumberFormatException e) {
      Double v = Double.parseDouble(val.toString());
      return v.longValue();
    }
    return super.toNativeType(val);
  }

  @Override
  public Query getPointRangeQuery(QParser parser, SchemaField field, String min, String max, boolean minInclusive,
      boolean maxInclusive) {
    long actualMin, actualMax;
    if (min == null) {
      actualMin = Long.MIN_VALUE;
    } else {
      actualMin = parseLongFromUser(field.getName(), min);
      if (!minInclusive) {
        if (actualMin == Long.MAX_VALUE) return new MatchNoDocsQuery();
        actualMin++;
      }
    }
    if (max == null) {
      actualMax = Long.MAX_VALUE;
    } else {
      actualMax = parseLongFromUser(field.getName(), max);
      if (!maxInclusive) {
        if (actualMax == Long.MIN_VALUE) return new MatchNoDocsQuery();
        actualMax--;
      }
    }
    return LongPoint.newRangeQuery(field.getName(), actualMin, actualMax);
  }

  @Override
  public Object toObject(SchemaField sf, BytesRef term) {
    return LongPoint.decodeDimension(term.bytes, term.offset);
  }
  
  @Override
  public Object toObject(IndexableField f) {
    final Number val = f.numericValue();
    if (val != null) {
      return val;
    } else {
      throw new AssertionError("Unexpected state. Field: '" + f + "'");
    }
  }

  @Override
  protected Query getExactQuery(SchemaField field, String externalVal) {
    return LongPoint.newExactQuery(field.getName(), parseLongFromUser(field.getName(), externalVal));
  }
  
  @Override
  public Query getSetQuery(QParser parser, SchemaField field, Collection<String> externalVal) {
    assert externalVal.size() > 0;
    if (!field.indexed()) {
      return super.getSetQuery(parser, field, externalVal);
    }
    long[] values = new long[externalVal.size()];
    int i = 0;
    for (String val:externalVal) {
      values[i] = parseLongFromUser(field.getName(), val);
      i++;
    }
    return LongPoint.newSetQuery(field.getName(), values);
  }

  @Override
  protected String indexedToReadable(BytesRef indexedForm) {
    return Long.toString(LongPoint.decodeDimension(indexedForm.bytes, indexedForm.offset));
  }

  @Override
  public void readableToIndexed(CharSequence val, BytesRefBuilder result) {
    result.grow(Long.BYTES);
    result.setLength(Long.BYTES);
    LongPoint.encodeDimension(parseLongFromUser(null, val.toString()), result.bytes(), 0);
  }

  @Override
  public Type getUninversionType(SchemaField sf) {
    if (sf.multiValued()) {
      return null;
    } else {
      return Type.LONG_POINT;
    }
  }

  @Override
  public ValueSource getValueSource(SchemaField field, QParser qparser) {
    field.checkFieldCacheSource();
    return new LongFieldSource(field.getName());
  }
  
  @Override
  protected ValueSource getSingleValueSource(org.apache.lucene.search.SortedNumericSelector.Type choice,
      SchemaField field) {
    return new MultiValuedLongFieldSource(field.getName(), choice);
  }

  @Override
  public IndexableField createField(SchemaField field, Object value) {
    long longValue = (value instanceof Number) ? ((Number) value).longValue() : Long.parseLong(value.toString());
    return new LongPoint(field.getName(), longValue);
  }

  @Override
  protected StoredField getStoredField(SchemaField sf, Object value) {
    return new StoredField(sf.getName(), (Long) this.toNativeType(value));
  }
}
