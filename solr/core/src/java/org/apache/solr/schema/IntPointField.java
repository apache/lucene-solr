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

import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.IntFieldSource;
import org.apache.lucene.queries.function.valuesource.MultiValuedIntFieldSource;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.solr.search.QParser;
import org.apache.solr.uninverting.UninvertingReader.Type;

/**
 * {@code PointField} implementation for {@code Integer} values.
 * @see PointField
 * @see IntPoint
 */
public class IntPointField extends PointField implements IntValueFieldType {

  public IntPointField() {
    type = NumberType.INTEGER;
  }

  @Override
  public Object toNativeType(Object val) {
    if (val == null) return null;
    if (val instanceof Number) return ((Number) val).intValue();
    try {
      if (val instanceof CharSequence) return Integer.parseInt( val.toString());
    } catch (NumberFormatException e) {
      Float v = Float.parseFloat(val.toString());
      return v.intValue();
    }
    return super.toNativeType(val);
  }

  @Override
  public Query getPointRangeQuery(QParser parser, SchemaField field, String min, String max, boolean minInclusive,
      boolean maxInclusive) {
    int actualMin, actualMax;
    if (min == null) {
      actualMin = Integer.MIN_VALUE;
    } else {
      actualMin = parseIntFromUser(field.getName(), min);
      if (!minInclusive) {
        if (actualMin == Integer.MAX_VALUE) return new MatchNoDocsQuery();
        actualMin++;
      }
    }
    if (max == null) {
      actualMax = Integer.MAX_VALUE;
    } else {
      actualMax = parseIntFromUser(field.getName(), max);
      if (!maxInclusive) {
        if (actualMax == Integer.MIN_VALUE) return new MatchNoDocsQuery();
        actualMax--;
      }
    }
    return IntPoint.newRangeQuery(field.getName(), actualMin, actualMax);
  }

  @Override
  public Object toObject(SchemaField sf, BytesRef term) {
    return IntPoint.decodeDimension(term.bytes, term.offset);
  }
  
  @Override
  public Object toObject(IndexableField f) {
    final Number val = f.numericValue();
    if (val != null) {
      return val.intValue();
    } else {
      throw new AssertionError("Unexpected state. Field: '" + f + "'");
    }
  }

  @Override
  protected Query getExactQuery(SchemaField field, String externalVal) {
    return IntPoint.newExactQuery(field.getName(), parseIntFromUser(field.getName(), externalVal));
  }
  
  @Override
  public Query getSetQuery(QParser parser, SchemaField field, Collection<String> externalVal) {
    assert externalVal.size() > 0;
    if (!field.indexed()) {
      return super.getSetQuery(parser, field, externalVal);
    }
    int[] values = new int[externalVal.size()];
    int i = 0;
    for (String val:externalVal) {
      values[i] = parseIntFromUser(field.getName(), val);
      i++;
    }
    return IntPoint.newSetQuery(field.getName(), values);
  }

  @Override
  protected String indexedToReadable(BytesRef indexedForm) {
    return Integer.toString(IntPoint.decodeDimension(indexedForm.bytes, indexedForm.offset));
  }

  @Override
  public void readableToIndexed(CharSequence val, BytesRefBuilder result) {
    result.grow(Integer.BYTES);
    result.setLength(Integer.BYTES);
    IntPoint.encodeDimension(parseIntFromUser(null, val.toString()), result.bytes(), 0);
  }

  @Override
  public Type getUninversionType(SchemaField sf) {
    if (sf.multiValued()) {
      return null; 
    } else {
      return Type.INTEGER_POINT;
    }
  }

  @Override
  public ValueSource getValueSource(SchemaField field, QParser qparser) {
    field.checkFieldCacheSource();
    return new IntFieldSource(field.getName());
  }

  @Override
  public IndexableField createField(SchemaField field, Object value) {
    int intValue = (value instanceof Number) ? ((Number) value).intValue() : Integer.parseInt(value.toString());
    return new IntPoint(field.getName(), intValue);
  }

  @Override
  protected StoredField getStoredField(SchemaField sf, Object value) {
    return new StoredField(sf.getName(), (Integer) this.toNativeType(value));
  }
  
  @Override
  protected ValueSource getSingleValueSource(SortedNumericSelector.Type choice, SchemaField f) {
    return new MultiValuedIntFieldSource(f.getName(), choice);
  }

}
