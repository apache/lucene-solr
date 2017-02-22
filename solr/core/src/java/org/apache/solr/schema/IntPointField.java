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

import java.lang.invoke.MethodHandles;
import java.util.Collection;

import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.legacy.LegacyNumericType;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.IntFieldSource;
import org.apache.lucene.queries.function.valuesource.MultiValuedIntFieldSource;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.solr.search.QParser;
import org.apache.solr.uninverting.UninvertingReader.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code PointField} implementation for {@code Integer} values.
 * @see PointField
 * @see IntPoint
 */
public class IntPointField extends PointField implements IntValueFieldType {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public IntPointField() {
    type = NumberType.INTEGER;
  }

  @Override
  public Object toNativeType(Object val) {
    if (val == null) return null;
    if (val instanceof Number) return ((Number) val).intValue();
    try {
      if (val instanceof String) return Integer.parseInt((String) val);
    } catch (NumberFormatException e) {
      Float v = Float.parseFloat((String) val);
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
      actualMin = Integer.parseInt(min);
      if (!minInclusive) {
        actualMin++;
      }
    }
    if (max == null) {
      actualMax = Integer.MAX_VALUE;
    } else {
      actualMax = Integer.parseInt(max);
      if (!maxInclusive) {
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
    return IntPoint.newExactQuery(field.getName(), Integer.parseInt(externalVal));
  }
  
  @Override
  public Query getSetQuery(QParser parser, SchemaField field, Collection<String> externalVal) {
    assert externalVal.size() > 0;
    int[] values = new int[externalVal.size()];
    int i = 0;
    for (String val:externalVal) {
      values[i] = Integer.parseInt(val);
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
    IntPoint.encodeDimension(Integer.parseInt(val.toString()), result.bytes(), 0);
  }

  @Override
  public SortField getSortField(SchemaField field, boolean top) {
    field.checkSortability();

    Object missingValue = null;
    boolean sortMissingLast = field.sortMissingLast();
    boolean sortMissingFirst = field.sortMissingFirst();

    if (sortMissingLast) {
      missingValue = top ? Integer.MIN_VALUE : Integer.MAX_VALUE;
    } else if (sortMissingFirst) {
      missingValue = top ? Integer.MAX_VALUE : Integer.MIN_VALUE;
    }
    SortField sf = new SortField(field.getName(), SortField.Type.INT, top);
    sf.setMissingValue(missingValue);
    return sf;
  }

  @Override
  public Type getUninversionType(SchemaField sf) {
    if (sf.multiValued()) {
      return Type.SORTED_INTEGER;
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
  public LegacyNumericType getNumericType() {
    return LegacyNumericType.INT;
  }

  @Override
  public IndexableField createField(SchemaField field, Object value, float boost) {
    if (!isFieldUsed(field)) return null;

    if (boost != 1.0 && log.isTraceEnabled()) {
      log.trace("Can't use document/field boost for PointField. Field: " + field.getName() + ", boost: " + boost);
    }
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
