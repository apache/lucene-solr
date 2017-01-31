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

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.legacy.LegacyNumericType;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.LongFieldSource;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.solr.search.QParser;
import org.apache.solr.uninverting.UninvertingReader.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code PointField} implementation for {@code Long} values.
 * @see PointField
 * @see LongPoint
 */
public class LongPointField extends PointField implements LongValueFieldType {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public LongPointField() {
    type = NumberType.LONG;
  }

  @Override
  public Object toNativeType(Object val) {
    if (val == null) return null;
    if (val instanceof Number) return ((Number) val).longValue();
    try {
      if (val instanceof String) return Long.parseLong((String) val);
    } catch (NumberFormatException e) {
      Double v = Double.parseDouble((String) val);
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
      actualMin = Long.parseLong(min);
      if (!minInclusive) {
        actualMin++;
      }
    }
    if (max == null) {
      actualMax = Long.MAX_VALUE;
    } else {
      actualMax = Long.parseLong(max);
      if (!maxInclusive) {
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
    return LongPoint.newExactQuery(field.getName(), Long.parseLong(externalVal));
  }
  
  @Override
  public Query getSetQuery(QParser parser, SchemaField field, Collection<String> externalVal) {
    assert externalVal.size() > 0;
    long[] values = new long[externalVal.size()];
    int i = 0;
    for (String val:externalVal) {
      values[i] = Long.parseLong(val);
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
    LongPoint.encodeDimension(Long.parseLong(val.toString()), result.bytes(), 0);
  }

  @Override
  public SortField getSortField(SchemaField field, boolean top) {
    field.checkSortability();

    Object missingValue = null;
    boolean sortMissingLast = field.sortMissingLast();
    boolean sortMissingFirst = field.sortMissingFirst();

    if (sortMissingLast) {
      missingValue = top ? Long.MIN_VALUE : Long.MAX_VALUE;
    } else if (sortMissingFirst) {
      missingValue = top ? Long.MAX_VALUE : Long.MIN_VALUE;
    }
    SortField sf = new SortField(field.getName(), SortField.Type.LONG, top);
    sf.setMissingValue(missingValue);
    return sf;
  }

  @Override
  public Type getUninversionType(SchemaField sf) {
    if (sf.multiValued()) {
      throw new UnsupportedOperationException("MultiValued Point fields with DocValues is not currently supported");
//      return Type.SORTED_LONG;
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
  public LegacyNumericType getNumericType() {
    return LegacyNumericType.LONG;
  }

  @Override
  public IndexableField createField(SchemaField field, Object value, float boost) {
    if (!isFieldUsed(field)) return null;

    if (boost != 1.0 && log.isTraceEnabled()) {
      log.trace("Can't use document/field boost for PointField. Field: " + field.getName() + ", boost: " + boost);
    }
    long longValue = (value instanceof Number) ? ((Number) value).longValue() : Long.parseLong(value.toString());
    return new LongPoint(field.getName(), longValue);
  }

  @Override
  protected StoredField getStoredField(SchemaField sf, Object value) {
    return new StoredField(sf.getName(), (Long) this.toNativeType(value));
  }
}
