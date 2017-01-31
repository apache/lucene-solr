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

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.legacy.LegacyNumericType;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.DoubleFieldSource;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.solr.search.QParser;
import org.apache.solr.uninverting.UninvertingReader.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code PointField} implementation for {@code Double} values.
 * @see PointField
 * @see DoublePoint
 */
public class DoublePointField extends PointField implements DoubleValueFieldType {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public DoublePointField() {
    type = NumberType.DOUBLE;
  }

  @Override
  public Object toNativeType(Object val) {
    if (val == null) return null;
    if (val instanceof Number) return ((Number) val).doubleValue();
    if (val instanceof String) return Double.parseDouble((String) val);
    return super.toNativeType(val);
  }

  @Override
  public Query getPointRangeQuery(QParser parser, SchemaField field, String min, String max, boolean minInclusive,
      boolean maxInclusive) {
    double actualMin, actualMax;
    if (min == null) {
      actualMin = Double.NEGATIVE_INFINITY;
    } else {
      actualMin = Double.parseDouble(min);
      if (!minInclusive) {
        actualMin = DoublePoint.nextUp(actualMin);
      }
    }
    if (max == null) {
      actualMax = Double.POSITIVE_INFINITY;
    } else {
      actualMax = Double.parseDouble(max);
      if (!maxInclusive) {
        actualMax = DoublePoint.nextDown(actualMax);
      }
    }
    return DoublePoint.newRangeQuery(field.getName(), actualMin, actualMax);
  }

  @Override
  public Object toObject(SchemaField sf, BytesRef term) {
    return DoublePoint.decodeDimension(term.bytes, term.offset);
  }
  
  @Override
  public Object toObject(IndexableField f) {
    final Number val = f.numericValue();
    if (val != null) {
      if (f.fieldType().stored() == false && f.fieldType().docValuesType() == DocValuesType.NUMERIC) {
        return Double.longBitsToDouble(val.longValue());
      } else {
        return val;
      }
    } else {
      throw new AssertionError("Unexpected state. Field: '" + f + "'");
    }
  }

  @Override
  protected Query getExactQuery(SchemaField field, String externalVal) {
    return DoublePoint.newExactQuery(field.getName(), Double.parseDouble(externalVal));
  }

  @Override
  public Query getSetQuery(QParser parser, SchemaField field, Collection<String> externalVal) {
    assert externalVal.size() > 0;
    double[] values = new double[externalVal.size()];
    int i = 0;
    for (String val:externalVal) {
      values[i] = Double.parseDouble(val);
      i++;
    }
    return DoublePoint.newSetQuery(field.getName(), values);
  }

  @Override
  protected String indexedToReadable(BytesRef indexedForm) {
    return Double.toString(DoublePoint.decodeDimension(indexedForm.bytes, indexedForm.offset));
  }

  @Override
  public void readableToIndexed(CharSequence val, BytesRefBuilder result) {
    result.grow(Double.BYTES);
    result.setLength(Double.BYTES);
    DoublePoint.encodeDimension(Double.parseDouble(val.toString()), result.bytes(), 0);
  }

  @Override
  public SortField getSortField(SchemaField field, boolean top) {
    field.checkSortability();

    Object missingValue = null;
    boolean sortMissingLast = field.sortMissingLast();
    boolean sortMissingFirst = field.sortMissingFirst();

    if (sortMissingLast) {
      missingValue = top ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
    } else if (sortMissingFirst) {
      missingValue = top ? Double.POSITIVE_INFINITY : Double.NEGATIVE_INFINITY;
    }
    SortField sf = new SortField(field.getName(), SortField.Type.DOUBLE, top);
    sf.setMissingValue(missingValue);
    return sf;
  }

  @Override
  public Type getUninversionType(SchemaField sf) {
    if (sf.multiValued()) {
      throw new UnsupportedOperationException("MultiValued Point fields with DocValues is not currently supported");
//      return Type.SORTED_DOUBLE;
    } else {
      return Type.DOUBLE_POINT;
    }
  }

  @Override
  public ValueSource getValueSource(SchemaField field, QParser qparser) {
    field.checkFieldCacheSource();
    return new DoubleFieldSource(field.getName());
  }

  @Override
  public LegacyNumericType getNumericType() {
    // TODO: refactor this to not use LegacyNumericType
    return LegacyNumericType.DOUBLE;
  }

  @Override
  public IndexableField createField(SchemaField field, Object value, float boost) {
    if (!isFieldUsed(field)) return null;

    if (boost != 1.0 && log.isTraceEnabled()) {
      log.trace("Can't use document/field boost for PointField. Field: " + field.getName() + ", boost: " + boost);
    }
    double doubleValue = (value instanceof Number) ? ((Number) value).doubleValue() : Double.parseDouble(value.toString());
    return new DoublePoint(field.getName(), doubleValue);
  }

  @Override
  protected StoredField getStoredField(SchemaField sf, Object value) {
    return new StoredField(sf.getName(), (Double) this.toNativeType(value));
  }
}
