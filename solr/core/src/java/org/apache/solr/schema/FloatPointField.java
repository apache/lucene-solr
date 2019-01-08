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
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.FloatFieldSource;
import org.apache.lucene.queries.function.valuesource.MultiValuedFloatFieldSource;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.NumericUtils;
import org.apache.solr.search.QParser;
import org.apache.solr.uninverting.UninvertingReader.Type;

/**
 * {@code PointField} implementation for {@code Float} values.
 * @see PointField
 * @see FloatPoint
 */
public class FloatPointField extends PointField implements FloatValueFieldType {

  public FloatPointField() {
    type = NumberType.FLOAT;
  }

  @Override
  public Object toNativeType(Object val) {
    if (val == null) return null;
    if (val instanceof Number) return ((Number) val).floatValue();
    if (val instanceof CharSequence) return Float.parseFloat(val.toString());
    return super.toNativeType(val);
  }

  @Override
  public Query getPointRangeQuery(QParser parser, SchemaField field, String min, String max, boolean minInclusive,
      boolean maxInclusive) {
    float actualMin, actualMax;
    if (min == null) {
      actualMin = Float.NEGATIVE_INFINITY;
    } else {
      actualMin = parseFloatFromUser(field.getName(), min);
      if (!minInclusive) {
        if (actualMin == Float.POSITIVE_INFINITY) return new MatchNoDocsQuery();
        actualMin = FloatPoint.nextUp(actualMin);
      }
    }
    if (max == null) {
      actualMax = Float.POSITIVE_INFINITY;
    } else {
      actualMax = parseFloatFromUser(field.getName(), max);
      if (!maxInclusive) {
        if (actualMax == Float.NEGATIVE_INFINITY) return new MatchNoDocsQuery();
        actualMax = FloatPoint.nextDown(actualMax);
      }
    }
    return FloatPoint.newRangeQuery(field.getName(), actualMin, actualMax);
  }

  @Override
  public Object toObject(SchemaField sf, BytesRef term) {
    return FloatPoint.decodeDimension(term.bytes, term.offset);
  }
  
  @Override
  public Object toObject(IndexableField f) {
    final Number val = f.numericValue();
    if (val != null) {
      if (f.fieldType().stored() == false && f.fieldType().docValuesType() == DocValuesType.NUMERIC) {
        return Float.intBitsToFloat(val.intValue());
      } else if (f.fieldType().stored() == false && f.fieldType().docValuesType() == DocValuesType.SORTED_NUMERIC) {
        return NumericUtils.sortableIntToFloat(val.intValue());
      } else  {
        return val;
      }
    } else {
      throw new AssertionError("Unexpected state. Field: '" + f + "'");
    }
  }

  @Override
  protected Query getExactQuery(SchemaField field, String externalVal) {
    return FloatPoint.newExactQuery(field.getName(), parseFloatFromUser(field.getName(), externalVal));
  }

  @Override
  public Query getSetQuery(QParser parser, SchemaField field, Collection<String> externalVal) {
    assert externalVal.size() > 0;
    if (!field.indexed()) {
      return super.getSetQuery(parser, field, externalVal);
    }
    float[] values = new float[externalVal.size()];
    int i = 0;
    for (String val:externalVal) {
      values[i] = parseFloatFromUser(field.getName(), val);
      i++;
    }
    return FloatPoint.newSetQuery(field.getName(), values);
  }

  @Override
  protected String indexedToReadable(BytesRef indexedForm) {
    return Float.toString(FloatPoint.decodeDimension(indexedForm.bytes, indexedForm.offset));
  }

  @Override
  public void readableToIndexed(CharSequence val, BytesRefBuilder result) {
    result.grow(Float.BYTES);
    result.setLength(Float.BYTES);
    FloatPoint.encodeDimension(parseFloatFromUser(null, val.toString()), result.bytes(), 0);
  }

  @Override
  public Type getUninversionType(SchemaField sf) {
    if (sf.multiValued()) {
      return null;
    } else {
      return Type.FLOAT_POINT;
    }
  }

  @Override
  public ValueSource getValueSource(SchemaField field, QParser qparser) {
    field.checkFieldCacheSource();
    return new FloatFieldSource(field.getName());
  }
  
  @Override
  protected ValueSource getSingleValueSource(SortedNumericSelector.Type choice, SchemaField f) {
    return new MultiValuedFloatFieldSource(f.getName(), choice);
  }

  @Override
  public IndexableField createField(SchemaField field, Object value) {
    float floatValue = (value instanceof Number) ? ((Number) value).floatValue() : Float.parseFloat(value.toString());
    return new FloatPoint(field.getName(), floatValue);
  }

  @Override
  protected StoredField getStoredField(SchemaField sf, Object value) {
    return new StoredField(sf.getName(), (Float) this.toNativeType(value));
  }
}
