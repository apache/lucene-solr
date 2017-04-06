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
import java.time.Instant;
import java.util.Collection;
import java.util.Date;

import org.apache.lucene.document.*;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.LongFieldSource;
import org.apache.lucene.queries.function.valuesource.MultiValuedLongFieldSource;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.mutable.MutableValueDate;
import org.apache.lucene.util.mutable.MutableValueLong;
import org.apache.solr.search.QParser;
import org.apache.solr.uninverting.UninvertingReader;
import org.apache.solr.util.DateMathParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatePointField extends PointField implements DateValueFieldType {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public DatePointField() {
    type = NumberType.DATE;
  }


  @Override
  public Object toNativeType(Object val) {
    if (val instanceof String) {
      return DateMathParser.parseMath(null, (String) val);
    }
    return super.toNativeType(val);
  }

  @Override
  public Query getPointRangeQuery(QParser parser, SchemaField field, String min, String max, boolean minInclusive, boolean maxInclusive) {
    long actualMin, actualMax;
    if (min == null) {
      actualMin = Long.MIN_VALUE;
    } else {
      actualMin = DateMathParser.parseMath(null, min).getTime();
      if (!minInclusive) {
        actualMin++;
      }
    }
    if (max == null) {
      actualMax = Long.MAX_VALUE;
    } else {
      actualMax = DateMathParser.parseMath(null, max).getTime();
      if (!maxInclusive) {
        actualMax--;
      }
    }
    return LongPoint.newRangeQuery(field.getName(), actualMin, actualMax);
  }

  @Override
  public Object toObject(SchemaField sf, BytesRef term) {
    return new Date(LongPoint.decodeDimension(term.bytes, term.offset));
  }

  @Override
  public Object toObject(IndexableField f) {
    final Number val = f.numericValue();
    if (val != null) {
      return new Date(val.longValue());
    } else {
      throw new AssertionError("Unexpected state. Field: '" + f + "'");
    }
  }

  @Override
  protected Query getExactQuery(SchemaField field, String externalVal) {
    return LongPoint.newExactQuery(field.getName(), DateMathParser.parseMath(null, externalVal).getTime());
  }

  @Override
  public Query getSetQuery(QParser parser, SchemaField field, Collection<String> externalVals) {
    assert externalVals.size() > 0;
    if (!field.indexed()) {
      return super.getSetQuery(parser, field, externalVals);
    }
    long[] values = new long[externalVals.size()];
    int i = 0;
    for (String val:externalVals) {
      values[i] = DateMathParser.parseMath(null, val).getTime();
      i++;
    }
    return LongPoint.newSetQuery(field.getName(), values);
  }

  @Override
  protected String indexedToReadable(BytesRef indexedForm) {
    return Instant.ofEpochMilli(LongPoint.decodeDimension(indexedForm.bytes, indexedForm.offset)).toString();
  }

  @Override
  public void readableToIndexed(CharSequence val, BytesRefBuilder result) {
    Date date = (Date) toNativeType(val.toString());
    result.grow(Long.BYTES);
    result.setLength(Long.BYTES);
    LongPoint.encodeDimension(date.getTime(), result.bytes(), 0);
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
  public UninvertingReader.Type getUninversionType(SchemaField sf) {
    if (sf.multiValued()) {
      return UninvertingReader.Type.SORTED_LONG;
    } else {
      return UninvertingReader.Type.LONG_POINT;
    }
  }

  @Override
  public ValueSource getValueSource(SchemaField field, QParser parser) {
    field.checkFieldCacheSource();
    return new DatePointFieldSource(field.getName());
  }

  @Override
  protected ValueSource getSingleValueSource(SortedNumericSelector.Type choice, SchemaField field) {
    return new MultiValuedLongFieldSource(field.getName(), choice);
  }

  @Override
  public FieldType.LegacyNumericType getNumericType() {
    return FieldType.LegacyNumericType.LONG;
  }

  @Override
  public IndexableField createField(SchemaField field, Object value, float boost) {
    Date date = (value instanceof Date)
        ? ((Date)value)
        : DateMathParser.parseMath(null, value.toString());
    return new LongPoint(field.getName(), date.getTime());
  }

  @Override
  protected StoredField getStoredField(SchemaField sf, Object value) {
    return new StoredField(sf.getName(), ((Date) this.toNativeType(value)).getTime());
  }
}

class DatePointFieldSource extends LongFieldSource {

  public DatePointFieldSource(String field) {
    super(field);
  }

  @Override
  public String description() {
    return "date(" + field + ')';
  }

  @Override
  protected MutableValueLong newMutableValueLong() {
    return new MutableValueDate();
  }

  @Override
  public Date longToObject(long val) {
    return new Date(val);
  }

  @Override
  public String longToString(long val) {
    return longToObject(val).toInstant().toString();
  }

  @Override
  public long externalToLong(String extVal) {
    return DateMathParser.parseMath(null, extVal).getTime();
  }
}
