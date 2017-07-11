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

import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.NumericUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.search.FunctionRangeQuery;
import org.apache.solr.search.QParser;
import org.apache.solr.search.function.ValueSourceRangeFilter;
import org.apache.solr.util.DateMathParser;

public abstract class NumericFieldType extends PrimitiveFieldType {

  protected NumberType type;

  /**
   * @return the type of this field
   */
  @Override
  public NumberType getNumberType() {
    return type;
  }

  private static long FLOAT_NEGATIVE_INFINITY_BITS = (long)Float.floatToIntBits(Float.NEGATIVE_INFINITY);
  private static long DOUBLE_NEGATIVE_INFINITY_BITS = Double.doubleToLongBits(Double.NEGATIVE_INFINITY);
  private static long FLOAT_POSITIVE_INFINITY_BITS = (long)Float.floatToIntBits(Float.POSITIVE_INFINITY);
  private static long DOUBLE_POSITIVE_INFINITY_BITS = Double.doubleToLongBits(Double.POSITIVE_INFINITY);
  private static long FLOAT_MINUS_ZERO_BITS = (long)Float.floatToIntBits(-0f);
  private static long DOUBLE_MINUS_ZERO_BITS = Double.doubleToLongBits(-0d);
  private static long FLOAT_ZERO_BITS = (long)Float.floatToIntBits(0f);
  private static long DOUBLE_ZERO_BITS = Double.doubleToLongBits(0d);

  protected Query getDocValuesRangeQuery(QParser parser, SchemaField field, String min, String max,
      boolean minInclusive, boolean maxInclusive) {
    assert field.hasDocValues() && (field.getType().isPointField() || !field.multiValued());
    
    switch (getNumberType()) {
      case INTEGER:
        return numericDocValuesRangeQuery(field.getName(),
              min == null ? null : (long) parseIntFromUser(field.getName(), min),
              max == null ? null : (long) parseIntFromUser(field.getName(), max),
              minInclusive, maxInclusive, field.multiValued());
      case FLOAT:
        if (field.multiValued()) {
          return getRangeQueryForMultiValuedFloatDocValues(field, min, max, minInclusive, maxInclusive);
        } else {
          return getRangeQueryForFloatDoubleDocValues(field, min, max, minInclusive, maxInclusive);
        }
      case LONG:
        return numericDocValuesRangeQuery(field.getName(),
              min == null ? null : parseLongFromUser(field.getName(), min),
              max == null ? null : parseLongFromUser(field.getName(),max),
              minInclusive, maxInclusive, field.multiValued());
      case DOUBLE:
        if (field.multiValued()) { 
          return getRangeQueryForMultiValuedDoubleDocValues(field, min, max, minInclusive, maxInclusive);
        } else {
          return getRangeQueryForFloatDoubleDocValues(field, min, max, minInclusive, maxInclusive);
        }
      case DATE:
        return numericDocValuesRangeQuery(field.getName(),
              min == null ? null : DateMathParser.parseMath(null, min).getTime(),
              max == null ? null : DateMathParser.parseMath(null, max).getTime(),
              minInclusive, maxInclusive, field.multiValued());
      default:
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for numeric field");
    }
  }
  
  protected Query getRangeQueryForFloatDoubleDocValues(SchemaField sf, String min, String max, boolean minInclusive, boolean maxInclusive) {
    Query query;
    String fieldName = sf.getName();

    Number minVal = min == null ? null : getNumberType() == NumberType.FLOAT ? parseFloatFromUser(sf.getName(), min): parseDoubleFromUser(sf.getName(), min);
    Number maxVal = max == null ? null : getNumberType() == NumberType.FLOAT ? parseFloatFromUser(sf.getName(), max): parseDoubleFromUser(sf.getName(), max);
    
    Long minBits = 
        min == null ? null : getNumberType() == NumberType.FLOAT ? (long) Float.floatToIntBits(minVal.floatValue()): Double.doubleToLongBits(minVal.doubleValue());
    Long maxBits = 
        max == null ? null : getNumberType() == NumberType.FLOAT ? (long) Float.floatToIntBits(maxVal.floatValue()): Double.doubleToLongBits(maxVal.doubleValue());
    
    long negativeInfinityBits = getNumberType() == NumberType.FLOAT ? FLOAT_NEGATIVE_INFINITY_BITS : DOUBLE_NEGATIVE_INFINITY_BITS;
    long positiveInfinityBits = getNumberType() == NumberType.FLOAT ? FLOAT_POSITIVE_INFINITY_BITS : DOUBLE_POSITIVE_INFINITY_BITS;
    long minusZeroBits = getNumberType() == NumberType.FLOAT ? FLOAT_MINUS_ZERO_BITS : DOUBLE_MINUS_ZERO_BITS;
    long zeroBits = getNumberType() == NumberType.FLOAT ? FLOAT_ZERO_BITS : DOUBLE_ZERO_BITS;
    
    // If min is negative (or -0d) and max is positive (or +0d), then issue a FunctionRangeQuery
    if ((minVal == null || minVal.doubleValue() < 0d || minBits == minusZeroBits) && 
        (maxVal == null || (maxVal.doubleValue() > 0d || maxBits == zeroBits))) {

      ValueSource vs = getValueSource(sf, null);
      query = new FunctionRangeQuery(new ValueSourceRangeFilter(vs, min, max, minInclusive, maxInclusive));

    } else { // If both max and min are negative (or -0d), then issue range query with max and min reversed
      if ((minVal == null || minVal.doubleValue() < 0d || minBits == minusZeroBits) &&
          (maxVal != null && (maxVal.doubleValue() < 0d || maxBits == minusZeroBits))) {
        query = numericDocValuesRangeQuery
            (fieldName, maxBits, (min == null ? Long.valueOf(negativeInfinityBits) : minBits), maxInclusive, minInclusive, false);
      } else { // If both max and min are positive, then issue range query
        query = numericDocValuesRangeQuery
            (fieldName, minBits, (max == null ? Long.valueOf(positiveInfinityBits) : maxBits), minInclusive, maxInclusive, false);
      }
    }
    return query;
  }
  
  protected Query getRangeQueryForMultiValuedDoubleDocValues(SchemaField sf, String min, String max, boolean minInclusive, boolean maxInclusive) {
    Long minBits = min == null ? NumericUtils.doubleToSortableLong(Double.NEGATIVE_INFINITY): NumericUtils.doubleToSortableLong(parseDoubleFromUser(sf.getName(), min));
    Long maxBits = max == null ? NumericUtils.doubleToSortableLong(Double.POSITIVE_INFINITY): NumericUtils.doubleToSortableLong(parseDoubleFromUser(sf.getName(), max));
    return numericDocValuesRangeQuery(sf.getName(), minBits, maxBits, minInclusive, maxInclusive, true);
  }
  
  protected Query getRangeQueryForMultiValuedFloatDocValues(SchemaField sf, String min, String max, boolean minInclusive, boolean maxInclusive) {
    Long minBits = (long)(min == null ? NumericUtils.floatToSortableInt(Float.NEGATIVE_INFINITY): NumericUtils.floatToSortableInt(parseFloatFromUser(sf.getName(), min)));
    Long maxBits = (long)(max == null ? NumericUtils.floatToSortableInt(Float.POSITIVE_INFINITY): NumericUtils.floatToSortableInt(parseFloatFromUser(sf.getName(), max)));
    return numericDocValuesRangeQuery(sf.getName(), minBits, maxBits, minInclusive, maxInclusive, true);
  }
  
  public static Query numericDocValuesRangeQuery(
      String field,
      Number lowerValue, Number upperValue,
      boolean lowerInclusive, boolean upperInclusive,
      boolean multiValued) {

    long actualLowerValue = Long.MIN_VALUE;
    if (lowerValue != null) {
      actualLowerValue = lowerValue.longValue();
      if (lowerInclusive == false) {
        if (actualLowerValue == Long.MAX_VALUE) {
          return new MatchNoDocsQuery();
        }
        ++actualLowerValue;
      }
    }

    long actualUpperValue = Long.MAX_VALUE;
    if (upperValue != null) {
      actualUpperValue = upperValue.longValue();
      if (upperInclusive == false) {
        if (actualUpperValue == Long.MIN_VALUE) {
          return new MatchNoDocsQuery();
        }
        --actualUpperValue;
      }
    }
    if (multiValued) {
      // In multiValued case use SortedNumericDocValuesField, this won't work for Trie*Fields wince they use BinaryDV in the multiValue case
      return SortedNumericDocValuesField.newSlowRangeQuery(field, actualLowerValue, actualUpperValue);
    } else {
      return NumericDocValuesField.newSlowRangeQuery(field, actualLowerValue, actualUpperValue);
    }
  }
  
  /** 
   * Wrapper for {@link Long#parseLong(String)} that throws a BAD_REQUEST error if the input is not valid 
   * @param fieldName used in any exception, may be null
   * @param val string to parse, NPE if null
   */
  static long parseLongFromUser(String fieldName, String val) {
    if (val == null) {
      throw new NullPointerException("Invalid input" + (null == fieldName ? "" : " for field " + fieldName));
    }
    try {
      return Long.parseLong(val);
    } catch (NumberFormatException e) {
      String msg = "Invalid Number: " + val + (null == fieldName ? "" : " for field " + fieldName);
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, msg);
    }
  }
  
  /** 
   * Wrapper for {@link Integer#parseInt(String)} that throws a BAD_REQUEST error if the input is not valid 
   * @param fieldName used in any exception, may be null
   * @param val string to parse, NPE if null
   */
  static int parseIntFromUser(String fieldName, String val) {
    if (val == null) {
      throw new NullPointerException("Invalid input" + (null == fieldName ? "" : " for field " + fieldName));
    }
    try {
      return Integer.parseInt(val);
    } catch (NumberFormatException e) {
      String msg = "Invalid Number: " + val + (null == fieldName ? "" : " for field " + fieldName);
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, msg);
    }
  }
  
  /** 
   * Wrapper for {@link Double#parseDouble(String)} that throws a BAD_REQUEST error if the input is not valid 
   * @param fieldName used in any exception, may be null
   * @param val string to parse, NPE if null
   */
  static double parseDoubleFromUser(String fieldName, String val) {
    if (val == null) {
      throw new NullPointerException("Invalid input" + (null == fieldName ? "" : " for field " + fieldName));
    }
    try {
      return Double.parseDouble(val);
    } catch (NumberFormatException e) {
      String msg = "Invalid Number: " + val + (null == fieldName ? "" : " for field " + fieldName);
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, msg);
    }
  }
  
  /** 
   * Wrapper for {@link Float#parseFloat(String)} that throws a BAD_REQUEST error if the input is not valid 
   * @param fieldName used in any exception, may be null
   * @param val string to parse, NPE if null
   */
  static float parseFloatFromUser(String fieldName, String val) {
    if (val == null) {
      throw new NullPointerException("Invalid input" + (null == fieldName ? "" : " for field " + fieldName));
    }
    try {
      return Float.parseFloat(val);
    } catch (NumberFormatException e) {
      String msg = "Invalid Number: " + val + (null == fieldName ? "" : " for field " + fieldName);
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, msg);
    }
  }
}
