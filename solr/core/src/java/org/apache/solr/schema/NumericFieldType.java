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

import java.util.EnumSet;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
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

  private static long FLOAT_MINUS_ZERO_BITS = (long)Float.floatToIntBits(-0f);
  private static long DOUBLE_MINUS_ZERO_BITS = Double.doubleToLongBits(-0d);

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
    long minBits, maxBits;
    boolean minNegative, maxNegative;
    Number minVal, maxVal;
    if (getNumberType() == NumberType.FLOAT) {
      if (min == null) {
        minVal = Float.NEGATIVE_INFINITY;
      } else {
        minVal = parseFloatFromUser(sf.getName(), min);
        if (!minInclusive) {
          if (minVal.floatValue() == Float.POSITIVE_INFINITY) return new MatchNoDocsQuery();
          minVal = FloatPoint.nextUp(minVal.floatValue());
        }
      }
      if (max == null) {
        maxVal = Float.POSITIVE_INFINITY;
      } else {
        maxVal = parseFloatFromUser(sf.getName(), max);
        if (!maxInclusive) {
          if (maxVal.floatValue() == Float.NEGATIVE_INFINITY) return new MatchNoDocsQuery();
          maxVal = FloatPoint.nextDown(maxVal.floatValue());
        }
      }
      minBits = Float.floatToIntBits(minVal.floatValue());
      maxBits = Float.floatToIntBits(maxVal.floatValue());
      minNegative = minVal.floatValue() < 0f || minBits == FLOAT_MINUS_ZERO_BITS;
      maxNegative = maxVal.floatValue() < 0f || maxBits == FLOAT_MINUS_ZERO_BITS;
    } else {
      assert getNumberType() == NumberType.DOUBLE;
      if (min == null) {
        minVal = Double.NEGATIVE_INFINITY;
      } else {
        minVal = parseDoubleFromUser(sf.getName(), min);
        if (!minInclusive) {
          if (minVal.doubleValue() == Double.POSITIVE_INFINITY) return new MatchNoDocsQuery();
          minVal = DoublePoint.nextUp(minVal.doubleValue());
        }
      }
      if (max == null) {
        maxVal = Double.POSITIVE_INFINITY;
      } else {
        maxVal = parseDoubleFromUser(sf.getName(), max);
        if (!maxInclusive) {
          if (maxVal.doubleValue() == Double.NEGATIVE_INFINITY) return new MatchNoDocsQuery();
          maxVal = DoublePoint.nextDown(maxVal.doubleValue());
        }
      }
      minBits = Double.doubleToLongBits(minVal.doubleValue());
      maxBits = Double.doubleToLongBits(maxVal.doubleValue());
      minNegative = minVal.doubleValue() < 0d || minBits == DOUBLE_MINUS_ZERO_BITS;
      maxNegative = maxVal.doubleValue() < 0d || maxBits == DOUBLE_MINUS_ZERO_BITS;
    }
    // If min is negative (or -0d) and max is positive (or +0d), then issue a FunctionRangeQuery
    if (minNegative && !maxNegative) {
      ValueSource vs = getValueSource(sf, null);
      query = new FunctionRangeQuery(new ValueSourceRangeFilter(vs, minVal.toString(), maxVal.toString(), true, true));
    } else if (minNegative && maxNegative) {// If both max and min are negative (or -0d), then issue range query with max and min reversed
      query = numericDocValuesRangeQuery
          (fieldName, maxBits, minBits, true, true, false);
    } else { // If both max and min are positive, then issue range query
      query = numericDocValuesRangeQuery
          (fieldName, minBits, maxBits, true, true, false);
    }
    return query;
  }

  protected Query getRangeQueryForMultiValuedDoubleDocValues(SchemaField sf, String min, String max, boolean minInclusive, boolean maxInclusive) {
    double minVal,maxVal;
    if (min == null) {
      minVal = Double.NEGATIVE_INFINITY;
    } else {
      minVal = parseDoubleFromUser(sf.getName(), min);
      if (!minInclusive) {
        if (minVal == Double.POSITIVE_INFINITY) return new MatchNoDocsQuery();
        minVal = DoublePoint.nextUp(minVal);
      }
    }
    if (max == null) {
      maxVal = Double.POSITIVE_INFINITY;
    } else {
      maxVal = parseDoubleFromUser(sf.getName(), max);
      if (!maxInclusive) {
        if (maxVal == Double.NEGATIVE_INFINITY) return new MatchNoDocsQuery();
        maxVal = DoublePoint.nextDown(maxVal);
      }
    }
    Long minBits = NumericUtils.doubleToSortableLong(minVal);
    Long maxBits = NumericUtils.doubleToSortableLong(maxVal);
    return numericDocValuesRangeQuery(sf.getName(), minBits, maxBits, true, true, true);
  }

  protected Query getRangeQueryForMultiValuedFloatDocValues(SchemaField sf, String min, String max, boolean minInclusive, boolean maxInclusive) {
    float minVal,maxVal;
    if (min == null) {
      minVal = Float.NEGATIVE_INFINITY;
    } else {
      minVal = parseFloatFromUser(sf.getName(), min);
      if (!minInclusive) {
        if (minVal == Float.POSITIVE_INFINITY) return new MatchNoDocsQuery();
        minVal = FloatPoint.nextUp(minVal);
      }
    }
    if (max == null) {
      maxVal = Float.POSITIVE_INFINITY;
    } else {
      maxVal = parseFloatFromUser(sf.getName(), max);
      if (!maxInclusive) {
        if (maxVal == Float.NEGATIVE_INFINITY) return new MatchNoDocsQuery();
        maxVal = FloatPoint.nextDown(maxVal);
      }
    }
    Long minBits = (long)NumericUtils.floatToSortableInt(minVal);
    Long maxBits = (long)NumericUtils.floatToSortableInt(maxVal);
    return numericDocValuesRangeQuery(sf.getName(), minBits, maxBits, true, true, true);
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

  public static EnumSet<NumberType> doubleOrFloat = EnumSet.of(NumberType.FLOAT, NumberType.DOUBLE);

  /**
   * For doubles and floats, unbounded range queries (which do not match NaN values) are not equivalent to existence queries (which do match NaN values).
   *
   * The two types of queries are equivalent for all other numeric types.
   *
   * @param field the schema field
   * @return false for double and float fields, true for all others
   */
  @Override
  protected boolean treatUnboundedRangeAsExistence(SchemaField field) {
    return !doubleOrFloat.contains(getNumberType());
  }

  /**
   * Override the default existence behavior, so that the non-docValued/norms implementation matches NaN values for double and float fields.
   * The [* TO *] query for those fields does not match 'NaN' values, so they must be matched separately.
   * <p>
   * For doubles and floats the query behavior is equivalent to (field:[* TO *] OR field:NaN).
   * For all other numeric types, the default existence query behavior is used.
   */
  @Override
  public Query getSpecializedExistenceQuery(QParser parser, SchemaField field) {
    if (doubleOrFloat.contains(getNumberType())) {
      return new ConstantScoreQuery(new BooleanQuery.Builder()
          .add(getSpecializedRangeQuery(parser, field, null, null, true, true), BooleanClause.Occur.SHOULD)
          .add(getFieldQuery(parser, field, Float.toString(Float.NaN)), BooleanClause.Occur.SHOULD)
          .setMinimumNumberShouldMatch(1).build());
    } else {
      return super.getSpecializedExistenceQuery(parser, field);
    }
  }
}
