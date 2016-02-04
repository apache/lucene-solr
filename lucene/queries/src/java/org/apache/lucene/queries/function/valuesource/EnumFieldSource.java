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
package org.apache.lucene.queries.function.valuesource;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSourceScorer;
import org.apache.lucene.queries.function.docvalues.IntDocValues;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.lucene.util.mutable.MutableValueInt;

/**
 * Obtains int field values from {@link org.apache.lucene.index.LeafReader#getNumericDocValues} and makes
 * those values available as other numeric types, casting as needed.
 * strVal of the value is not the int value, but its string (displayed) value
 */
public class EnumFieldSource extends FieldCacheSource {
  static final Integer DEFAULT_VALUE = -1;

  final Map<Integer, String> enumIntToStringMap;
  final Map<String, Integer> enumStringToIntMap;

  public EnumFieldSource(String field, Map<Integer, String> enumIntToStringMap, Map<String, Integer> enumStringToIntMap) {
    super(field);
    this.enumIntToStringMap = enumIntToStringMap;
    this.enumStringToIntMap = enumStringToIntMap;
  }

  private static Integer tryParseInt(String valueStr) {
    Integer intValue = null;
    try {
      intValue = Integer.parseInt(valueStr);
    }
    catch (NumberFormatException e) {
    }
    return intValue;
  }

  private String intValueToStringValue(Integer intVal) {
    if (intVal == null)
      return null;

    final String enumString = enumIntToStringMap.get(intVal);
    if (enumString != null)
      return enumString;
    // can't find matching enum name - return DEFAULT_VALUE.toString()
    return DEFAULT_VALUE.toString();
  }

  private Integer stringValueToIntValue(String stringVal) {
    if (stringVal == null)
      return null;

    Integer intValue;
    final Integer enumInt = enumStringToIntMap.get(stringVal);
    if (enumInt != null) //enum int found for string
      return enumInt;

    //enum int not found for string
    intValue = tryParseInt(stringVal);
    if (intValue == null) //not Integer
      intValue = DEFAULT_VALUE;
    final String enumString = enumIntToStringMap.get(intValue);
    if (enumString != null) //has matching string
      return intValue;

    return DEFAULT_VALUE;
  }

  @Override
  public String description() {
    return "enum(" + field + ')';
  }


  @Override
  public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
    final NumericDocValues arr = DocValues.getNumeric(readerContext.reader(), field);
    final Bits valid = DocValues.getDocsWithField(readerContext.reader(), field);

    return new IntDocValues(this) {
      final MutableValueInt val = new MutableValueInt();

      @Override
      public int intVal(int doc) {
        return (int) arr.get(doc);
      }

      @Override
      public String strVal(int doc) {
        Integer intValue = intVal(doc);
        return intValueToStringValue(intValue);
      }

      @Override
      public boolean exists(int doc) {
        return valid.get(doc);
      }

      @Override
      public ValueSourceScorer getRangeScorer(IndexReader reader, String lowerVal, String upperVal, boolean includeLower, boolean includeUpper) {
        Integer lower = stringValueToIntValue(lowerVal);
        Integer upper = stringValueToIntValue(upperVal);

        // instead of using separate comparison functions, adjust the endpoints.

        if (lower == null) {
          lower = Integer.MIN_VALUE;
        } else {
          if (!includeLower && lower < Integer.MAX_VALUE) lower++;
        }

        if (upper == null) {
          upper = Integer.MAX_VALUE;
        } else {
          if (!includeUpper && upper > Integer.MIN_VALUE) upper--;
        }

        final int ll = lower;
        final int uu = upper;

        return new ValueSourceScorer(reader, this) {
          @Override
          public boolean matches(int doc) {
            int val = intVal(doc);
            // only check for deleted if it's the default value
            // if (val==0 && reader.isDeleted(doc)) return false;
            return val >= ll && val <= uu;
          }
        };
      }

      @Override
      public ValueFiller getValueFiller() {
        return new ValueFiller() {
          private final MutableValueInt mval = new MutableValueInt();

          @Override
          public MutableValue getValue() {
            return mval;
          }

          @Override
          public void fillValue(int doc) {
            mval.value = intVal(doc);
            mval.exists = valid.get(doc);
          }
        };
      }
    };
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    EnumFieldSource that = (EnumFieldSource) o;

    if (!enumIntToStringMap.equals(that.enumIntToStringMap)) return false;
    if (!enumStringToIntMap.equals(that.enumStringToIntMap)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + enumIntToStringMap.hashCode();
    result = 31 * result + enumStringToIntMap.hashCode();
    return result;
  }
}

