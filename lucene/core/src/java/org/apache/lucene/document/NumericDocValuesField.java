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
package org.apache.lucene.document;


import java.io.IOException;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.Query;

/**
 * <p>
 * Field that stores a per-document <code>long</code> value for scoring, 
 * sorting or value retrieval. Here's an example usage:
 * 
 * <pre class="prettyprint">
 *   document.add(new NumericDocValuesField(name, 22L));
 * </pre>
 * 
 * <p>
 * If you also need to store the value, you should add a
 * separate {@link StoredField} instance.
 * */

public class NumericDocValuesField extends Field {

  /**
   * Type for numeric DocValues.
   */
  public static final FieldType TYPE = new FieldType();
  static {
    TYPE.setDocValuesType(DocValuesType.NUMERIC);
    TYPE.freeze();
  }

  /** 
   * Creates a new DocValues field with the specified 64-bit long value 
   * @param name field name
   * @param value 64-bit long value
   * @throws IllegalArgumentException if the field name is null
   */
  public NumericDocValuesField(String name, long value) {
    super(name, TYPE);
    fieldsData = Long.valueOf(value);
  }

  /**
   * Create a range query that matches all documents whose value is between
   * {@code lowerValue} and {@code upperValue} included.
   * <p>
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting {@code lowerValue = Long.MIN_VALUE} or {@code upperValue = Long.MAX_VALUE}. 
   * <p>
   * Ranges are inclusive. For exclusive ranges, pass {@code Math.addExact(lowerValue, 1)}
   * or {@code Math.addExact(upperValue, -1)}.
   * <p><b>NOTE</b>: Such queries cannot efficiently advance to the next match,
   * which makes them slow if they are not ANDed with a selective query. As a
   * consequence, they are best used wrapped in an {@link IndexOrDocValuesQuery},
   * alongside a range query that executes on points, such as
   * {@link LongPoint#newRangeQuery}.
   */
  public static Query newRangeQuery(String field, long lowerValue, long upperValue) {
    return new SortedNumericDocValuesRangeQuery(field, lowerValue, upperValue) {
      @Override
      SortedNumericDocValues getValues(LeafReader reader, String field) throws IOException {
        NumericDocValues values = reader.getNumericDocValues(field);
        if (values == null) {
          return null;
        }
        return DocValues.singleton(values, reader.getDocsWithField(field));
      }
    };
  }

  /** 
   * Create a query for matching an exact long value.
   * <p><b>NOTE</b>: Such queries cannot efficiently advance to the next match,
   * which makes them slow if they are not ANDed with a selective query. As a
   * consequence, they are best used wrapped in an {@link IndexOrDocValuesQuery},
   * alongside a range query that executes on points, such as
   * {@link LongPoint#newExactQuery}.
   */
  public static Query newExactQuery(String field, long value) {
    return newRangeQuery(field, value, value);
  }
}
