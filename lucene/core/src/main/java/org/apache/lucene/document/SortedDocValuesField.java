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
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;

/**
 * <p>
 * Field that stores
 * a per-document {@link BytesRef} value, indexed for
 * sorting.  Here's an example usage:
 * 
 * <pre class="prettyprint">
 *   document.add(new SortedDocValuesField(name, new BytesRef("hello")));
 * </pre>
 * 
 * <p>
 * If you also need to store the value, you should add a
 * separate {@link StoredField} instance.
 *
 * <p>
 * This value can be at most 32766 bytes long.
 * */

public class SortedDocValuesField extends Field {

  /**
   * Type for sorted bytes DocValues
   */
  public static final FieldType TYPE = new FieldType();
  static {
    TYPE.setDocValuesType(DocValuesType.SORTED);
    TYPE.freeze();
  }

  /**
   * Create a new sorted DocValues field.
   * @param name field name
   * @param bytes binary content
   * @throws IllegalArgumentException if the field name is null
   */
  public SortedDocValuesField(String name, BytesRef bytes) {
    super(name, TYPE);
    fieldsData = bytes;
  }

  /**
   * Create a range query that matches all documents whose value is between
   * {@code lowerValue} and {@code upperValue} included.
   * <p>
   * You can have half-open ranges by setting {@code lowerValue = null}
   * or {@code upperValue = null}.
   * <p><b>NOTE</b>: Such queries cannot efficiently advance to the next match,
   * which makes them slow if they are not ANDed with a selective query. As a
   * consequence, they are best used wrapped in an {@link IndexOrDocValuesQuery},
   * alongside a range query that executes on points, such as
   * {@link BinaryPoint#newRangeQuery}.
   */
  public static Query newSlowRangeQuery(String field,
      BytesRef lowerValue, BytesRef upperValue,
      boolean lowerInclusive, boolean upperInclusive) {
    return new SortedSetDocValuesRangeQuery(field, lowerValue, upperValue, lowerInclusive, upperInclusive) {
      @Override
      SortedSetDocValues getValues(LeafReader reader, String field) throws IOException {
        return DocValues.singleton(DocValues.getSorted(reader, field));
      }
    };
  }

  /** 
   * Create a query for matching an exact {@link BytesRef} value.
   * <p><b>NOTE</b>: Such queries cannot efficiently advance to the next match,
   * which makes them slow if they are not ANDed with a selective query. As a
   * consequence, they are best used wrapped in an {@link IndexOrDocValuesQuery},
   * alongside a range query that executes on points, such as
   * {@link BinaryPoint#newExactQuery}.
   */
  public static Query newSlowExactQuery(String field, BytesRef value) {
    return newSlowRangeQuery(field, value, value, true, true);
  }
}
