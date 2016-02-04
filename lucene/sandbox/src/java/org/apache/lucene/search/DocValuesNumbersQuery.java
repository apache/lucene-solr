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
package org.apache.lucene.search;

import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.ToStringUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Like {@link DocValuesTermsQuery}, but this query only
 * runs on a long {@link NumericDocValuesField} or a
 * {@link SortedNumericDocValuesField}, matching
 * all documents whose value in the specified field is
 * contained in the provided set of long values.
 *
 * <p>
 * <b>NOTE</b>: be very careful using this query: it is
 * typically much slower than using {@code TermsQuery},
 * but in certain specialized cases may be faster.
 *
 * @lucene.experimental
 */
public class DocValuesNumbersQuery extends Query {

  private final String field;
  private final Set<Long> numbers;

  public DocValuesNumbersQuery(String field, Set<Long> numbers) {
    this.field = Objects.requireNonNull(field);
    this.numbers = Objects.requireNonNull(numbers, "Set of numbers must not be null");
  }

  public DocValuesNumbersQuery(String field, Long... numbers) {
    this(field, new HashSet<Long>(Arrays.asList(numbers)));
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    // super.equals ensures we are the same class:
    DocValuesNumbersQuery that = (DocValuesNumbersQuery) obj;
    if (!field.equals(that.field)) {
      return false;
    }
    return numbers.equals(that.numbers);
  }

  @Override
  public int hashCode() {
    return 31 * super.hashCode() + Objects.hash(field, numbers);
  }

  @Override
  public String toString(String defaultField) {
    StringBuilder sb = new StringBuilder();
    sb.append(field).append(": [");
    for (Long number : numbers) {
      sb.append(number).append(", ");
    }
    if (numbers.size() > 0) {
      sb.setLength(sb.length() - 2);
    }
    return sb.append(']').append(ToStringUtils.boost(getBoost())).toString();
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    return new RandomAccessWeight(this) {

      @Override
      protected Bits getMatchingDocs(final LeafReaderContext context) throws IOException {
         final SortedNumericDocValues values = DocValues.getSortedNumeric(context.reader(), field);
         return new Bits() {

           @Override
           public boolean get(int doc) {
             values.setDocument(doc);
             int count = values.count();
             for(int i=0;i<count;i++) {
               if (numbers.contains(values.valueAt(i))) {
                 return true;
               }
             }

             return false;
          }

          @Override
          public int length() {
            return context.reader().maxDoc();
          }
        };
      }
    };
  }
}
