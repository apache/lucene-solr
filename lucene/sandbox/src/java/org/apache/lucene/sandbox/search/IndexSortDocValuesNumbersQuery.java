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
package org.apache.lucene.sandbox.search;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PointInSetQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * The basic idea is like as {@link DocValuesNumbersQuery}, but try to take advantage of index
 * sort to speed it up.
 * <p>
 * This optimized execution strategy is only used if the following conditions hold:
 * <ul>
 *   <li> The index is sorted, and its primary sort is on the same field as the query.
 *   <li> The query field has either {@link SortedNumericDocValues} or {@link NumericDocValues}.
 *   <li> The segments must have at most one field value per document (otherwise we cannot easily
 * determine the matching document IDs through a binary search).
 * </ul>
 * <p>
 * If any of these conditions isn't met, the search is delegated to {@code fallbackQuery}.
 * This fallback must be an equivalent query -- it should produce the same documents and give
 * constant scores.
 *
 * <b>NOTE</b>: If the index sort optimization is applied, this can be faster than {@link DocValuesNumbersQuery},
 * but there is no guarantee that this will be faster than a {@link PointInSetQuery}. More details can be found
 * in {@link DocValuesTermsQuery}.
 * <p>
 * Here is a simple benchmark based on 100,000,000 doc, each long in query can hit at least one doc:
 *
 * <table>
 *   <caption>Benchmark Results</caption>
 *   <tr><th>LongsCount</th>
 *   <th>{@link PointInSetQuery}(s/op)</th>
 *   <th>{@link IndexSortDocValuesNumbersQuery}(s/op)</th>
 *   <th>{@link DocValuesNumbersQuery}(s/op)</th></tr>
 *   <tr><td>1000</td><td>0.189 ± 0.003</td><td>0.860 ± 0.028</td><td>2.408 ± 0.087</td></tr>
 *   <tr><td>5000</td><td>0.715 ± 0.010</td><td>0.879 ± 0.010</td><td>2.642 ± 0.105</td></tr>
 *   <tr><td>10000</td><td>1.075 ± 0.034</td><td>0.859 ± 0.010</td><td>2.629 ± 0.077</td></tr>
 *   <tr><td>50000</td><td>2.652 ± 0.114</td><td>0.865 ± 0.009</td><td>2.737 ± 0.099</td></tr>
 *   <tr><td>100000</td><td>2.905 ± 0.027</td><td>0.881 ± 0.005</td><td>2.820 ± 0.474</td></tr>
 * </table>
 *
 * @lucene.experimental
 */
public class IndexSortDocValuesNumbersQuery extends Query {
  private final String field;
  private final long[] values;
  private final Query fallbackQuery;

  /**
   * Creates a new {@link IndexSortDocValuesNumbersQuery}.
   *
   * @param field         The field name.
   * @param fallbackQuery A query to fall back to if the optimization cannot be applied.
   * @param values        Numbers you want to query
   */
  public IndexSortDocValuesNumbersQuery(String field, Query fallbackQuery, long... values) {
    this.field = Objects.requireNonNull(field);
    this.values = values.clone();
    Arrays.sort(this.values);
    this.fallbackQuery = fallbackQuery;
  }

  public Query getFallbackQuery() {
    return fallbackQuery;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    IndexSortDocValuesNumbersQuery that = (IndexSortDocValuesNumbersQuery) o;
    return Objects.equals(field, that.field) &&
        Arrays.equals(values, that.values) &&
        Objects.equals(fallbackQuery, that.fallbackQuery);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(field, fallbackQuery);
    result = 31 * result + Arrays.hashCode(values);
    return result;
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field)) {
      visitor.visitLeaf(this);
      fallbackQuery.visit(visitor);
    }
  }

  @Override
  public String toString(String field) {
    StringBuilder b = new StringBuilder();
    if (!this.field.equals(field)) {
      b.append(this.field).append(":");
    }
    return b.append(Arrays.toString(values))
        .toString();
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    Query rewrittenFallback = fallbackQuery.rewrite(reader);
    if (rewrittenFallback == fallbackQuery) {
      return this;
    } else {
      return new IndexSortDocValuesNumbersQuery(
          field, rewrittenFallback, values);
    }
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    Weight fallbackWeight = fallbackQuery.createWeight(searcher, scoreMode, boost);

    return new ConstantScoreWeight(this, boost) {
      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        SortedNumericDocValues sortedNumericValues = DocValues.getSortedNumeric(context.reader(), field);
        NumericDocValues numericValues = DocValues.unwrapSingleton(sortedNumericValues);

        if (numericValues != null) {
          Sort indexSort = context.reader().getMetaData().getSort();
          if (indexSort != null
              && indexSort.getSort().length > 0
              && indexSort.getSort()[0].getField().equals(field)) {
            SortField sortField = indexSort.getSort()[0];
            IndexSortNumericDocValueHelper.BoundedNumericDocValues singleton = boundDocValues(sortField, context, numericValues);
            if (singleton == null) {
              return null;
            }
            // the pos will move at most values.length times, with indexing
            // in an array and compare to value, so we get a *3 here.
            // +4 is a estimate value, if we returned luckily in the
            // first if, the cost can be 2 (array & compare), otherwise
            // it can be 6
            final float avgCost = ((float) values.length / singleton.cost()) * 3 + 4;
            TwoPhaseIterator iterator;
            if (!sortField.getReverse()) {
              iterator = new TwoPhaseIterator(singleton) {
                private int pos = 0;

                @Override
                public boolean matches() throws IOException {
                  long value = singleton.longValue();
                  if (values[pos] > value) {
                    return false;
                  }
                  while (values[pos] < value) {
                    pos++;
                  }
                  return values[pos] == value;
                }

                @Override
                public float matchCost() {
                  return avgCost;
                }
              };
            } else {
              iterator = new TwoPhaseIterator(singleton) {
                private int pos = values.length - 1;

                @Override
                public boolean matches() throws IOException {
                  long value = singleton.longValue();
                  if (values[pos] < value) {
                    return false;
                  }
                  while (values[pos] > value) {
                    pos--;
                  }
                  return values[pos] == value;
                }

                @Override
                public float matchCost() {
                  return avgCost;
                }
              };
            }
            return new ConstantScoreScorer(this, score(), scoreMode, iterator);
          }
        }
        return fallbackWeight.scorer(context);
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        // Both queries should always return the same values, so we can just check
        // if the fallback query is cacheable.
        return fallbackWeight.isCacheable(ctx);
      }
    };
  }

  /**
   * use the max value and min value of longs to limit doc value range by binary search
   * null can be returned to indicate no doc will match
   */
  private IndexSortNumericDocValueHelper.BoundedNumericDocValues boundDocValues(SortField sortField,
                                                                                LeafReaderContext context,
                                                                                NumericDocValues delegate) throws IOException {
    long miss = sortField.getMissingValue() == null ? 0L : (long) sortField.getMissingValue();
    int maxDoc = context.reader().maxDoc();
    NumericDocValues tmp = DocValues.unwrapSingleton(DocValues.getSortedNumeric(context.reader(), sortField.getField()));
    if (tmp == null) {
      return null;
    }
    tmp.nextDoc();
    long first = tmp.longValue();
    long last = tmp.advanceExact(maxDoc - 1) ? tmp.longValue() : miss;
    long min = sortField.getReverse() ? last : first;
    long max = sortField.getReverse() ? first : last;
    long lowerValue = values[0];
    long upperValue = values[values.length - 1];
    // different from range query, terms query's result docs can be thin
    // terms: 1, 10, 11, 100
    // doc values: [2, 99]
    // the range can be [10, 11] instead of [1, 100] with following loops
    for (int i = 0; i < values.length; i++) {
      if (values[i] >= min) {
        lowerValue = values[i];
        break;
      }
    }
    for (int i = values.length - 1; i >= 0; i--) {
      if (values[i] <= max) {
        upperValue = values[i];
        break;
      }
    }
    if (lowerValue > max || upperValue < min) {
      return null;
    }

    return IndexSortNumericDocValueHelper
        .boundNumericDocValueOptimizedByIndexSort(lowerValue, upperValue, sortField, delegate, context);
  }
}
