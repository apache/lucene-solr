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

import java.io.IOException;
import java.util.Objects;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.Weight;

/**
 * A range query that can take advantage of the fact that the index is sorted to speed up
 * execution. If the index is sorted on the same field as the query, it performs binary
 * search on the field's numeric doc values to find the documents at the lower and upper
 * ends of the range.
 *
 * This optimized execution strategy is only used if the following conditions hold:
 * <ul>
 *   <li> The index is sorted, and its primary sort is on the same field as the query.
 *   <li> The query field has either {@link SortedNumericDocValues} or {@link NumericDocValues}.
 *   <li> The segments must have at most one field value per document (otherwise we cannot easily
 * determine the matching document IDs through a binary search).
 * </ul>
 *
 * If any of these conditions isn't met, the search is delegated to {@code fallbackQuery}.
 *
 * This fallback must be an equivalent range query -- it should produce the same documents and give
 * constant scores. As an example, an {@link IndexSortSortedNumericDocValuesRangeQuery} might be
 * constructed as follows:
 * <pre class="prettyprint">
 *   String field = "field";
 *   long lowerValue = 0, long upperValue = 10;
 *   Query fallbackQuery = LongPoint.newRangeQuery(field, lowerValue, upperValue);
 *   Query rangeQuery = new IndexSortSortedNumericDocValuesRangeQuery(
 *       field, lowerValue, upperValue, fallbackQuery);
 * </pre>
 *
 * @lucene.experimental
 */
public class IndexSortSortedNumericDocValuesRangeQuery extends Query {

  private final String field;
  private final long lowerValue;
  private final long upperValue;
  private final Query fallbackQuery;

  /**
   * Creates a new {@link IndexSortSortedNumericDocValuesRangeQuery}.
   *
   * @param field The field name.
   * @param lowerValue The lower end of the range (inclusive).
   * @param upperValue The upper end of the range (exclusive).
   * @param fallbackQuery A query to fall back to if the optimization cannot be applied.
      */
  public IndexSortSortedNumericDocValuesRangeQuery(String field,
                                                   long lowerValue,
                                                   long upperValue,
                                                   Query fallbackQuery) {
    this.field = Objects.requireNonNull(field);
    this.lowerValue = lowerValue;
    this.upperValue = upperValue;
    this.fallbackQuery = fallbackQuery;
  }

  public Query getFallbackQuery() {
    return fallbackQuery;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    IndexSortSortedNumericDocValuesRangeQuery that = (IndexSortSortedNumericDocValuesRangeQuery) o;
    return lowerValue == that.lowerValue &&
        upperValue == that.upperValue &&
        Objects.equals(field, that.field) &&
        Objects.equals(fallbackQuery, that.fallbackQuery);
  }

  @Override
  public int hashCode() {
    return Objects.hash(field, lowerValue, upperValue, fallbackQuery);
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
    if (this.field.equals(field) == false) {
      b.append(this.field).append(":");
    }
    return b
        .append("[")
        .append(lowerValue)
        .append(" TO ")
        .append(upperValue)
        .append("]")
        .toString();
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    if (lowerValue == Long.MIN_VALUE && upperValue == Long.MAX_VALUE) {
      return new DocValuesFieldExistsQuery(field);
    }

    Query rewrittenFallback = fallbackQuery.rewrite(reader);
    if (rewrittenFallback == fallbackQuery) {
      return this;
    } else {
      return new IndexSortSortedNumericDocValuesRangeQuery(
          field, lowerValue, upperValue, rewrittenFallback);
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
            IndexSortNumericDocValueHelper.BoundedNumericDocValues disi
                = IndexSortNumericDocValueHelper.boundNumericDocValueOptimizedByIndexSort(lowerValue, upperValue, sortField, numericValues, context);
            return new ConstantScoreScorer(this, score(), scoreMode, disi);
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
}
