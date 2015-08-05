package org.apache.lucene.search.join;

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

import java.io.IOException;
import java.util.Locale;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;

/**
 * Utility for query time joining.
 *
 * @lucene.experimental
 */
public final class JoinUtil {

  // No instances allowed
  private JoinUtil() {
  }

  /**
   * Method for query time joining.
   * <p>
   * Execute the returned query with a {@link IndexSearcher} to retrieve all documents that have the same terms in the
   * to field that match with documents matching the specified fromQuery and have the same terms in the from field.
   * <p>
   * In the case a single document relates to more than one document the <code>multipleValuesPerDocument</code> option
   * should be set to true. When the <code>multipleValuesPerDocument</code> is set to <code>true</code> only the
   * the score from the first encountered join value originating from the 'from' side is mapped into the 'to' side.
   * Even in the case when a second join value related to a specific document yields a higher score. Obviously this
   * doesn't apply in the case that {@link ScoreMode#None} is used, since no scores are computed at all.
   * <p>
   * Memory considerations: During joining all unique join values are kept in memory. On top of that when the scoreMode
   * isn't set to {@link ScoreMode#None} a float value per unique join value is kept in memory for computing scores.
   * When scoreMode is set to {@link ScoreMode#Avg} also an additional integer value is kept in memory per unique
   * join value.
   *
   * @param fromField                 The from field to join from
   * @param multipleValuesPerDocument Whether the from field has multiple terms per document
   * @param toField                   The to field to join to
   * @param fromQuery                 The query to match documents on the from side
   * @param fromSearcher              The searcher that executed the specified fromQuery
   * @param scoreMode                 Instructs how scores from the fromQuery are mapped to the returned query
   * @return a {@link Query} instance that can be used to join documents based on the
   *         terms in the from and to field
   * @throws IOException If I/O related errors occur
   */
  public static Query createJoinQuery(String fromField,
                                      boolean multipleValuesPerDocument,
                                      String toField,
                                      Query fromQuery,
                                      IndexSearcher fromSearcher,
                                      ScoreMode scoreMode) throws IOException {
    switch (scoreMode) {
      case None:
        TermsCollector termsCollector = TermsCollector.create(fromField, multipleValuesPerDocument);
        fromSearcher.search(fromQuery, termsCollector);
        return new TermsQuery(toField, fromQuery, termsCollector.getCollectorTerms());
      case Total:
      case Max:
      case Min:
      case Avg:
        TermsWithScoreCollector termsWithScoreCollector =
            TermsWithScoreCollector.create(fromField, multipleValuesPerDocument, scoreMode);
        fromSearcher.search(fromQuery, termsWithScoreCollector);
        return new TermsIncludingScoreQuery(
            toField,
            multipleValuesPerDocument,
            termsWithScoreCollector.getCollectedTerms(),
            termsWithScoreCollector.getScoresPerTerm(),
            fromQuery
        );
      default:
        throw new IllegalArgumentException(String.format(Locale.ROOT, "Score mode %s isn't supported.", scoreMode));
    }
  }

  /**
   * Delegates to {@link #createJoinQuery(String, Query, Query, IndexSearcher, ScoreMode, MultiDocValues.OrdinalMap, int, int)},
   * but disables the min and max filtering.
   *
   * @param joinField   The {@link SortedDocValues} field containing the join values
   * @param fromQuery   The query containing the actual user query. Also the fromQuery can only match "from" documents.
   * @param toQuery     The query identifying all documents on the "to" side.
   * @param searcher    The index searcher used to execute the from query
   * @param scoreMode   Instructs how scores from the fromQuery are mapped to the returned query
   * @param ordinalMap  The ordinal map constructed over the joinField. In case of a single segment index, no ordinal map
   *                    needs to be provided.
   * @return a {@link Query} instance that can be used to join documents based on the join field
   * @throws IOException If I/O related errors occur
   */
  public static Query createJoinQuery(String joinField,
                                      Query fromQuery,
                                      Query toQuery,
                                      IndexSearcher searcher,
                                      ScoreMode scoreMode,
                                      MultiDocValues.OrdinalMap ordinalMap) throws IOException {
    return createJoinQuery(joinField, fromQuery, toQuery, searcher, scoreMode, ordinalMap, 0, Integer.MAX_VALUE);
  }

  /**
   * A query time join using global ordinals over a dedicated join field.
   *
   * This join has certain restrictions and requirements:
   * 1) A document can only refer to one other document. (but can be referred by one or more documents)
   * 2) Documents on each side of the join must be distinguishable. Typically this can be done by adding an extra field
   *    that identifies the "from" and "to" side and then the fromQuery and toQuery must take the this into account.
   * 3) There must be a single sorted doc values join field used by both the "from" and "to" documents. This join field
   *    should store the join values as UTF-8 strings.
   * 4) An ordinal map must be provided that is created on top of the join field.
   *
   * Note: min and max filtering and the avg score mode will require this join to keep track of the number of times
   * a document matches per join value. This will increase the per join cost in terms of execution time and memory.
   *
   * @param joinField   The {@link SortedDocValues} field containing the join values
   * @param fromQuery   The query containing the actual user query. Also the fromQuery can only match "from" documents.
   * @param toQuery     The query identifying all documents on the "to" side.
   * @param searcher    The index searcher used to execute the from query
   * @param scoreMode   Instructs how scores from the fromQuery are mapped to the returned query
   * @param ordinalMap  The ordinal map constructed over the joinField. In case of a single segment index, no ordinal map
   *                    needs to be provided.
   * @param min         Optionally the minimum number of "from" documents that are required to match for a "to" document
   *                    to be a match. The min is inclusive. Setting min to 0 and max to <code>Interger.MAX_VALUE</code>
   *                    disables the min and max "from" documents filtering
   * @param max         Optionally the maximum number of "from" documents that are allowed to match for a "to" document
   *                    to be a match. The max is inclusive. Setting min to 0 and max to <code>Interger.MAX_VALUE</code>
   *                    disables the min and max "from" documents filtering
   * @return a {@link Query} instance that can be used to join documents based on the join field
   * @throws IOException If I/O related errors occur
   */
  public static Query createJoinQuery(String joinField,
                                      Query fromQuery,
                                      Query toQuery,
                                      IndexSearcher searcher,
                                      ScoreMode scoreMode,
                                      MultiDocValues.OrdinalMap ordinalMap,
                                      int min,
                                      int max) throws IOException {
    IndexReader indexReader = searcher.getIndexReader();
    int numSegments = indexReader.leaves().size();
    final long valueCount;
    if (numSegments == 0) {
      return new MatchNoDocsQuery();
    } else if (numSegments == 1) {
      // No need to use the ordinal map, because there is just one segment.
      ordinalMap = null;
      LeafReader leafReader = searcher.getIndexReader().leaves().get(0).reader();
      SortedDocValues joinSortedDocValues = leafReader.getSortedDocValues(joinField);
      if (joinSortedDocValues != null) {
        valueCount = joinSortedDocValues.getValueCount();
      } else {
        return new MatchNoDocsQuery();
      }
    } else {
      if (ordinalMap == null) {
        throw new IllegalArgumentException("OrdinalMap is required, because there is more than 1 segment");
      }
      valueCount = ordinalMap.getValueCount();
    }

    final Query rewrittenFromQuery = searcher.rewrite(fromQuery);
    final Query rewrittenToQuery = searcher.rewrite(toQuery);
    GlobalOrdinalsWithScoreCollector globalOrdinalsWithScoreCollector;
    switch (scoreMode) {
      case Total:
        globalOrdinalsWithScoreCollector = new GlobalOrdinalsWithScoreCollector.Sum(joinField, ordinalMap, valueCount, min, max);
        break;
      case Min:
        globalOrdinalsWithScoreCollector = new GlobalOrdinalsWithScoreCollector.Min(joinField, ordinalMap, valueCount, min, max);
        break;
      case Max:
        globalOrdinalsWithScoreCollector = new GlobalOrdinalsWithScoreCollector.Max(joinField, ordinalMap, valueCount, min, max);
        break;
      case Avg:
        globalOrdinalsWithScoreCollector = new GlobalOrdinalsWithScoreCollector.Avg(joinField, ordinalMap, valueCount, min, max);
        break;
      case None:
        if (min <= 0 && max == Integer.MAX_VALUE) {
          GlobalOrdinalsCollector globalOrdinalsCollector = new GlobalOrdinalsCollector(joinField, ordinalMap, valueCount);
          searcher.search(rewrittenFromQuery, globalOrdinalsCollector);
          return new GlobalOrdinalsQuery(globalOrdinalsCollector.getCollectorOrdinals(), joinField, ordinalMap, rewrittenToQuery, rewrittenFromQuery, indexReader);
        } else {
          globalOrdinalsWithScoreCollector = new GlobalOrdinalsWithScoreCollector.NoScore(joinField, ordinalMap, valueCount, min, max);
          break;
        }
      default:
        throw new IllegalArgumentException(String.format(Locale.ROOT, "Score mode %s isn't supported.", scoreMode));
    }
    searcher.search(rewrittenFromQuery, globalOrdinalsWithScoreCollector);
    return new GlobalOrdinalsWithScoreQuery(globalOrdinalsWithScoreCollector, joinField, ordinalMap, rewrittenToQuery, rewrittenFromQuery, min, max, indexReader);
  }

}
