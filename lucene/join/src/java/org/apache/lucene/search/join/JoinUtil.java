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

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;

import java.io.IOException;
import java.util.Locale;

/**
 * Utility for query time joining using TermsQuery and TermsCollector.
 *
 * @lucene.experimental
 */
public final class JoinUtil {

  // No instances allowed
  private JoinUtil() {
  }

  /**
   * Method for query time joining.
   * <p/>
   * Execute the returned query with a {@link IndexSearcher} to retrieve all documents that have the same terms in the
   * to field that match with documents matching the specified fromQuery and have the same terms in the from field.
   * <p/>
   * In the case a single document relates to more than one document the <code>multipleValuesPerDocument</code> option
   * should be set to true. When the <code>multipleValuesPerDocument</code> is set to <code>true</code> only the
   * the score from the first encountered join value originating from the 'from' side is mapped into the 'to' side.
   * Even in the case when a second join value related to a specific document yields a higher score. Obviously this
   * doesn't apply in the case that {@link ScoreMode#None} is used, since no scores are computed at all.
   * </p>
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

}
