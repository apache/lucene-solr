package org.apache.lucene.search;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.lucene.index.IndexReader;

/** Expert: Calculate query weights and build query scorers.
 *
 * <p>A Weight is constructed by a query, given a Searcher ({@link
 * Query#createWeight(Searcher)}).  The {@link #sumOfSquaredWeights()} method
 * is then called on the top-level query to compute the query normalization
 * factor (@link Similarity#queryNorm(float)}).  This factor is then passed to
 * {@link #normalize(float)}.  At this point the weighting is complete and a
 * scorer may be constructed by calling {@link #scorer(IndexReader)}.
 */
public interface Weight extends java.io.Serializable {
  /** The query that this concerns. */
  Query getQuery();

  /** The weight for this query. */
  float getValue();

  /** The sum of squared weights of contained query clauses. */
  float sumOfSquaredWeights() throws IOException;

  /** Assigns the query normalization factor to this. */
  void normalize(float norm);

  /** Constructs a scorer for this. */
  Scorer scorer(IndexReader reader) throws IOException;

  /** An explanation of the score computation for the named document. */
  Explanation explain(IndexReader reader, int doc) throws IOException;
}
