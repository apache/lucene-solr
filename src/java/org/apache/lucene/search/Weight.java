package org.apache.lucene.search;

/**
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

import org.apache.lucene.index.IndexReader;

/** Expert: Calculate query weights and build query scorers.
 * <p>
 * The purpose of Weight is to make it so that searching does not modify
 * a Query, so that a Query instance can be reused. <br>
 * Searcher dependent state of the query should reside in the Weight. <br>
 * IndexReader dependent state should reside in the Scorer.
 * <p>
 * A <code>Weight</code> is used in the following way:
 * <ol>
 * <li>A <code>Weight</code> is constructed by a top-level query,
 *     given a <code>Searcher</code> ({@link Query#createWeight(Searcher)}).
 * <li>The {@link #sumOfSquaredWeights()} method is called
 *     on the <code>Weight</code> to compute
 *     the query normalization factor {@link Similarity#queryNorm(float)}
 *     of the query clauses contained in the query.
 * <li>The query normalization factor is passed to {@link #normalize(float)}.
 *     At this point the weighting is complete.
 * <li>A <code>Scorer</code> is constructed by {@link #scorer(IndexReader)}.
 * </ol>
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
