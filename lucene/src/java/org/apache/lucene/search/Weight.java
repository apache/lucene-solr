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

import org.apache.lucene.index.AtomicReader; // javadocs
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReaderContext; // javadocs
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.Bits;

/**
 * Expert: Calculate query weights and build query scorers.
 * <p>
 * The purpose of {@link Weight} is to ensure searching does not modify a
 * {@link Query}, so that a {@link Query} instance can be reused. <br>
 * {@link IndexSearcher} dependent state of the query should reside in the
 * {@link Weight}. <br>
 * {@link AtomicReader} dependent state should reside in the {@link Scorer}.
 * <p>
 * Since {@link Weight} creates {@link Scorer} instances for a given
 * {@link AtomicReaderContext} ({@link #scorer(AtomicReaderContext, 
 * boolean, boolean, Bits)})
 * callers must maintain the relationship between the searcher's top-level
 * {@link IndexReaderContext} and the context used to create a {@link Scorer}. 
 * <p>
 * A <code>Weight</code> is used in the following way:
 * <ol>
 * <li>A <code>Weight</code> is constructed by a top-level query, given a
 * <code>IndexSearcher</code> ({@link Query#createWeight(IndexSearcher)}).
 * <li>The {@link #getValueForNormalization()} method is called on the
 * <code>Weight</code> to compute the query normalization factor
 * {@link Similarity#queryNorm(float)} of the query clauses contained in the
 * query.
 * <li>The query normalization factor is passed to {@link #normalize(float, float)}. At
 * this point the weighting is complete.
 * <li>A <code>Scorer</code> is constructed by
 * {@link #scorer(AtomicReaderContext, boolean, boolean, Bits)}.
 * </ol>
 * 
 * @since 2.9
 */
public abstract class Weight {

  /**
   * An explanation of the score computation for the named document.
   * 
   * @param context the readers context to create the {@link Explanation} for.
   * @param doc the document's id relative to the given context's reader
   * @return an Explanation for the score
   * @throws IOException if an {@link IOException} occurs
   */
  public abstract Explanation explain(AtomicReaderContext context, int doc) throws IOException;

  /** The query that this concerns. */
  public abstract Query getQuery();
  
  /** The value for normalization of contained query clauses (e.g. sum of squared weights). */
  public abstract float getValueForNormalization() throws IOException;

  /** Assigns the query normalization factor and boost from parent queries to this. */
  public abstract void normalize(float norm, float topLevelBoost);

  /**
   * Returns a {@link Scorer} which scores documents in/out-of order according
   * to <code>scoreDocsInOrder</code>.
   * <p>
   * <b>NOTE:</b> even if <code>scoreDocsInOrder</code> is false, it is
   * recommended to check whether the returned <code>Scorer</code> indeed scores
   * documents out of order (i.e., call {@link #scoresDocsOutOfOrder()}), as
   * some <code>Scorer</code> implementations will always return documents
   * in-order.<br>
   * <b>NOTE:</b> null can be returned if no documents will be scored by this
   * query.
   * 
   * @param context
   *          the {@link AtomicReaderContext} for which to return the {@link Scorer}.
   * @param scoreDocsInOrder
   *          specifies whether in-order scoring of documents is required. Note
   *          that if set to false (i.e., out-of-order scoring is required),
   *          this method can return whatever scoring mode it supports, as every
   *          in-order scorer is also an out-of-order one. However, an
   *          out-of-order scorer may not support {@link Scorer#nextDoc()}
   *          and/or {@link Scorer#advance(int)}, therefore it is recommended to
   *          request an in-order scorer if use of these methods is required.
   * @param topScorer
   *          if true, {@link Scorer#score(Collector)} will be called; if false,
   *          {@link Scorer#nextDoc()} and/or {@link Scorer#advance(int)} will
   *          be called.
   * @param acceptDocs
   *          Bits that represent the allowable docs to match (typically deleted docs
   *          but possibly filtering other documents)
   *          
   * @return a {@link Scorer} which scores documents in/out-of order.
   * @throws IOException
   */
  public abstract Scorer scorer(AtomicReaderContext context, boolean scoreDocsInOrder,
      boolean topScorer, Bits acceptDocs) throws IOException;

  /**
   * Returns true iff this implementation scores docs only out of order. This
   * method is used in conjunction with {@link Collector}'s
   * {@link Collector#acceptsDocsOutOfOrder() acceptsDocsOutOfOrder} and
   * {@link #scorer(AtomicReaderContext, boolean, boolean, Bits)} to
   * create a matching {@link Scorer} instance for a given {@link Collector}, or
   * vice versa.
   * <p>
   * <b>NOTE:</b> the default implementation returns <code>false</code>, i.e.
   * the <code>Scorer</code> scores documents in-order.
   */
  public boolean scoresDocsOutOfOrder() { return false; }
}
