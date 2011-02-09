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

import org.apache.lucene.search.BooleanClause.Occur;

/**
 * Expert: Common scoring functionality for different types of queries.
 *
 * <p>
 * A <code>Scorer</code> iterates over documents matching a
 * query in increasing order of doc Id.
 * </p>
 * <p>
 * Document scores are computed using a given <code>Similarity</code>
 * implementation.
 * </p>
 *
 * <p><b>NOTE</b>: The values Float.Nan,
 * Float.NEGATIVE_INFINITY and Float.POSITIVE_INFINITY are
 * not valid scores.  Certain collectors (eg {@link
 * TopScoreDocCollector}) will not properly collect hits
 * with these scores.
 */
public abstract class Scorer extends DocIdSetIterator {
  protected final Weight weight;

  /**
   * Constructs a Scorer
   * @param weight The scorers <code>Weight</code>.
   */
  protected Scorer(Weight weight) {
    this.weight = weight;
  }

  /** Scores and collects all matching documents.
   * @param collector The collector to which all matching documents are passed.
   */
  public void score(Collector collector) throws IOException {
    collector.setScorer(this);
    int doc;
    while ((doc = nextDoc()) != NO_MORE_DOCS) {
      collector.collect(doc);
    }
  }

  /**
   * Expert: Collects matching documents in a range. Hook for optimization.
   * Note, <code>firstDocID</code> is added to ensure that {@link #nextDoc()}
   * was called before this method.
   * 
   * @param collector
   *          The collector to which all matching documents are passed.
   * @param max
   *          Do not score documents past this.
   * @param firstDocID
   *          The first document ID (ensures {@link #nextDoc()} is called before
   *          this method.
   * @return true if more matching documents may remain.
   */
  public boolean score(Collector collector, int max, int firstDocID) throws IOException {
    collector.setScorer(this);
    int doc = firstDocID;
    while (doc < max) {
      collector.collect(doc);
      doc = nextDoc();
    }
    return doc != NO_MORE_DOCS;
  }
  
  /** Returns the score of the current document matching the query.
   * Initially invalid, until {@link #nextDoc()} or {@link #advance(int)}
   * is called the first time, or when called from within
   * {@link Collector#collect}.
   */
  public abstract float score() throws IOException;

  /** Returns number of matches for the current document.
   *  This returns a float (not int) because
   *  SloppyPhraseScorer discounts its freq according to how
   *  "sloppy" the match was.
   *
   * @lucene.experimental */
  public float freq() throws IOException {
    throw new UnsupportedOperationException(this + " does not implement freq()");
  }

  /**
   * A callback to gather information from a scorer and its sub-scorers. Each
   * the top-level scorer as well as each of its sub-scorers are passed to
   * either one of the visit methods depending on their boolean relationship in
   * the query.
   * @lucene.experimental
   */
  public static abstract class ScorerVisitor<P extends Query, C extends Query, S extends Scorer> {
    /**
     * Invoked for all optional scorer 
     * 
     * @param parent the parent query of the child query or <code>null</code> if the child is a top-level query
     * @param child the query of the currently visited scorer
     * @param scorer the current scorer
     */
    public void visitOptional(P parent, C child, S scorer) {}
    
    /**
     * Invoked for all required scorer 
     * 
     * @param parent the parent query of the child query or <code>null</code> if the child is a top-level query
     * @param child the query of the currently visited scorer
     * @param scorer the current scorer
     */
    public void visitRequired(P parent, C child, S scorer) {}
    
    /**
     * Invoked for all prohibited scorer 
     * 
     * @param parent the parent query of the child query or <code>null</code> if the child is a top-level query
     * @param child the query of the currently visited scorer
     * @param scorer the current scorer
     */
    public void visitProhibited(P parent, C child, S scorer) {}
  } 

  /**
   * Expert: call this to gather details for all sub-scorers for this query.
   * This can be used, in conjunction with a custom {@link Collector} to gather
   * details about how each sub-query matched the current hit.
   * 
   * @param visitor a callback executed for each sub-scorer
   * @lucene.experimental
   */
  public void visitScorers(ScorerVisitor<Query, Query, Scorer> visitor) {
    visitSubScorers(null, Occur.MUST/*must id default*/, visitor);
  }

  /**
   * {@link Scorer} subclasses should implement this method if the subclass
   * itself contains multiple scorers to support gathering details for
   * sub-scorers via {@link ScorerVisitor}
   * <p>
   * Note: this method will throw {@link UnsupportedOperationException} if no
   * associated {@link Weight} instance is provided to
   * {@link #Scorer(Weight)}
   * </p>
   * 
   * @lucene.experimental
   */
  protected void visitSubScorers(Query parent, Occur relationship,
      ScorerVisitor<Query, Query, Scorer> visitor) {
    if (weight == null)
      throw new UnsupportedOperationException();

    final Query q = weight.getQuery();
    switch (relationship) {
    case MUST:
      visitor.visitRequired(parent, q, this);
      break;
    case MUST_NOT:
      visitor.visitProhibited(parent, q, this);
      break;
    case SHOULD:
      visitor.visitOptional(parent, q, this);
      break;
    }
  }
}
