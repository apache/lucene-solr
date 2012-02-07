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
import java.util.Collection;
import java.util.Collections;

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
  
  /** returns parent Weight
   * @lucene.experimental
   */
  public Weight getWeight() {
    return weight;
  }
  
  /** Returns child sub-scorers
   * @lucene.experimental */
  public Collection<ChildScorer> getChildren() {
    return Collections.emptyList();
  }
  
  /** a child Scorer and its relationship to its parent.
   * the meaning of the relationship depends upon the parent query. 
   * @lucene.experimental */
  public static class ChildScorer {
    public final Scorer child;
    public final String relationship;
    
    public ChildScorer(Scorer child, String relationship) {
      this.child = child;
      this.relationship = relationship;
    }
  }
}
