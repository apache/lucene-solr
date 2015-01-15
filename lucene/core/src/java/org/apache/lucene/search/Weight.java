package org.apache.lucene.search;

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

import org.apache.lucene.index.LeafReaderContext;
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
 * {@link org.apache.lucene.index.LeafReader} dependent state should reside in the {@link Scorer}.
 * <p>
 * Since {@link Weight} creates {@link Scorer} instances for a given
 * {@link org.apache.lucene.index.LeafReaderContext} ({@link #scorer(org.apache.lucene.index.LeafReaderContext, Bits)})
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
 * {@link #scorer(org.apache.lucene.index.LeafReaderContext, Bits)}.
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
  public abstract Explanation explain(LeafReaderContext context, int doc) throws IOException;

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
   * <b>NOTE:</b> null can be returned if no documents will be scored by this
   * query.
   * 
   * @param context
   *          the {@link org.apache.lucene.index.LeafReaderContext} for which to return the {@link Scorer}.
   * @param acceptDocs
   *          Bits that represent the allowable docs to match (typically deleted docs
   *          but possibly filtering other documents)
   *          
   * @return a {@link Scorer} which scores documents in/out-of order.
   * @throws IOException if there is a low-level I/O error
   */
  public abstract Scorer scorer(LeafReaderContext context, Bits acceptDocs) throws IOException;

  /**
   * Optional method, to return a {@link BulkScorer} to
   * score the query and send hits to a {@link Collector}.
   * Only queries that have a different top-level approach
   * need to override this; the default implementation
   * pulls a normal {@link Scorer} and iterates and
   * collects the resulting hits.
   *
   * @param context
   *          the {@link org.apache.lucene.index.LeafReaderContext} for which to return the {@link Scorer}.
   * @param acceptDocs
   *          Bits that represent the allowable docs to match (typically deleted docs
   *          but possibly filtering other documents)
   *
   * @return a {@link BulkScorer} which scores documents and
   * passes them to a collector.
   * @throws IOException if there is a low-level I/O error
   */
  public BulkScorer bulkScorer(LeafReaderContext context, Bits acceptDocs) throws IOException {

    Scorer scorer = scorer(context, acceptDocs);
    if (scorer == null) {
      // No docs match
      return null;
    }

    // This impl always scores docs in order, so we can
    // ignore scoreDocsInOrder:
    return new DefaultBulkScorer(scorer);
  }

  /** Just wraps a Scorer and performs top scoring using it. */
  static class DefaultBulkScorer extends BulkScorer {
    private final Scorer scorer;

    public DefaultBulkScorer(Scorer scorer) {
      if (scorer == null) {
        throw new NullPointerException();
      }
      this.scorer = scorer;
    }

    @Override
    public boolean score(LeafCollector collector, int max) throws IOException {
      // TODO: this may be sort of weird, when we are
      // embedded in a BooleanScorer, because we are
      // called for every chunk of 2048 documents.  But,
      // then, scorer is a FakeScorer in that case, so any
      // Collector doing something "interesting" in
      // setScorer will be forced to use BS2 anyways:
      collector.setScorer(scorer);
      if (max == DocIdSetIterator.NO_MORE_DOCS) {
        scoreAll(collector, scorer);
        return false;
      } else {
        int doc = scorer.docID();
        if (doc < 0) {
          doc = scorer.nextDoc();
        }
        return scoreRange(collector, scorer, doc, max);
      }
    }

    /** Specialized method to bulk-score a range of hits; we
     *  separate this from {@link #scoreAll} to help out
     *  hotspot.
     *  See <a href="https://issues.apache.org/jira/browse/LUCENE-5487">LUCENE-5487</a> */
    static boolean scoreRange(LeafCollector collector, Scorer scorer, int currentDoc, int end) throws IOException {
      while (currentDoc < end) {
        collector.collect(currentDoc);
        currentDoc = scorer.nextDoc();
      }
      return currentDoc != DocIdSetIterator.NO_MORE_DOCS;
    }
    
    /** Specialized method to bulk-score all hits; we
     *  separate this from {@link #scoreRange} to help out
     *  hotspot.
     *  See <a href="https://issues.apache.org/jira/browse/LUCENE-5487">LUCENE-5487</a> */
    static void scoreAll(LeafCollector collector, Scorer scorer) throws IOException {
      int doc;
      while ((doc = scorer.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        collector.collect(doc);
      }
    }
  }
}
