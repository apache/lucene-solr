package org.apache.lucene.facet.search;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.FixedBitSet;

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

/**
 * A {@link Collector} which stores all docIDs and their scores in a
 * {@link ScoredDocIDs} instance. If scoring is not enabled, then the default
 * score as set in {@link #setDefaultScore(float)} (or
 * {@link ScoredDocIDsIterator#DEFAULT_SCORE}) will be set for all documents.
 * 
 * @lucene.experimental
 */
public abstract class ScoredDocIdCollector extends Collector {

  private static final class NonScoringDocIdCollector extends ScoredDocIdCollector {

    float defaultScore = ScoredDocIDsIterator.DEFAULT_SCORE;

    @SuppressWarnings("synthetic-access")
    public NonScoringDocIdCollector(int maxDoc) {
      super(maxDoc);
    }

    @Override
    public boolean acceptsDocsOutOfOrder() { return true; }

    @Override
    public void collect(int doc) {
      docIds.set(docBase + doc);
      ++numDocIds;
    }

    @Override
    public float getDefaultScore() {
      return defaultScore;
    }

    @Override
    protected ScoredDocIDsIterator scoredDocIdsIterator() {
      return new ScoredDocIDsIterator() {

        private DocIdSetIterator docIdsIter = docIds.iterator();
        private int nextDoc;

        @Override
        public int getDocID() { return nextDoc; }
        @Override
        public float getScore() { return defaultScore; }

        @Override
        public boolean next() {
          try {
            nextDoc = docIdsIter.nextDoc();
            return nextDoc != DocIdSetIterator.NO_MORE_DOCS;
          } catch (IOException e) {
            // This should not happen as we're iterating over an OpenBitSet. For
            // completeness, terminate iteration
            nextDoc = DocIdSetIterator.NO_MORE_DOCS;
            return false;
          }
        }

      };
    }

    @Override
    public void setDefaultScore(float defaultScore) {
      this.defaultScore = defaultScore;
    }

    @Override
    public void setScorer(Scorer scorer) {}
  }

  private static final class ScoringDocIdCollector extends ScoredDocIdCollector {

    float[] scores;
    private Scorer scorer;

    @SuppressWarnings("synthetic-access")
    public ScoringDocIdCollector(int maxDoc) {
      super(maxDoc);
      // only matching documents have an entry in the scores array. Therefore start with
      // a small array and grow when needed.
      scores = new float[64];
    }

    @Override
    public boolean acceptsDocsOutOfOrder() { return false; }

    @Override
    public void collect(int doc) throws IOException {
      docIds.set(docBase + doc);

      float score = this.scorer.score();
      if (numDocIds >= scores.length) {
        float[] newScores = new float[ArrayUtil.oversize(numDocIds + 1, 4)];
        System.arraycopy(scores, 0, newScores, 0, numDocIds);
        scores = newScores;
      }
      scores[numDocIds] = score;
      ++numDocIds;
    }

    @Override
    protected ScoredDocIDsIterator scoredDocIdsIterator() {
      return new ScoredDocIDsIterator() {

        private DocIdSetIterator docIdsIter = docIds.iterator();
        private int nextDoc;
        private int scoresIdx = -1;

        @Override
        public int getDocID() { return nextDoc; }
        @Override
        public float getScore() { return scores[scoresIdx]; }

        @Override
        public boolean next() {
          try {
            nextDoc = docIdsIter.nextDoc();
            if (nextDoc == DocIdSetIterator.NO_MORE_DOCS) {
              return false;
            }
            ++scoresIdx;
            return true;
          } catch (IOException e) {
            // This should not happen as we're iterating over an OpenBitSet. For
            // completeness, terminate iteration
            nextDoc = DocIdSetIterator.NO_MORE_DOCS;
            return false;
          }
        }

      };
    }

    @Override
    public float getDefaultScore() { return ScoredDocIDsIterator.DEFAULT_SCORE; }

    @Override
    public void setDefaultScore(float defaultScore) {}

    @Override
    public void setScorer(Scorer scorer) {
      this.scorer = scorer;
    }
  }

  protected int numDocIds;
  protected int docBase;
  protected final FixedBitSet docIds;

  /**
   * Creates a new {@link ScoredDocIdCollector} with the given parameters.
   * 
   * @param maxDoc the number of documents that are expected to be collected.
   *        Note that if more documents are collected, unexpected exceptions may
   *        be thrown. Usually you should pass {@link IndexReader#maxDoc()} of
   *        the same IndexReader with which the search is executed.
   * @param enableScoring if scoring is enabled, a score will be computed for
   *        every matching document, which might be expensive. Therefore if you
   *        do not require scoring, it is better to set it to <i>false</i>.
   */
  public static ScoredDocIdCollector create(int maxDoc, boolean enableScoring) {
    return enableScoring ? new ScoringDocIdCollector(maxDoc) : new NonScoringDocIdCollector(maxDoc);
  }

  private ScoredDocIdCollector(int maxDoc) {
    numDocIds = 0;
    docIds = new FixedBitSet(maxDoc);
  }

  protected abstract ScoredDocIDsIterator scoredDocIdsIterator() throws IOException;

  /** Returns the default score used when scoring is disabled. */
  public abstract float getDefaultScore();

  /** Set the default score. Only applicable if scoring is disabled. */
  public abstract void setDefaultScore(float defaultScore);


  public ScoredDocIDs getScoredDocIDs() {
    return new ScoredDocIDs() {

      @Override
      public ScoredDocIDsIterator iterator() throws IOException {
        return scoredDocIdsIterator();
      }

      @Override
      public DocIdSet getDocIDs() {
        return docIds;
      }

      @Override
      public int size() {
        return numDocIds;
      }

    };
  }

  @Override
  public void setNextReader(AtomicReaderContext context) throws IOException {
    this.docBase = context.docBase;
  }

}
