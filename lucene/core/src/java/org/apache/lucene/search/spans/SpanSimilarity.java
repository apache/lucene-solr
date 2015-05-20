package org.apache.lucene.search.spans;

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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.Similarity;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Encapsulates similarity statistics required for SpanScorers
 */
public abstract class SpanSimilarity {

  /**
   * The field term statistics are taken from
   */
  protected final String field;

  /**
   * Create a new SpanSimilarity
   * @param field the similarity field for term statistics
   */
  protected SpanSimilarity(String field) {
    this.field = field;
  }

  /**
   * Create a SimScorer for this SpanSimilarity's statistics
   * @param context the LeafReaderContext to calculate the scorer for
   * @return a SimScorer, or null if no scoring is required
   * @throws IOException on error
   */
  public abstract Similarity.SimScorer simScorer(LeafReaderContext context) throws IOException;

  /**
   * @return the field for term statistics
   */
  public String getField() {
    return field;
  }

  /**
   * See {@link org.apache.lucene.search.Weight#getValueForNormalization()}
   *
   * @return the value for normalization
   * @throws IOException on error
   */
  public abstract float getValueForNormalization() throws IOException;

  /**
   * See {@link org.apache.lucene.search.Weight#normalize(float,float)}
   *
   * @param queryNorm the query norm
   * @param topLevelBoost the top level boost
   */
  public abstract void normalize(float queryNorm, float topLevelBoost);

  /**
   * A SpanSimilarity class that calculates similarity statistics based on the term statistics
   * of a set of terms.
   */
  public static class ScoringSimilarity extends SpanSimilarity {

    private final Similarity similarity;
    private final Similarity.SimWeight stats;

    private ScoringSimilarity(SpanQuery query, IndexSearcher searcher, TermStatistics... termStats) throws IOException {
      super(query.getField());
      this.similarity = searcher.getSimilarity();
      this.stats = similarity.computeWeight(query.getBoost(), searcher.collectionStatistics(field), termStats);
    }

    @Override
    public Similarity.SimScorer simScorer(LeafReaderContext context) throws IOException {
      return similarity.simScorer(stats, context);
    }

    @Override
    public String getField() {
      return field;
    }

    @Override
    public float getValueForNormalization() throws IOException {
      return stats.getValueForNormalization();
    }

    @Override
    public void normalize(float queryNorm, float topLevelBoost) {
      stats.normalize(queryNorm, topLevelBoost);
    }

  }

  /**
   * A SpanSimilarity class that does no scoring
   */
  public static class NonScoringSimilarity extends SpanSimilarity {

    private NonScoringSimilarity(String field) {
      super(field);
    }

    @Override
    public Similarity.SimScorer simScorer(LeafReaderContext context) throws IOException {
      return null;
    }

    @Override
    public float getValueForNormalization() throws IOException {
      return 0;
    }

    @Override
    public void normalize(float queryNorm, float topLevelBoost) {

    }
  }

  /**
   * Build a SpanSimilarity
   * @param query the SpanQuery to be run
   * @param searcher the searcher
   * @param needsScores whether or not scores are required
   * @param stats an array of TermStatistics to use in creating the similarity
   * @return a SpanSimilarity, or null if there are no statistics to use
   * @throws IOException on error
   */
  public static SpanSimilarity build(SpanQuery query, IndexSearcher searcher,
                                     boolean needsScores, TermStatistics... stats) throws IOException {
    return needsScores ? new ScoringSimilarity(query, searcher, stats) : new NonScoringSimilarity(query.getField());
  }

  /**
   * Build a SpanSimilarity
   * @param query the SpanQuery to be run
   * @param searcher the searcher
   * @param needsScores whether or not scores are required
   * @param weights a set of {@link org.apache.lucene.search.spans.SpanWeight}s to extract terms from
   * @return a SpanSimilarity, or null if there are no statistics to use
   * @throws IOException on error
   */
  public static SpanSimilarity build(SpanQuery query, IndexSearcher searcher, boolean needsScores, List<SpanWeight> weights) throws IOException {
    return build(query, searcher, needsScores, weights.toArray(new SpanWeight[weights.size()]));
  }

  /**
   * Build a SpanSimilarity
   * @param query the SpanQuery to run
   * @param searcher the searcher
   * @param needsScores whether or not scores are required
   * @param weights an array of {@link org.apache.lucene.search.spans.SpanWeight}s to extract terms from
   * @return a SpanSimilarity, or null if there are no statistics to use
   * @throws IOException on error
   */
  public static SpanSimilarity build(SpanQuery query, IndexSearcher searcher, boolean needsScores, SpanWeight... weights) throws IOException {

    if (!needsScores)
      return new NonScoringSimilarity(query.getField());

    Map<Term, TermContext> contexts = new HashMap<>();
    for (SpanWeight w : weights) {
      w.extractTermContexts(contexts);
    }

    if (contexts.size() == 0)
      return null;

    TermStatistics[] stats = new TermStatistics[contexts.size()];
    int i = 0;
    for (Term term : contexts.keySet()) {
      stats[i] = searcher.termStatistics(term, contexts.get(term));
      i++;
    }

    return new ScoringSimilarity(query, searcher, stats);
  }

}
