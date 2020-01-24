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
package org.apache.lucene.search.similarities;


import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.util.SmallFloat;

/**
 * A subclass of {@code Similarity} that provides a simplified API for its
 * descendants. Subclasses are only required to implement the {@link #score}
 * and {@link #toString()} methods. Implementing
 * {@link #explain(List, BasicStats, double, double)} is optional,
 * inasmuch as SimilarityBase already provides a basic explanation of the score
 * and the term frequency. However, implementers of a subclass are encouraged to
 * include as much detail about the scoring method as possible.
 * <p>
 * Note: multi-word queries such as phrase queries are scored in a different way
 * than Lucene's default ranking algorithm: whereas it "fakes" an IDF value for
 * the phrase as a whole (since it does not know it), this class instead scores
 * phrases as a summation of the individual term scores.
 * @lucene.experimental
 */
public abstract class SimilarityBase extends Similarity {
  /** For {@link #log2(double)}. Precomputed for efficiency reasons. */
  private static final double LOG_2 = Math.log(2);
  
  /** 
   * True if overlap tokens (tokens with a position of increment of zero) are
   * discounted from the document's length.
   */
  protected boolean discountOverlaps = true;
  
  /**
   * Sole constructor. (For invocation by subclass 
   * constructors, typically implicit.)
   */
  public SimilarityBase() {}
  
  /** Determines whether overlap tokens (Tokens with
   *  0 position increment) are ignored when computing
   *  norm.  By default this is true, meaning overlap
   *  tokens do not count when computing norms.
   *
   *  @lucene.experimental
   *
   *  @see #computeNorm
   */
  public void setDiscountOverlaps(boolean v) {
    discountOverlaps = v;
  }

  /**
   * Returns true if overlap tokens are discounted from the document's length. 
   * @see #setDiscountOverlaps 
   */
  public boolean getDiscountOverlaps() {
    return discountOverlaps;
  }
  
  @Override
  public final SimScorer scorer(float boost, CollectionStatistics collectionStats, TermStatistics... termStats) {
    SimScorer weights[] = new SimScorer[termStats.length];
    for (int i = 0; i < termStats.length; i++) {
      BasicStats stats = newStats(collectionStats.field(), boost);
      fillBasicStats(stats, collectionStats, termStats[i]);
      weights[i] = new BasicSimScorer(stats);
    }
    if (weights.length == 1) {
      return weights[0];
    } else {
      return new MultiSimilarity.MultiSimScorer(weights);
    }
  }
  
  /** Factory method to return a custom stats object */
  protected BasicStats newStats(String field, double boost) {
    return new BasicStats(field, boost);
  }
  
  /** Fills all member fields defined in {@code BasicStats} in {@code stats}. 
   *  Subclasses can override this method to fill additional stats. */
  protected void fillBasicStats(BasicStats stats, CollectionStatistics collectionStats, TermStatistics termStats) {
    // TODO: validate this for real, somewhere else
    assert termStats.totalTermFreq() <= collectionStats.sumTotalTermFreq();
    assert termStats.docFreq() <= collectionStats.sumDocFreq();
 
    // TODO: add sumDocFreq for field (numberOfFieldPostings)
    stats.setNumberOfDocuments(collectionStats.docCount());
    stats.setNumberOfFieldTokens(collectionStats.sumTotalTermFreq());
    stats.setAvgFieldLength(collectionStats.sumTotalTermFreq() / (double) collectionStats.docCount());
    stats.setDocFreq(termStats.docFreq());
    stats.setTotalTermFreq(termStats.totalTermFreq());
  }
  
  /**
   * Scores the document {@code doc}.
   * <p>Subclasses must apply their scoring formula in this class.</p>
   * @param stats the corpus level statistics.
   * @param freq the term frequency.
   * @param docLen the document length.
   * @return the score.
   */
  protected abstract double score(BasicStats stats, double freq, double docLen);

  /**
   * Subclasses should implement this method to explain the score. {@code expl}
   * already contains the score, the name of the class and the doc id, as well
   * as the term frequency and its explanation; subclasses can add additional
   * clauses to explain details of their scoring formulae.
   * <p>The default implementation does nothing.</p>
   * 
   * @param subExpls the list of details of the explanation to extend
   * @param stats the corpus level statistics.
   * @param freq the term frequency.
   * @param docLen the document length.
   */
  protected void explain(
      List<Explanation> subExpls, BasicStats stats, double freq, double docLen) {}
  
  /**
   * Explains the score. The implementation here provides a basic explanation
   * in the format <em>score(name-of-similarity, doc=doc-id,
   * freq=term-frequency), computed from:</em>, and
   * attaches the score (computed via the {@link #score(BasicStats, double, double)}
   * method) and the explanation for the term frequency. Subclasses content with
   * this format may add additional details in
   * {@link #explain(List, BasicStats, double, double)}.
   *  
   * @param stats the corpus level statistics.
   * @param freq the term frequency and its explanation.
   * @param docLen the document length.
   * @return the explanation.
   */
  protected Explanation explain(
      BasicStats stats, Explanation freq, double docLen) {
    List<Explanation> subs = new ArrayList<>();
    explain(subs, stats, freq.getValue().floatValue(), docLen);
    
    return Explanation.match(
        (float) score(stats, freq.getValue().floatValue(), docLen),
        "score(" + getClass().getSimpleName() + ", freq=" + freq.getValue() +"), computed from:",
        subs);
  }
  
  /**
   * Subclasses must override this method to return the name of the Similarity
   * and preferably the values of parameters (if any) as well.
   */
  @Override
  public abstract String toString();

  // ------------------------------ Norm handling ------------------------------
  
  /** Cache of decoded bytes. */
  private static final float[] LENGTH_TABLE = new float[256];

  static {
    for (int i = 0; i < 256; i++) {
      LENGTH_TABLE[i] = SmallFloat.byte4ToInt((byte) i);
    }
  }

  /** Encodes the document length in the same way as {@link BM25Similarity}. */
  @Override
  public final long computeNorm(FieldInvertState state) {
    final int numTerms;
    if (state.getIndexOptions() == IndexOptions.DOCS && state.getIndexCreatedVersionMajor() >= 8) {
      numTerms = state.getUniqueTermCount();
    } else if (discountOverlaps) {
      numTerms = state.getLength() - state.getNumOverlap();
    } else {
      numTerms = state.getLength();
    }
    return SmallFloat.intToByte4(numTerms);
  }

  // ----------------------------- Static methods ------------------------------
  
  /** Returns the base two logarithm of {@code x}. */
  public static double log2(double x) {
    // Put this to a 'util' class if we need more of these.
    return Math.log(x) / LOG_2;
  }
  
  // --------------------------------- Classes ---------------------------------
  
  /** Delegates the {@link #score(float, long)} and
   * {@link #explain(Explanation, long)} methods to
   * {@link SimilarityBase#score(BasicStats, double, double)} and
   * {@link SimilarityBase#explain(BasicStats, Explanation, double)},
   * respectively.
   */
  final class BasicSimScorer extends SimScorer {
    final BasicStats stats;
    
    BasicSimScorer(BasicStats stats) {
      this.stats = stats;
    }

    double getLengthValue(long norm) {
      return LENGTH_TABLE[Byte.toUnsignedInt((byte) norm)];
    }
    
    @Override
    public float score(float freq, long norm) {
      return (float) SimilarityBase.this.score(stats, freq, getLengthValue(norm));
    }

    @Override
    public Explanation explain(Explanation freq, long norm) {
      return SimilarityBase.this.explain(stats, freq, getLengthValue(norm));
    }

  }
}
