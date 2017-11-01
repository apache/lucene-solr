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


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SmallFloat;

/**
 * A subclass of {@code Similarity} that provides a simplified API for its
 * descendants. Subclasses are only required to implement the {@link #score}
 * and {@link #toString()} methods. Implementing
 * {@link #explain(List, BasicStats, int, float, float)} is optional,
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
  public final SimWeight computeWeight(float boost, CollectionStatistics collectionStats, TermStatistics... termStats) {
    BasicStats stats[] = new BasicStats[termStats.length];
    for (int i = 0; i < termStats.length; i++) {
      stats[i] = newStats(collectionStats.field(), boost);
      fillBasicStats(stats[i], collectionStats, termStats[i]);
    }
    return stats.length == 1 ? stats[0] : new MultiSimilarity.MultiStats(stats);
  }
  
  /** Factory method to return a custom stats object */
  protected BasicStats newStats(String field, float boost) {
    return new BasicStats(field, boost);
  }
  
  /** Fills all member fields defined in {@code BasicStats} in {@code stats}. 
   *  Subclasses can override this method to fill additional stats. */
  protected void fillBasicStats(BasicStats stats, CollectionStatistics collectionStats, TermStatistics termStats) {
    // #positions(field) must be >= #positions(term)
    assert collectionStats.sumTotalTermFreq() == -1 || collectionStats.sumTotalTermFreq() >= termStats.totalTermFreq();
    long numberOfDocuments = collectionStats.docCount() == -1 ? collectionStats.maxDoc() : collectionStats.docCount();
    
    long docFreq = termStats.docFreq();
    long totalTermFreq = termStats.totalTermFreq();

    // frequencies are omitted, all postings have tf=1, so totalTermFreq = docFreq
    if (totalTermFreq == -1) {
      totalTermFreq = docFreq;
    }

    final long numberOfFieldTokens;
    final float avgFieldLength;

    if (collectionStats.sumTotalTermFreq() == -1) {
      // frequencies are omitted, so sumTotalTermFreq = # postings
      if (collectionStats.sumDocFreq() == -1) {
        // theoretical case only: remove!
        numberOfFieldTokens = docFreq;
        avgFieldLength = 1f;
      } else {
        numberOfFieldTokens = collectionStats.sumDocFreq();
        avgFieldLength = (float) (collectionStats.sumDocFreq() / (double)numberOfDocuments);
      }
    } else {
      numberOfFieldTokens = collectionStats.sumTotalTermFreq();
      avgFieldLength = (float) (collectionStats.sumTotalTermFreq() / (double)numberOfDocuments);
    }
 
    // TODO: add sumDocFreq for field (numberOfFieldPostings)
    stats.setNumberOfDocuments(numberOfDocuments);
    stats.setNumberOfFieldTokens(numberOfFieldTokens);
    stats.setAvgFieldLength(avgFieldLength);
    stats.setDocFreq(docFreq);
    stats.setTotalTermFreq(totalTermFreq);
  }
  
  /**
   * Scores the document {@code doc}.
   * <p>Subclasses must apply their scoring formula in this class.</p>
   * @param stats the corpus level statistics.
   * @param freq the term frequency.
   * @param docLen the document length.
   * @return the score.
   */
  protected abstract float score(BasicStats stats, float freq, float docLen);
  
  /**
   * Subclasses should implement this method to explain the score. {@code expl}
   * already contains the score, the name of the class and the doc id, as well
   * as the term frequency and its explanation; subclasses can add additional
   * clauses to explain details of their scoring formulae.
   * <p>The default implementation does nothing.</p>
   * 
   * @param subExpls the list of details of the explanation to extend
   * @param stats the corpus level statistics.
   * @param doc the document id.
   * @param freq the term frequency.
   * @param docLen the document length.
   */
  protected void explain(
      List<Explanation> subExpls, BasicStats stats, int doc, float freq, float docLen) {}
  
  /**
   * Explains the score. The implementation here provides a basic explanation
   * in the format <em>score(name-of-similarity, doc=doc-id,
   * freq=term-frequency), computed from:</em>, and
   * attaches the score (computed via the {@link #score(BasicStats, float, float)}
   * method) and the explanation for the term frequency. Subclasses content with
   * this format may add additional details in
   * {@link #explain(List, BasicStats, int, float, float)}.
   *  
   * @param stats the corpus level statistics.
   * @param doc the document id.
   * @param freq the term frequency and its explanation.
   * @param docLen the document length.
   * @return the explanation.
   */
  protected Explanation explain(
      BasicStats stats, int doc, Explanation freq, float docLen) {
    List<Explanation> subs = new ArrayList<>();
    explain(subs, stats, doc, freq.getValue(), docLen);
    
    return Explanation.match(
        score(stats, freq.getValue(), docLen),
        "score(" + getClass().getSimpleName() + ", doc=" + doc + ", freq=" + freq.getValue() +"), computed from:",
        subs);
  }
  
  @Override
  public final SimScorer simScorer(SimWeight stats, LeafReaderContext context) throws IOException {
    int indexCreatedVersionMajor = context.reader().getMetaData().getCreatedVersionMajor();
    if (stats instanceof MultiSimilarity.MultiStats) {
      // a multi term query (e.g. phrase). return the summation, 
      // scoring almost as if it were boolean query
      SimWeight subStats[] = ((MultiSimilarity.MultiStats) stats).subStats;
      SimScorer subScorers[] = new SimScorer[subStats.length];
      for (int i = 0; i < subScorers.length; i++) {
        BasicStats basicstats = (BasicStats) subStats[i];
        subScorers[i] = new BasicSimScorer(basicstats, indexCreatedVersionMajor, context.reader().getNormValues(basicstats.field));
      }
      return new MultiSimilarity.MultiSimScorer(subScorers);
    } else {
      BasicStats basicstats = (BasicStats) stats;
      return new BasicSimScorer(basicstats, indexCreatedVersionMajor, context.reader().getNormValues(basicstats.field));
    }
  }
  
  /**
   * Subclasses must override this method to return the name of the Similarity
   * and preferably the values of parameters (if any) as well.
   */
  @Override
  public abstract String toString();

  // ------------------------------ Norm handling ------------------------------
  
  /** Cache of decoded bytes. */
  private static final float[] OLD_LENGTH_TABLE = new float[256];
  private static final float[] LENGTH_TABLE = new float[256];

  static {
    for (int i = 1; i < 256; i++) {
      float f = SmallFloat.byte315ToFloat((byte)i);
      OLD_LENGTH_TABLE[i] = 1.0f / (f*f);
    }
    OLD_LENGTH_TABLE[0] = 1.0f / OLD_LENGTH_TABLE[255]; // otherwise inf

    for (int i = 0; i < 256; i++) {
      LENGTH_TABLE[i] = SmallFloat.byte4ToInt((byte) i);
    }
  }

  /** Encodes the document length in the same way as {@link BM25Similarity}. */
  @Override
  public final long computeNorm(FieldInvertState state) {
    final int numTerms;
    if (discountOverlaps)
      numTerms = state.getLength() - state.getNumOverlap();
    else
      numTerms = state.getLength();
    int indexCreatedVersionMajor = state.getIndexCreatedVersionMajor();
    if (indexCreatedVersionMajor >= 7) {
      return SmallFloat.intToByte4(numTerms);
    } else {
      return SmallFloat.floatToByte315((float) (1 / Math.sqrt(numTerms)));
    }
  }

  // ----------------------------- Static methods ------------------------------
  
  /** Returns the base two logarithm of {@code x}. */
  public static double log2(double x) {
    // Put this to a 'util' class if we need more of these.
    return Math.log(x) / LOG_2;
  }
  
  // --------------------------------- Classes ---------------------------------
  
  /** Delegates the {@link #score(int, float)} and
   * {@link #explain(int, Explanation)} methods to
   * {@link SimilarityBase#score(BasicStats, float, float)} and
   * {@link SimilarityBase#explain(BasicStats, int, Explanation, float)},
   * respectively.
   */
  final class BasicSimScorer extends SimScorer {
    private final BasicStats stats;
    private final NumericDocValues norms;
    private final float[] normCache;
    
    BasicSimScorer(BasicStats stats, int indexCreatedVersionMajor, NumericDocValues norms) throws IOException {
      this.stats = stats;
      this.norms = norms;
      this.normCache = indexCreatedVersionMajor >= 7 ? LENGTH_TABLE : OLD_LENGTH_TABLE;
    }

    float getLengthValue(int doc) throws IOException {
      if (norms == null) {
        return 1F;
      }
      if (norms.advanceExact(doc)) {
        return normCache[Byte.toUnsignedInt((byte) norms.longValue())];
      } else {
        return 0;
      }
    }
    
    @Override
    public float score(int doc, float freq) throws IOException {
      // We have to supply something in case norms are omitted
      return SimilarityBase.this.score(stats, freq, getLengthValue(doc));
    }

    @Override
    public Explanation explain(int doc, Explanation freq) throws IOException {
      return SimilarityBase.this.explain(stats, doc, freq, getLengthValue(doc));
    }

    @Override
    public float computeSlopFactor(int distance) {
      return 1.0f / (distance + 1);
    }

    @Override
    public float computePayloadFactor(int doc, int start, int end, BytesRef payload) {
      return 1f;
    }
  }
}
