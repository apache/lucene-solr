package org.apache.lucene.search.similarities;

import static org.apache.lucene.search.similarities.EasySimilarity.log2;

/**
 * Limiting form of the Bose-Einstein model.
 * @lucene.experimental
 */
public class BasicModelBE extends BasicModel {
  @Override
  public final float score(EasyStats stats, float tfn) {
    long N = stats.getNumberOfDocuments();
    long F = stats.getTotalTermFreq();
    return (float)(-log2((N - 1) * Math.E)
        + f(N + F -1, N + F - tfn - 2) - f(F, F - tfn));
  }
  
  /** The <em>f</em> helper function defined for <em>B<sub>E</sub></em>. */
  private final double f(long n, float m) {
    return (m + 0.5) * log2((double)n / m) + (n - m) * log2(n);
  }
}
