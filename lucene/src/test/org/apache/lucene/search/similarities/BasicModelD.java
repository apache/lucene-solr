package org.apache.lucene.search.similarities;

import static org.apache.lucene.search.similarities.EasySimilarity.log2;

/**
 * Implements the approximation of the binomial model with the divergence
 * for DFR.
 * @lucene.experimental
 */
public class BasicModelD extends BasicModel {
  @Override
  public final float score(EasyStats stats, float tfn) {
    long F = stats.getTotalTermFreq();
    double phi = (double)tfn / F;
    double nphi = 1 - phi;
    double p = 1.0 / stats.getNumberOfDocuments();
    double D = phi * log2(phi / p) + nphi * log2(nphi / (1 - p));
    return (float)(D * F + 0.5 * log2(2 * Math.PI * tfn * nphi));
  }
}
