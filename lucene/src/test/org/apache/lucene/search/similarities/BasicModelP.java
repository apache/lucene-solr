package org.apache.lucene.search.similarities;

import static org.apache.lucene.search.similarities.EasySimilarity.log2;

/**
 * Implements the Poisson approximation for the binomial model for DFR.
 * @lucene.experimental
 */
public class BasicModelP extends BasicModel {
  @Override
  public final float score(EasyStats stats, float tfn) {
    float lambda = (float)stats.getTotalTermFreq() / stats.getNumberOfDocuments();
    return (float)(tfn * log2(tfn / lambda)
        + (lambda + 1 / 12 / tfn - tfn) * log2(Math.E)
        + 0.5 * log2(2 * Math.PI * tfn));
  }
}
