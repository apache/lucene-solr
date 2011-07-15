package org.apache.lucene.search.similarities;

import static org.apache.lucene.search.similarities.EasySimilarity.log2;

/**
 * Geometric as limiting form of the Bose-Einstein model.
 * @lucene.experimental
 */
public class BasicModelG extends BasicModel {
  @Override
  public final float score(EasyStats stats, float tfn) {
    double lambda = stats.getTotalTermFreq() / stats.getNumberOfDocuments();
    // -log(1 / (lambda + 1)) -> log(lambda + 1)
    return (float)(log2(lambda + 1) + tfn * log2((1 + lambda) / lambda));
  }
}
