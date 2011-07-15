package org.apache.lucene.search.similarities;

import static org.apache.lucene.search.similarities.EasySimilarity.log2;

/**
 * Tf-idf model of randomness, based on a mixture of Poisson and inverse
 * document frequency.
 * @lucene.experimental
 */ 
public class BasicModelIne extends BasicModel {
  @Override
  public final float score(EasyStats stats, float tfn) {
    int N = stats.getNumberOfDocuments();
    long F = stats.getTotalTermFreq();
    double ne = N * (1 - Math.pow((N - 1) / N, F));
    return tfn * (float)(log2((N + 1) / (ne + 0.5)));
  }
}
