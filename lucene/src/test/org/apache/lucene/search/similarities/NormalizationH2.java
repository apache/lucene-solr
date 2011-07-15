package org.apache.lucene.search.similarities;

import static org.apache.lucene.search.similarities.EasySimilarity.log2;

/**
 * Normalization model in which the term frequency is inversely related to the
 * length.
 */
public class NormalizationH2 extends Normalization {
  @Override
  public final float tfn(EasyStats stats, float tf, int len) {
    return (float)(tf * log2(1 + stats.getAvgFieldLength() / len));
  }
}