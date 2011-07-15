package org.apache.lucene.search.similarities;

/**
 * Normalization model that assumes a uniform distribution of the term frequency.
 */
public class NormalizationH1 extends Normalization {
  @Override
  public final float tfn(EasyStats stats, float tf, int len) {
    return tf * stats.getAvgFieldLength() / len;
  }
}
