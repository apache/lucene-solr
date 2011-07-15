package org.apache.lucene.search.similarities;

/**
 * Log-logistic distribution.
 * <p>Unlike for DFR, the natural logarithm is used, as
 * it is faster to compute and the original paper does not express any
 * preference to a specific base.</p>
 * @lucene.experimental
 */
public abstract class DistributionLL extends Distribution {
  @Override
  public final float score(EasyStats stats, float tfn, float lambda) {
    return (float)-Math.log(lambda / (tfn + lambda));
  }
}
