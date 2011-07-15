package org.apache.lucene.search.similarities;

/**
 * The smoothed power-law (SPL) distribution for the information-based framework
 * that is described in the original paper.
 * <p>Unlike for DFR, the natural logarithm is used, as
 * it is faster to compute and the original paper does not express any
 * preference to a specific base.</p>
 * @lucene.experimental
 */
public abstract class DistributionSPL extends Distribution {
  @Override
  public final float score(EasyStats stats, float tfn, float lambda) {
    return (float)-Math.log(
        (Math.pow(lambda, (tfn / (tfn + 1))) - lambda) / (1 - lambda));
  }
}
