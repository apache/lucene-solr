package org.apache.lucene.search.similarities;

import org.apache.lucene.search.Explanation;

/**
 * Provides a framework for the family of information-based models, as described
 * in St&eacute;phane Clinchant and Eric Gaussier. 2010. Information-based
 * models for ad hoc IR. In Proceeding of the 33rd international ACM SIGIR
 * conference on Research and development in information retrieval (SIGIR '10).
 * ACM, New York, NY, USA, 234-241.
 * <p>The retrieval function is of the form <em>RSV(q, d) = &sum;
 * -x<sup>q</sup><sub>w</sub> log Prob(X<sub>w</sub> &ge;
 * t<sup>d</sup><sub>w</sub> | &lambda;<sub>w</sub>)</em>, where
 * <ul>
 *   <li><em>x<sup>q</sup><sub>w</sub></em> is the query boost;</li>
 *   <li><em>X<sub>w</sub></em> is a random variable that counts the occurrences
 *   of word <em>w</em>;</li>
 *   <li><em>t<sup>d</sup><sub>w</sub></em> is the normalized term frequency;</li>
 *   <li><em>&lambda;<sub>w</sub></em> is a parameter.</li>
 * </ul>
 * </p>
 * <p>The framework described in the paper has many similarities to the DFR
 * framework (see {@link DFRSimilarity}). It is possible that the two
 * Similarities will be merged at one point.</p>
 * @lucene.experimental 
 */
public class IBSimilarity extends EasySimilarity {
  /** The probabilistic distribution used to model term occurrence. */
  protected final Distribution distribution;
  /** The <em>lambda (&lambda;<sub>w</sub>)</em> parameter. */
  protected final Lambda lambda;
  /** The term frequency normalization. */
  protected final Normalization normalization;
  
  public IBSimilarity(Class<Distribution> distributionClass,
                      Class<Lambda> lambdaClass,
                      Class<Normalization> normalizationClass)
  throws InstantiationException, IllegalAccessException {
    distribution = distributionClass.newInstance();
    lambda = lambdaClass.newInstance();
    normalization = (normalizationClass != null)
                  ? normalizationClass.newInstance()
                  : new Normalization.NoNormalization();
  }
  
  @Override
  protected float score(EasyStats stats, float freq, byte norm) {
    return stats.getTotalBoost() *
        distribution.score(
            stats,
            normalization.tfn(stats, freq, decodeNormValue(norm)),
            lambda.lambda(stats));
  }

  @Override
  protected void explain(
      Explanation expl, EasyStats stats, int doc, float freq, byte norm) {
    if (stats.getTotalBoost() != 1.0f) {
      expl.addDetail(new Explanation(stats.getTotalBoost(), "boost"));
    }
    int len = decodeNormValue(norm);
    Explanation normExpl = normalization.explain(stats, freq, len);
    Explanation lambdaExpl = lambda.explain(stats);
    expl.addDetail(normExpl);
    expl.addDetail(lambdaExpl);
    expl.addDetail(distribution.explain(
        stats, normExpl.getValue(), lambdaExpl.getValue()));
  }
}
