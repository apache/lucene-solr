package org.apache.lucene.search.similarities;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.similarities.LMSimilarity.LMStats;

/**
 * Language model based on the Jelinek-Mercer smoothing method. From Chengxiang
 * Zhai and John Lafferty. 2001. A study of smoothing methods for language
 * models applied to Ad Hoc information retrieval. In Proceedings of the 24th
 * annual international ACM SIGIR conference on Research and development in
 * information retrieval (SIGIR '01). ACM, New York, NY, USA, 334-342.
 *
 * @lucene.experimental
 */
public class LMJelinekMercerSimilarity extends LMSimilarity {
  /** The &lambda; parameter. */
  private final float lambda;
  
  /** @param lambda the &lambda; parameter. */
  public LMJelinekMercerSimilarity(
      CollectionModel collectionModel, float lambda) {
    super(collectionModel);
    this.lambda = lambda;
  }

  /** @param lambda the &lambda; parameter. */
  public LMJelinekMercerSimilarity(float lambda) {
    this.lambda = lambda;
  }
  
  @Override
  protected float score(EasyStats stats, float freq, byte norm) {
    return stats.getTotalBoost() *
        (float)Math.log(1 +
            ((1 - lambda) * freq / decodeNormValue(norm)) /
            (lambda * ((LMStats)stats).getCollectionProbability()));
  }
  
  @Override
  protected void explain(Explanation expl, EasyStats stats, int doc,
      float freq, byte norm) {
    if (stats.getTotalBoost() != 1.0f) {
      expl.addDetail(new Explanation(stats.getTotalBoost(), "boost"));
    }
    super.explain(expl, stats, doc, freq, norm);
  }

  /** Returns the &lambda; parameter. */
  public float getLambda() {
    return lambda;
  }
}
