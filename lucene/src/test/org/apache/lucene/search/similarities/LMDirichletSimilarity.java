package org.apache.lucene.search.similarities;

import org.apache.lucene.search.Explanation;

/**
 * Bayesian smoothing using Dirichlet priors. From Chengxiang Zhai and John
 * Lafferty. 2001. A study of smoothing methods for language models applied to
 * Ad Hoc information retrieval. In Proceedings of the 24th annual international
 * ACM SIGIR conference on Research and development in information retrieval
 * (SIGIR '01). ACM, New York, NY, USA, 334-342.
 * 
 * @lucene.experimental
 */
public class LMDirichletSimilarity extends LMSimilarity {
  /** The &mu; parameter. */
  private final float mu;
  
  /** @param mu the &mu; parameter. */
  public LMDirichletSimilarity(CollectionModel collectionModel, float mu) {
    super(collectionModel);
    this.mu = mu;
  }
  
  /** @param mu the &mu; parameter. */
  public LMDirichletSimilarity(float mu) {
    this.mu = mu;
  }

  /** Instantiates the similarity with the default &mu; value of 2000. */
  public LMDirichletSimilarity(CollectionModel collectionModel) {
    this(collectionModel, 2000);
  }
  
  /** Instantiates the similarity with the default &mu; value of 2000. */
  public LMDirichletSimilarity() {
    this(2000);
  }
  
  @Override
  protected float score(EasyStats stats, float freq, byte norm) {
    return stats.getTotalBoost() *
        (float)(Math.log(1 + freq /
            (mu * ((LMStats)stats).getCollectionProbability())) +
        Math.log(mu / (decodeNormValue(norm) + mu)));
  }
  
  @Override
  protected void explain(Explanation expl, EasyStats stats, int doc,
      float freq, byte norm) {
    if (stats.getTotalBoost() != 1.0f) {
      expl.addDetail(new Explanation(stats.getTotalBoost(), "boost"));
    }
    
    // nocommit: mu?
    Explanation weightExpl = new Explanation();
    weightExpl.setValue((float)Math.log(1 + freq /
        (mu * ((LMStats)stats).getCollectionProbability())));
    weightExpl.setDescription("term weight");
    expl.addDetail(weightExpl);
    expl.addDetail(new Explanation(
        (float)Math.log(mu / (decodeNormValue(norm) + mu)), "document norm"));
    super.explain(expl, stats, doc, freq, norm);
  }

  /** Returns the &mu; parameter. */
  public float getMu() {
    return mu;
  }
}
