package org.apache.lucene.search.similarities;

import org.apache.lucene.search.Explanation;

/**
 * Model of the information gain based on the ration of two Bernoulli processes.
 * @lucene.experimental
 */
public class AfterEffectB extends AfterEffect {
  @Override
  public final float score(EasyStats stats, float tfn) {
    long F = stats.getTotalTermFreq();
    int n = stats.getDocFreq();
    return (F + 1) / (n * (tfn + 1));
  }
  
  @Override
  public final Explanation explain(EasyStats stats, float tfn) {
    Explanation result = new Explanation();
    result.setDescription(getClass().getSimpleName() + ", computed from: ");
    result.setValue(score(stats, tfn));
    result.addDetail(new Explanation(tfn, "tfn"));
    result.addDetail(new Explanation(stats.getTotalTermFreq(), "totalTermFreq"));
    result.addDetail(new Explanation(stats.getDocFreq(), "docFreq"));
    return result;
  }
}
