package org.apache.lucene.search.similarities;

import org.apache.lucene.search.Explanation;

/**
 * Model of the information gain based on Laplace's law of succession.
 * @lucene.experimental
 */
public class AfterEffectL extends AfterEffect {
  @Override
  public final float score(EasyStats stats, float tfn) {
    return 1 / (tfn + 1);
  }
  
  @Override
  public final Explanation explain(EasyStats stats, float tfn) {
    Explanation result = new Explanation();
    result.setDescription(getClass().getSimpleName() + ", computed from: ");
    result.setValue(score(stats, tfn));
    result.addDetail(new Explanation(tfn, "tfn"));
    return result;
  }
}
