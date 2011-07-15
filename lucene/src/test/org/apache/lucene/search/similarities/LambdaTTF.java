package org.apache.lucene.search.similarities;

import org.apache.lucene.search.Explanation;

/**
 * Computes lambda as {@code docFreq / numberOfDocuments}.
 * @lucene.experimental
 */
public class LambdaTTF extends Lambda {
  @Override
  public final float lambda(EasyStats stats) {
    return (float)stats.getDocFreq() / stats.getNumberOfDocuments();
  }
  
  @Override
  public final Explanation explain(EasyStats stats) {
    Explanation result = new Explanation();
    result.setDescription(getClass().getSimpleName() + ", computed from: ");
    result.setValue(lambda(stats));
    result.addDetail(
        new Explanation(stats.getDocFreq(), "docFreq"));
    result.addDetail(
        new Explanation(stats.getNumberOfDocuments(), "numberOfDocuments"));
    return result;
  }
}
