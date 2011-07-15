package org.apache.lucene.search.similarities;

import org.apache.lucene.search.Explanation;

/**
 * Computes lambda as {@code totalTermFreq / numberOfDocuments}.
 * @lucene.experimental
 */
public class LambdaDF extends Lambda {
  @Override
  public final float lambda(EasyStats stats) {
    return (float)stats.getTotalTermFreq() / stats.getNumberOfDocuments();
  }

  @Override
  public final Explanation explain(EasyStats stats) {
    Explanation result = new Explanation();
    result.setDescription(getClass().getSimpleName() + ", computed from: ");
    result.setValue(lambda(stats));
    result.addDetail(
        new Explanation(stats.getTotalTermFreq(), "totalTermFreq"));
    result.addDetail(
        new Explanation(stats.getNumberOfDocuments(), "numberOfDocuments"));
    return result;
  }
}
