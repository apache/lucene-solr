package org.apache.lucene.search.similarities;

import org.apache.lucene.search.Explanation;
import static org.apache.lucene.search.similarities.EasySimilarity.log2;

/**
 * The basic tf-idf model of randomness.
 * @lucene.experimental
 */ 
public class BasicModelIn extends BasicModel {
  @Override
  public final float score(EasyStats stats, float tfn) {
    int N = stats.getNumberOfDocuments();
    int n = stats.getDocFreq();
    return tfn * (float)(log2((N + 1) / (n + 0.5)));
  }
  
  @Override
  public final Explanation explain(EasyStats stats, float tfn) {
    Explanation result = new Explanation();
    result.setDescription(getClass().getSimpleName() + ", computed from: ");
    result.setValue(score(stats, tfn));
    result.addDetail(new Explanation(tfn, "tfn"));
    result.addDetail(
        new Explanation(stats.getNumberOfDocuments(), "numberOfDocuments"));
    result.addDetail(
        new Explanation(stats.getDocFreq(), "docFreq"));
    return result;
  }
}
