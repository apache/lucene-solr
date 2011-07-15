package org.apache.lucene.search.similarities;

import static org.apache.lucene.search.similarities.EasySimilarity.log2;

/**
 * An approximation of the <em>I(n<sub>e</sub>)</em> model.
 * @lucene.experimental
 */ 
public class BasicModelIF extends BasicModel {
  @Override
  public final float score(EasyStats stats, float tfn) {
    // TODO: Refactor this method to a parent class? See the other two Ix model. 
    int N = stats.getNumberOfDocuments();
    long F = stats.getTotalTermFreq();
    return tfn * (float)(log2((N + 1) / (F + 0.5)));
  }
}
