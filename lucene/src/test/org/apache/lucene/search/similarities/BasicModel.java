package org.apache.lucene.search.similarities;

import org.apache.lucene.search.Explanation;

/**
 * This class acts as the base class for the specific <em>basic model</em>
 * implementations in the DFR framework. Basic models compute the
 * <em>informative content Inf<sub>1</sub> = -log<sub>2</sub>Prob<sub>1</sub>
 * </em>.
 * 
 * @see DFRSimilarity
 * @lucene.experimental
 */
public abstract class BasicModel {
  /** Returns the informative content score. */
  public abstract float score(EasyStats stats, float tfn);
  
  /**
   * Returns an explanation for the score.
   * <p>Most basic models use the number of documents and the total term
   * frequency to compute Inf<sub>1</sub>. This method provides a generic
   * explanation for such models. Subclasses that use other statistics must
   * override this method.</p>
   */
  public Explanation explain(EasyStats stats, float tfn) {
    Explanation result = new Explanation();
    result.setDescription(getClass().getSimpleName() + ", computed from: ");
    result.setValue(score(stats, tfn));
    result.addDetail(new Explanation(tfn, "tfn"));
    result.addDetail(
        new Explanation(stats.getNumberOfDocuments(), "numberOfDocuments"));
    result.addDetail(
        new Explanation(stats.getTotalTermFreq(), "totalTermFreq"));
    return result;
  }
}
