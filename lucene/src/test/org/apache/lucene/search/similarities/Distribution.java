package org.apache.lucene.search.similarities;

import org.apache.lucene.search.Explanation;

/**
 * The probabilistic distribution used to model term occurrence
 * in information-based models.
 * @see IBSimilarity
 * @lucene.experimental
 */
public abstract class Distribution {
  /** Computes the score. */
  public abstract float score(EasyStats stats, float tfn, float lambda);
  
  /** Explains the score. Returns the name of the model only, since
   * both {@code tfn} and {@code lambda} are explained elsewhere. */
  public Explanation explain(EasyStats stats, float tfn, float lambda) {
    return new Explanation(
        score(stats, tfn, lambda), getClass().getSimpleName());
  }
}
