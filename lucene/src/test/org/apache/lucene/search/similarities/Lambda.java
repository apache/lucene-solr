package org.apache.lucene.search.similarities;

import org.apache.lucene.search.Explanation;

/**
 * The <em>lambda (&lambda;<sub>w</sub>)</em> parameter in information-based
 * models.
 * @see IBSimilarity
 * @lucene.experimental
 */
public abstract class Lambda {
  /** Computes the lambda parameter. */
  public abstract float lambda(EasyStats stats);
  /** Explains the lambda parameter. */
  public abstract Explanation explain(EasyStats stats);
}
