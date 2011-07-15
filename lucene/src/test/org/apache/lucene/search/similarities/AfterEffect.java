package org.apache.lucene.search.similarities;

// nocommit: license header (for all these files)

import org.apache.lucene.search.Explanation;

/**
 * This class acts as the base class for the implementations of the <em>first
 * normalization of the informative content</em> in the DFR framework. This
 * component is also called the <em>after effect</em> and is defined by the
 * formula <em>Inf<sub>2</sub> = 1 - Prob<sub>2</sub></em>, where
 * <em>Prob<sub>2</sub></em> measures the <em>information gain</em>.
 * 
 * @see DFRSimilarity
 * @lucene.experimental
 */
public abstract class AfterEffect {
  /** Returns the aftereffect score. */
  public abstract float score(EasyStats stats, float tfn);
  
  /** Returns an explanation for the score. */
  public abstract Explanation explain(EasyStats stats, float tfn);

  /** Implementation used when there is no aftereffect. */
  public static final class NoAfterEffect extends AfterEffect {
    @Override
    public final float score(EasyStats stats, float tfn) {
      return 1f;
    }

    @Override
    public final Explanation explain(EasyStats stats, float tfn) {
      return new Explanation(1, "no aftereffect");
    }
  }
}
