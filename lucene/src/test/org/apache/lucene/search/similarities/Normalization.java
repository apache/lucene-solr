package org.apache.lucene.search.similarities;

import org.apache.lucene.search.Explanation;

/**
 * This class acts as the base class for the implementations of the term
 * frequency normalization methods in the DFR framework.
 * 
 * @see DFRSimilarity
 * @lucene.experimental
 */
public abstract class Normalization {
  /** Returns the normalized term frequency.
   * @param len the field length. */
  public abstract float tfn(EasyStats stats, float tf, int len);
  
  /** Returns an explanation for the normalized term frequency.
   * <p>The default normalization methods use the field length of the document
   * and the average field length to compute the normalized term frequency.
   * This method provides a generic explanation for such methods.
   * Subclasses that use other statistics must override this method.</p>
   */
  public Explanation explain(EasyStats stats, float tf, int len) {
    Explanation result = new Explanation();
    result.setDescription(getClass().getSimpleName() + ", computed from: ");
    result.setValue(tfn(stats, tf, len));
    result.addDetail(new Explanation(tf, "tf"));
    result.addDetail(
        new Explanation(stats.getAvgFieldLength(), "avgFieldLength"));
    result.addDetail(new Explanation(len, "len"));
    return result;
  }

  /** Implementation used when there is no normalization. */
  public static final class NoNormalization extends Normalization {
    @Override
    public final float tfn(EasyStats stats, float tf, int len) {
      return tf;
    }

    @Override
    public final Explanation explain(EasyStats stats, float tf, int len) {
      return new Explanation(1, "no normalization");
    }
  }
}
