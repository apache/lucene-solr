package org.apache.lucene.search.similarities;

import org.apache.lucene.search.Explanation;

/**
 * Implements the <em>divergence from randomness (DFR)</em> framework
 * introduced in Gianni Amati and Cornelis Joost Van Rijsbergen. 2002.
 * Probabilistic models of information retrieval based on measuring the
 * divergence from randomness. ACM Trans. Inf. Syst. 20, 4 (October 2002),
 * 357-389.
 * <p>The DFR scoring formula is composed of three separate components: the
 * <em>basic model</em>, the <em>aftereffect</em> and an additional
 * <em>normalization</em> component, represented by the classes
 * {@code BasicModel}, {@code AfterEffect} and {@code Normalization},
 * respectively. The names of these classes were chosen to match the names of
 * their counterparts in the Terrier IR engine.</p>
 * <p>Note that <em>qtf</em>, the multiplicity of term-occurrence in the query,
 * is not handled by this implementation.</p>
 * 
 * @see BasicModel
 * @see AfterEffect
 * @see Normalization
 * @lucene.experimental
 */
public class DFRSimilarity extends EasySimilarity {
  /** The basic model for information content. */
  protected final BasicModel basicModel;
  /** The first normalization of the information content. */
  protected final AfterEffect afterEffect;
  /** The term frequency normalization. */
  protected final Normalization normalization;
  
  public DFRSimilarity(BasicModel basicModel,
                       AfterEffect afterEffect,
                       Normalization normalization) {
    this.basicModel = basicModel;
    this.afterEffect = afterEffect != null
                     ? afterEffect : new AfterEffect.NoAfterEffect();
    this.normalization = normalization != null
                       ? normalization : new Normalization.NoNormalization();
  }
  
  @Override
  protected float score(EasyStats stats, float freq, byte norm) {
    float tfn = normalization.tfn(stats, freq, decodeNormValue(norm));
    return stats.getTotalBoost() *
        basicModel.score(stats, tfn) * afterEffect.score(stats, tfn);
  }

  @Override
  protected void explain(Explanation expl,
      EasyStats stats, int doc, float freq, byte norm) {
    if (stats.getTotalBoost() != 1.0f) {
      expl.addDetail(new Explanation(stats.getTotalBoost(), "boost"));
    }
    
    int len = decodeNormValue(norm);
    Explanation normExpl = normalization.explain(stats, freq, len);
    float tfn = normExpl.getValue();
    expl.addDetail(normExpl);
    expl.addDetail(basicModel.explain(stats, tfn));
    expl.addDetail(afterEffect.explain(stats, tfn));
  }
}
