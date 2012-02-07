package org.apache.lucene.search.similarities;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
 * @see BasicModel
 * @see AfterEffect
 * @see Normalization
 * @lucene.experimental
 */
public class DFRSimilarity extends SimilarityBase {
  /** The basic model for information content. */
  protected final BasicModel basicModel;
  /** The first normalization of the information content. */
  protected final AfterEffect afterEffect;
  /** The term frequency normalization. */
  protected final Normalization normalization;
  
  public DFRSimilarity(BasicModel basicModel,
                       AfterEffect afterEffect,
                       Normalization normalization) {
    if (basicModel == null || afterEffect == null || normalization == null) {
      throw new NullPointerException("null parameters not allowed.");
    }
    this.basicModel = basicModel;
    this.afterEffect = afterEffect;
    this.normalization = normalization;
  }

  @Override
  protected float score(BasicStats stats, float freq, float docLen) {
    float tfn = normalization.tfn(stats, freq, docLen);
    return stats.getTotalBoost() *
        basicModel.score(stats, tfn) * afterEffect.score(stats, tfn);
  }
  
  @Override
  protected void explain(Explanation expl,
      BasicStats stats, int doc, float freq, float docLen) {
    if (stats.getTotalBoost() != 1.0f) {
      expl.addDetail(new Explanation(stats.getTotalBoost(), "boost"));
    }
    
    Explanation normExpl = normalization.explain(stats, freq, docLen);
    float tfn = normExpl.getValue();
    expl.addDetail(normExpl);
    expl.addDetail(basicModel.explain(stats, tfn));
    expl.addDetail(afterEffect.explain(stats, tfn));
  }

  @Override
  public String toString() {
    return "DFR " + basicModel.toString() + afterEffect.toString()
                  + normalization.toString();
  }
  
  public BasicModel getBasicModel() {
    return basicModel;
  }
  
  public AfterEffect getAfterEffect() {
    return afterEffect;
  }
  
  public Normalization getNormalization() {
    return normalization;
  }
}
