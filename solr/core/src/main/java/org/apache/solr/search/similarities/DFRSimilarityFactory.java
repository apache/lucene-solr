/*
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
package org.apache.solr.search.similarities;

import org.apache.lucene.search.similarities.AfterEffect;
import org.apache.lucene.search.similarities.AfterEffectB;
import org.apache.lucene.search.similarities.AfterEffectL;
import org.apache.lucene.search.similarities.BasicModel;
import org.apache.lucene.search.similarities.BasicModelG;
import org.apache.lucene.search.similarities.BasicModelIF;
import org.apache.lucene.search.similarities.BasicModelIn;
import org.apache.lucene.search.similarities.BasicModelIne;
import org.apache.lucene.search.similarities.DFRSimilarity;
import org.apache.lucene.search.similarities.Normalization;
import org.apache.lucene.search.similarities.Normalization.NoNormalization; // javadoc
import org.apache.lucene.search.similarities.NormalizationH1;
import org.apache.lucene.search.similarities.NormalizationH2;
import org.apache.lucene.search.similarities.NormalizationH3;
import org.apache.lucene.search.similarities.NormalizationZ;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.schema.SimilarityFactory;

/**
 * Factory for {@link DFRSimilarity}
 * <p>
 * You must specify the implementations for all three components of
 * DFR (strings). In general the models are parameter-free, but two of the
 * normalizations take floating point parameters (see below):
 * <ol>
 *    <li>{@link BasicModel basicModel}: Basic model of information content:
 *        <ul>
 *           <li>{@link BasicModelG G}: Geometric approximation of Bose-Einstein
 *           <li>{@link BasicModelIn I(n)}: Inverse document frequency
 *           <li>{@link BasicModelIne I(ne)}: Inverse expected document
 *               frequency [mixture of Poisson and IDF]
 *           <li>{@link BasicModelIF I(F)}: Inverse term frequency
 *               [approximation of I(ne)]
 *        </ul>
 *    <li>{@link AfterEffect afterEffect}: First normalization of information
 *        gain:
 *        <ul>
 *           <li>{@link AfterEffectL L}: Laplace's law of succession
 *           <li>{@link AfterEffectB B}: Ratio of two Bernoulli processes
 *        </ul>
 *    <li>{@link Normalization normalization}: Second (length) normalization:
 *        <ul>
 *           <li>{@link NormalizationH1 H1}: Uniform distribution of term
 *               frequency
 *               <ul>
 *                  <li>parameter c (float): hyper-parameter that controls
 *                      the term frequency normalization with respect to the
 *                      document length. The default is <code>1</code>
 *               </ul>
 *           <li>{@link NormalizationH2 H2}: term frequency density inversely
 *               related to length
 *               <ul>
 *                  <li>parameter c (float): hyper-parameter that controls
 *                      the term frequency normalization with respect to the
 *                      document length. The default is <code>1</code>
 *                </ul>
 *           <li>{@link NormalizationH3 H3}: term frequency normalization
 *               provided by Dirichlet prior
 *               <ul>
 *                  <li>parameter mu (float): smoothing parameter &mu;. The
 *                      default is <code>800</code>
 *               </ul>
 *           <li>{@link NormalizationZ Z}: term frequency normalization provided
 *                by a Zipfian relation
 *               <ul>
 *                  <li>parameter z (float): represents <code>A/(A+1)</code>
 *                      where A measures the specificity of the language.
 *                      The default is <code>0.3</code>
 *               </ul>
 *           <li>{@link NoNormalization none}: no second normalization
 *        </ul>
 * </ol>
 * <p>
 * Optional settings:
 * <ul>
 *   <li>discountOverlaps (bool): Sets
 *       {@link DFRSimilarity#setDiscountOverlaps(boolean)}</li>
 * </ul>
 * @lucene.experimental
 */
public class DFRSimilarityFactory extends SimilarityFactory {
  private boolean discountOverlaps;
  private BasicModel basicModel;
  private AfterEffect afterEffect;
  private Normalization normalization;

  @Override
  public void init(SolrParams params) {
    super.init(params);
    discountOverlaps = params.getBool("discountOverlaps", true);
    basicModel = parseBasicModel(params.get("basicModel"));
    afterEffect = parseAfterEffect(params.get("afterEffect"));
    normalization = parseNormalization(
        params.get("normalization"), params.get("c"), params.get("mu"), params.get("z"));
  }
  
  private BasicModel parseBasicModel(String expr) {
    if ("G".equals(expr)) {
      return new BasicModelG();
    } else if ("I(F)".equals(expr)) {
      return new BasicModelIF();
    } else if ("I(n)".equals(expr)) {
      return new BasicModelIn();
    } else if ("I(ne)".equals(expr)) {
      return new BasicModelIne();
    } else {
      throw new RuntimeException("Invalid basicModel: " + expr);
    }
  }
  
  private AfterEffect parseAfterEffect(String expr) {
    if ("B".equals(expr)) {
      return new AfterEffectB();
    } else if ("L".equals(expr)) {
      return new AfterEffectL();
    } else {
      throw new RuntimeException("Invalid afterEffect: " + expr);
    }
  }
  
  // also used by IBSimilarityFactory
  static Normalization parseNormalization(String expr, String c, String mu, String z) {
    if (mu != null && !"H3".equals(expr)) {
      throw new RuntimeException(
          "parameter mu only makes sense for normalization H3");
    }
    if (z != null && !"Z".equals(expr)) {
      throw new RuntimeException(
          "parameter z only makes sense for normalization Z");
    }
    if (c != null && !("H1".equals(expr) || "H2".equals(expr))) {
      throw new RuntimeException(
          "parameter c only makese sense for normalizations H1 and H2");
    }
    if ("H1".equals(expr)) {
      return (c != null) ? new NormalizationH1(Float.parseFloat(c))
                         : new NormalizationH1();
    } else if ("H2".equals(expr)) {
      return (c != null) ? new NormalizationH2(Float.parseFloat(c))
                         : new NormalizationH2();
    } else if ("H3".equals(expr)) {
      return (mu != null) ? new NormalizationH3(Float.parseFloat(mu))
                          : new NormalizationH3();
    } else if ("Z".equals(expr)) {
      return (z != null) ? new NormalizationZ(Float.parseFloat(z))
                         : new NormalizationZ();
    } else if ("none".equals(expr)) {
      return new Normalization.NoNormalization();
    } else {
      throw new RuntimeException("Invalid normalization: " + expr);
    }
  }

  @Override
  public Similarity getSimilarity() {
    DFRSimilarity sim = new DFRSimilarity(basicModel, afterEffect, normalization);
    sim.setDiscountOverlaps(discountOverlaps);
    return sim;
  }
}
