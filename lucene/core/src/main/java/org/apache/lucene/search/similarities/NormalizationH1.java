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
package org.apache.lucene.search.similarities;


import org.apache.lucene.search.Explanation;

/**
 * Normalization model that assumes a uniform distribution of the term frequency.
 * <p>While this model is parameterless in the
 * <a href="http://citeseer.ist.psu.edu/viewdoc/summary?doi=10.1.1.101.742">
 * original article</a>, <a href="http://dl.acm.org/citation.cfm?id=1835490">
 * information-based models</a> (see {@link IBSimilarity}) introduced a
 * multiplying factor.
 * The default value for the {@code c} parameter is {@code 1}.</p>
 * @lucene.experimental
 */
public class NormalizationH1 extends Normalization {
  private final float c;
  
  /**
   * Creates NormalizationH1 with the supplied parameter <code>c</code>.
   * @param c hyper-parameter that controls the term frequency 
   * normalization with respect to the document length.
   */
  public NormalizationH1(float c) {
    // unbounded but typical range 0..10 or so
    if (Float.isFinite(c) == false || c < 0) {
      throw new IllegalArgumentException("illegal c value: " + c + ", must be a non-negative finite value");
    }
    this.c = c;
  }
  
  /**
   * Calls {@link #NormalizationH1(float) NormalizationH1(1)}
   */
  public NormalizationH1() {
    this(1);
  }
  
  @Override
  public final double tfn(BasicStats stats, double tf, double len) {
    return tf * c * (stats.getAvgFieldLength() / len);
  }

  @Override
  public Explanation explain(BasicStats stats, double tf, double len) {
    return Explanation.match(
        (float) tfn(stats, tf, len),
        getClass().getSimpleName()
            + ", computed as tf * c * (avgfl / fl) from:",
        Explanation.match((float) tf,
            "tf, number of occurrences of term in the document"),
        Explanation.match(c,
            "c, hyper-parameter"),
        Explanation.match((float) stats.getAvgFieldLength(),
            "avgfl, average length of field across all documents"),
        Explanation.match((float) len, "fl, field length of the document"));
  }

  @Override
  public String toString() {
    return "1";
  }
  
  /**
   * Returns the <code>c</code> parameter.
   * @see #NormalizationH1(float)
   */
  public float getC() {
    return c;
  }
}
