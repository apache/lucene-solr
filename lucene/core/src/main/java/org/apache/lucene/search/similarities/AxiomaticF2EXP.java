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
 * F2EXP is defined as Sum(tfln(term_doc_freq, docLen)*IDF(term))
 * where IDF(t) = pow((N+1)/df(t), k) N=total num of docs, df=doc freq
 *
 * @lucene.experimental
 */
public class AxiomaticF2EXP extends Axiomatic {
  /**
   * Constructor setting s and k, letting queryLen to default
   * @param s hyperparam for the growth function
   * @param k hyperparam for the primitive weighting function
   */
  public AxiomaticF2EXP(float s, float k) {
    super(s, 1, k);
  }

  /**
   * Constructor setting s only, letting k and queryLen to default
   * @param s hyperparam for the growth function
   */
  public AxiomaticF2EXP(float s) {
    this(s, 0.35f);
  }

  /**
   * Default constructor
   */
  public AxiomaticF2EXP() {
    super();
  }

  @Override
  public String toString() {
    return "F2EXP";
  }

  /**
   * compute the term frequency component
   */
  @Override
  protected double tf(BasicStats stats, double freq, double docLen) {
    return 1.0;
  }

  /**
   * compute the document length component
   */
  @Override
  protected double ln(BasicStats stats, double freq, double docLen) {
    return 1.0;
  }

  /**
   * compute the mixed term frequency and document length component
   */
  @Override
  protected double tfln(BasicStats stats, double freq, double docLen) {
    return freq / (freq + this.s + this.s * docLen / stats.getAvgFieldLength());
  }

  /**
   * compute the inverted document frequency component
   */
  @Override
  protected double idf(BasicStats stats, double freq, double docLen) {
    return Math.pow((stats.getNumberOfDocuments() + 1.0) / stats.getDocFreq(), this.k);
  }

  /**
   * compute the gamma component
   */
  @Override
  protected double gamma(BasicStats stats, double freq, double docLen) {
    return 0.0;
  }

  @Override
  protected Explanation tfExplain(BasicStats stats, double freq, double docLen){
    return Explanation.match((float) tf(stats, freq, docLen),
        "tf, term frequency, equals to 1");
  };

  @Override
  protected Explanation lnExplain(BasicStats stats, double freq, double docLen){
    return Explanation.match((float) ln(stats, freq, docLen),
        "ln, document length, equals to 1");
  };

  protected Explanation tflnExplain(BasicStats stats, double freq, double docLen){
    return Explanation.match((float) tfln(stats, freq, docLen),
        "tfln, mixed term frequency and document length, " +
        "computed as freq / (freq + s + s * dl / avgdl) from:",
        Explanation.match((float) freq,
            "freq, number of occurrences of term in the document"),
        Explanation.match((float) docLen,
            "dl, length of field"),
        Explanation.match((float) stats.getAvgFieldLength(),
            "avgdl, average length of field across all documents"));
  };

  protected Explanation idfExplain(BasicStats stats, double freq, double docLen){
    return Explanation.match((float) idf(stats, freq, docLen),
        "idf, inverted document frequency computed as " +
            "Math.pow((N + 1) / n, k) from:",
        Explanation.match((float) stats.getNumberOfDocuments(),
            "N, total number of documents with field"),
        Explanation.match((float) stats.getDocFreq(),
            "n, number of documents containing term"));
  };
}