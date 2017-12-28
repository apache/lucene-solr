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
 * F1LOG is defined as Sum(tf(term_doc_freq)*ln(docLen)*IDF(term))
 * where IDF(t) = ln((N+1)/df(t)) N=total num of docs, df=doc freq
 *
 * @lucene.experimental
 */
public class AxiomaticF1LOG extends Axiomatic {

  /**
   * Constructor setting s only, letting k and queryLen to default
   *
   * @param s hyperparam for the growth function
   */
  public AxiomaticF1LOG(float s) {
    super(s);
  }

  /**
   * Default constructor
   */
  public AxiomaticF1LOG() {
    super();
  }

  @Override
  public String toString() {
    return "F1LOG";
  }

  /**
   * compute the term frequency component
   */
  @Override
  protected double tf(BasicStats stats, double freq, double docLen) {
    freq += 1; // otherwise gives negative scores for freqs < 1
    return 1 + Math.log(1 + Math.log(freq));
  }

  /**
   * compute the document length component
   */
  @Override
  protected double ln(BasicStats stats, double freq, double docLen) {
    return (stats.getAvgFieldLength() + this.s) / (stats.getAvgFieldLength() + docLen * this.s);
  }

  /**
   * compute the mixed term frequency and document length component
   */
  @Override
  protected double tfln(BasicStats stats, double freq, double docLen) {
    return 1.0;
  }

  /**
   * compute the inverted document frequency component
   */
  @Override
  protected double idf(BasicStats stats, double freq, double docLen) {
    return Math.log((stats.getNumberOfDocuments() + 1.0) / stats.getDocFreq());
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
        "tf, term frequency computed as 1 + log(1 + log(freq)) from:",
        Explanation.match((float) freq,
            "freq, number of occurrences of term in the document"));
  };

  @Override
  protected Explanation lnExplain(BasicStats stats, double freq, double docLen){
    return Explanation.match((float) ln(stats, freq, docLen),
        "ln, document length computed as (avgdl + s) / (avgdl + dl * s) from:",
        Explanation.match((float) stats.getAvgFieldLength(),
            "avgdl, average length of field across all documents"),
        Explanation.match((float) docLen,
            "dl, length of field"));
  };

  protected Explanation tflnExplain(BasicStats stats, double freq, double docLen){
    return Explanation.match((float) tfln(stats, freq, docLen),
        "tfln, mixed term frequency and document length, equals to 1");
  };

  protected Explanation idfExplain(BasicStats stats, double freq, double docLen){
    return Explanation.match((float) idf(stats, freq, docLen),
        "idf, inverted document frequency computed as log((N + 1) / n) from:",
        Explanation.match((float) stats.getNumberOfDocuments(),
            "N, total number of documents with field"),
        Explanation.match((float) stats.getDocFreq(),
            "n, number of documents containing term"));
  };
}