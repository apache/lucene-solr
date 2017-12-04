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

/**
 * F3EXP is defined as Sum(tf(term_doc_freq)*IDF(term)-gamma(docLen, queryLen))
 * where IDF(t) = pow((N+1)/df(t), k) N=total num of docs, df=doc freq
 * gamma(docLen, queryLen) = (docLen-queryLen)*queryLen*s/avdl
 * NOTE: the gamma function of this similarity creates negative scores
 * @lucene.experimental
 */
public class AxiomaticF3EXP extends Axiomatic {

  /**
   * Constructor setting all Axiomatic hyperparameters
   *
   * @param s        hyperparam for the growth function
   * @param queryLen the query length
   * @param k        hyperparam for the primitive weighting function
   */
  public AxiomaticF3EXP(float s, int queryLen, float k) {
    super(s, queryLen, k);
  }

  /**
   * Constructor setting s and queryLen, letting k to default
   *
   * @param s        hyperparam for the growth function
   * @param queryLen the query length
   */
  public AxiomaticF3EXP(float s, int queryLen) {
    this(s, queryLen, 0.35f);
  }

  @Override
  public String toString() {
    return "F3EXP";
  }

  /**
   * compute the term frequency component
   */
  @Override
  protected double tf(BasicStats stats, double freq, double docLen) {
    if (freq <= 0.0) return 0.0;
    return 1 + Math.log(1 + Math.log(freq));
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
    return 1.0;
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
    return (docLen - this.queryLen) * this.s * this.queryLen / stats.getAvgFieldLength();
  }
}