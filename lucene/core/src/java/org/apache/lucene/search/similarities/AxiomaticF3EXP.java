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
 * F2EXP is defined as Sum(tf(term_doc_freq)*IDF(term)-gamma(docLen, queryLen))
 * where IDF(t) = pow((N+1)/df(t), k) N=total num of docs, df=doc freq
 * gamma(docLen, queryLen) = (docLen-queryLen)*queryLen*s/avdl
 *
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
  protected float tf(BasicStats stats, float freq, float docLen) {
    if (freq <= 0.0) return 0f;
    return (float) (1 + Math.log(1 + Math.log(freq)));
  }

  /**
   * compute the document length component
   */
  @Override
  protected float ln(BasicStats stats, float freq, float docLen) {
    return 1f;
  }

  /**
   * compute the mixed term frequency and document length component
   */
  @Override
  protected float tfln(BasicStats stats, float freq, float docLen) {
    return 1f;
  }

  /**
   * compute the inverted document frequency component
   */
  @Override
  protected float idf(BasicStats stats, float freq, float docLen) {
    return (float) Math.pow((stats.getNumberOfDocuments() + 1.0) / stats.getDocFreq(), this.k);
  }

  /**
   * compute the gamma component
   */
  @Override
  protected float gamma(BasicStats stats, float freq, float docLen) {
    return (docLen - this.queryLen) * this.s * this.queryLen / stats.getAvgFieldLength();
  }
}