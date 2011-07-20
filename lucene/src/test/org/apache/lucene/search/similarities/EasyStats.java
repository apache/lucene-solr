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

import org.apache.lucene.search.Similarity;

/**
 * Stores all statistics commonly used ranking methods.
 * @lucene.experimental
 */
public class EasyStats extends Similarity.Stats {
  /** The number of documents. */
  protected int numberOfDocuments;	// TODO: to long?
  /** The total number of tokens in the field. */
  protected long numberOfFieldTokens;
  /** The average field length. */
  protected float avgFieldLength;
  /** The document frequency. */
  protected int docFreq;
  /** The total number of occurrences of this term across all documents. */
  // TODO: same field?
  protected long totalTermFreq;
  /** The total number of terms across all documents. */
  // TODO: same field?
  protected long sumTotalTermFreq;
  /** The number of unique terms. */
  // nocommit might be per-segment only
  protected long uniqueTermCount;
  
  // -------------------------- Boost-related stuff --------------------------
  
  /** Query's inner boost. */
  protected final float queryBoost;
  /** Any outer query's boost. */
  protected float topLevelBoost;
  /** For most Similarities, the immediate and the top level query boosts are
   * not handled differently. Hence, this field is just the product of the
   * other two. */
  protected float totalBoost;
  
  /** Constructor. Sets the query boost. */
  public EasyStats(float queryBoost) {
    this.queryBoost = queryBoost;
  }
  
  // ------------------------- Getter/setter methods -------------------------
  
  /** Returns the number of documents. */
  public int getNumberOfDocuments() {
    return numberOfDocuments;
  }
  
  /** Sets the number of documents. */
  public void setNumberOfDocuments(int numberOfDocuments) {
    this.numberOfDocuments = numberOfDocuments;
  }
  
  /** Returns the total number of tokens in the field. */
  public long getNumberOfFieldTokens() {
    return numberOfFieldTokens;
  }
  
  /** Sets the total number of tokens in the field. */
  public void setNumberOfFieldTokens(long numberOfFieldTokens) {
    this.numberOfFieldTokens = numberOfFieldTokens;
  }
  
  /** Returns the average field length. */
  public float getAvgFieldLength() {
    return avgFieldLength;
  }
  
  /** Sets the average field length. */
  public void setAvgFieldLength(float avgFieldLength) {
    this.avgFieldLength = avgFieldLength;
  }
  
  /** Returns the document frequency. */
  public int getDocFreq() {
    return docFreq;
  }
  
  /** Sets the document frequency. */
  public void setDocFreq(int docFreq) {
    this.docFreq = docFreq;
  }
  
  /** Returns the total number of occurrences of this term across all documents. */
  public long getTotalTermFreq() {
    return totalTermFreq;
  }
  
  /** Sets the total number of occurrences of this term across all documents. */
  public void setTotalTermFreq(long totalTermFreq) {
    this.totalTermFreq = totalTermFreq;
  }
  
  /** Returns the total number of terms across all documents. */
  public long getSumTotalTermFreq() {
    return sumTotalTermFreq;
  }
  
  /** Sets the total number of terms across all documents. */
  public void setSumTotalTermFreq(long sumTotalTermFreq) {
    this.sumTotalTermFreq = sumTotalTermFreq;
  }
  
  /** Returns the number of unique terms. */
  public long getUniqueTermCount() {
    return uniqueTermCount;
  }
  
  /** Sets the number of unique terms. */
  public void setUniqueTermCount(long uniqueTermCount) {
    this.uniqueTermCount = uniqueTermCount;
  }
  
  // -------------------------- Boost-related stuff --------------------------
  
  /** The square of the raw normalization value.
   * @see #rawNormalizationValue() */
  @Override
  public float getValueForNormalization() {
    float rawValue = rawNormalizationValue();
    return rawValue * rawValue;
  }
  
  /** Computes the raw normalization value. This basic implementation returns
   * the query boost. Subclasses may override this method to include other
   * factors (such as idf), or to save the value for inclusion in
   * {@link #normalize(float, float)}, etc.
   */
  protected float rawNormalizationValue() {
    return queryBoost;
  }
  
  /** No normalization is done. {@code topLevelBoost} is saved in the object,
   * however. */
  @Override
  public void normalize(float queryNorm, float topLevelBoost) {
    this.topLevelBoost = topLevelBoost;
    totalBoost = queryBoost * topLevelBoost;
  }
  
  /** Returns the total boost. */
  public float getTotalBoost() {
    return totalBoost;
  }
}
