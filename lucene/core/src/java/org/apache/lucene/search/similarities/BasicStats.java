package org.apache.lucene.search.similarities;

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

import org.apache.lucene.index.Terms;

/**
 * Stores all statistics commonly used ranking methods.
 * @lucene.experimental
 */
public class BasicStats extends Similarity.SimWeight {
  final String field;
  /** The number of documents. */
  protected long numberOfDocuments;
  /** The total number of tokens in the field. */
  protected long numberOfFieldTokens;
  /** The average field length. */
  protected float avgFieldLength;
  /** The document frequency. */
  protected long docFreq;
  /** The total number of occurrences of this term across all documents. */
  protected long totalTermFreq;
  
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
  public BasicStats(String field, float queryBoost) {
    this.field = field;
    this.queryBoost = queryBoost;
    this.totalBoost = queryBoost;
  }
  
  // ------------------------- Getter/setter methods -------------------------
  
  /** Returns the number of documents. */
  public long getNumberOfDocuments() {
    return numberOfDocuments;
  }
  
  /** Sets the number of documents. */
  public void setNumberOfDocuments(long numberOfDocuments) {
    this.numberOfDocuments = numberOfDocuments;
  }
  
  /**
   * Returns the total number of tokens in the field.
   * @see Terms#getSumTotalTermFreq()
   */
  public long getNumberOfFieldTokens() {
    return numberOfFieldTokens;
  }
  
  /**
   * Sets the total number of tokens in the field.
   * @see Terms#getSumTotalTermFreq()
   */
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
  public long getDocFreq() {
    return docFreq;
  }
  
  /** Sets the document frequency. */
  public void setDocFreq(long docFreq) {
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
