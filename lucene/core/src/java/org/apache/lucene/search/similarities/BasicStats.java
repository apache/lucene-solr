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

  /** A query boost. Should be applied as a multiplicative factor to the score. */
  protected final float boost;
  
  /** Constructor. */
  public BasicStats(String field, float boost) {
    this.field = field;
    this.boost = boost;
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
  
  /** Returns the total boost. */
  public float getBoost() {
    return boost;
  }
}
