package org.apache.lucene.search;

import org.apache.lucene.index.FieldInvertState;

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

/** Expert: Delegating scoring implementation.  Useful in {@link
 * Query#getSimilarity(Searcher)} implementations, to override only certain
 * methods of a Searcher's Similarity implementation..
 * @deprecated this class will be removed in 4.0.  Please
 * subclass {@link Similarity} or {@link DefaultSimilarity} instead. */
@Deprecated
public class SimilarityDelegator extends Similarity {

  private Similarity delegee;

  /** Construct a {@link Similarity} that delegates all methods to another.
   *
   * @param delegee the Similarity implementation to delegate to
   */
  public SimilarityDelegator(Similarity delegee) {
    this.delegee = delegee;
  }

  @Override
  public float computeNorm(String fieldName, FieldInvertState state) {
    return delegee.computeNorm(fieldName, state);
  }

  @Override
  public float queryNorm(float sumOfSquaredWeights) {
    return delegee.queryNorm(sumOfSquaredWeights);
  }

  @Override
  public float tf(float freq) {
    return delegee.tf(freq);
  }
    
  @Override
  public float sloppyFreq(int distance) {
    return delegee.sloppyFreq(distance);
  }
    
  @Override
  public float idf(int docFreq, int numDocs) {
    return delegee.idf(docFreq, numDocs);
  }
    
  @Override
  public float coord(int overlap, int maxOverlap) {
    return delegee.coord(overlap, maxOverlap);
  }

  @Override
  public float scorePayload(int docId, String fieldName, int start, int end, byte [] payload, int offset, int length) {
    return delegee.scorePayload(docId, fieldName, start, end, payload, offset, length);
  }
}
