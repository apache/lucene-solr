package org.apache.lucene.search.similarities;

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.Norm;
import org.apache.lucene.util.BytesRef;

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

/** Expert: Default scoring implementation. */
public class DefaultSimilarity extends TFIDFSimilarity {
  
  /** Implemented as <code>overlap / maxOverlap</code>. */
  public float coord(int overlap, int maxOverlap) {
    return overlap / (float)maxOverlap;
  }

  /** Implemented as <code>1/sqrt(sumOfSquaredWeights)</code>. */
  public float queryNorm(float sumOfSquaredWeights) {
    return (float)(1.0 / Math.sqrt(sumOfSquaredWeights));
  }
  
  /** Implemented as
   *  <code>state.getBoost()*lengthNorm(numTerms)</code>, where
   *  <code>numTerms</code> is {@link FieldInvertState#getLength()} if {@link
   *  #setDiscountOverlaps} is false, else it's {@link
   *  FieldInvertState#getLength()} - {@link
   *  FieldInvertState#getNumOverlap()}.
   *
   *  @lucene.experimental */
  @Override
  public void computeNorm(FieldInvertState state, Norm norm) {
    final int numTerms;
    if (discountOverlaps)
      numTerms = state.getLength() - state.getNumOverlap();
    else
      numTerms = state.getLength();
    norm.setByte(encodeNormValue(state.getBoost() * ((float) (1.0 / Math.sqrt(numTerms)))));
  }

  /** Implemented as <code>sqrt(freq)</code>. */
  @Override
  public float tf(float freq) {
    return (float)Math.sqrt(freq);
  }
    
  /** Implemented as <code>1 / (distance + 1)</code>. */
  @Override
  public float sloppyFreq(int distance) {
    return 1.0f / (distance + 1);
  }
  
  /** The default implementation returns <code>1</code> */
  @Override
  public float scorePayload(int doc, int start, int end, BytesRef payload) {
    return 1;
  }

  /** Implemented as <code>log(numDocs/(docFreq+1)) + 1</code>. */
  @Override
  public float idf(long docFreq, long numDocs) {
    return (float)(Math.log(numDocs/(double)(docFreq+1)) + 1.0);
  }
    
  // Default true
  protected boolean discountOverlaps = true;

  /** Determines whether overlap tokens (Tokens with
   *  0 position increment) are ignored when computing
   *  norm.  By default this is true, meaning overlap
   *  tokens do not count when computing norms.
   *
   *  @lucene.experimental
   *
   *  @see #computeNorm
   */
  public void setDiscountOverlaps(boolean v) {
    discountOverlaps = v;
  }

  /** @see #setDiscountOverlaps */
  public boolean getDiscountOverlaps() {
    return discountOverlaps;
  }

  @Override
  public String toString() {
    return "DefaultSimilarity";
  }
}
