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

package org.apache.lucene.luwak.termextractor.weights;

import org.apache.lucene.luwak.termextractor.QueryTerm;

/**
 * Weight a token by its length
 */
public class TokenLengthNorm extends WeightNorm {

  private final float[] lengthNorms = new float[32];

  /**
   * Create a new TokenLengthNorm
   * <p>
   * Tokens will be scaled according to the equation a * e^(-k * tokenlength)
   *
   * @param a a
   * @param k k
   */
  public TokenLengthNorm(float a, float k) {
    for (int i = 0; i < 32; i++) {
      lengthNorms[i] = (float) (a * (Math.exp(-k * i)));
    }
  }

  public TokenLengthNorm() {
    this(3, 0.3f);
  }

  @Override
  public float norm(QueryTerm term) {
    if (term.term.bytes().length >= 32) {
      return Integer.MAX_VALUE;
    }
    return 4 - lengthNorms[term.term.bytes().length];
  }
}
