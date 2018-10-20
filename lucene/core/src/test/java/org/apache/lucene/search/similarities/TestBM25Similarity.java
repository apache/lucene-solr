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


import java.util.Random;

public class TestBM25Similarity extends BaseSimilarityTestCase {
  
  public void testIllegalK1() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new BM25Similarity(Float.POSITIVE_INFINITY, 0.75f);
    });
    assertTrue(expected.getMessage().contains("illegal k1 value"));
    
    expected = expectThrows(IllegalArgumentException.class, () -> {
      new BM25Similarity(-1, 0.75f);
    });
    assertTrue(expected.getMessage().contains("illegal k1 value"));
    
    expected = expectThrows(IllegalArgumentException.class, () -> {
      new BM25Similarity(Float.NaN, 0.75f);
    });
    assertTrue(expected.getMessage().contains("illegal k1 value"));
  }
  
  public void testIllegalB() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new BM25Similarity(1.2f, 2f);
    });
    assertTrue(expected.getMessage().contains("illegal b value"));
    
    expected = expectThrows(IllegalArgumentException.class, () -> {
      new BM25Similarity(1.2f, -1f);
    });
    assertTrue(expected.getMessage().contains("illegal b value"));
    
    expected = expectThrows(IllegalArgumentException.class, () -> {
      new BM25Similarity(1.2f, Float.POSITIVE_INFINITY);
    });
    assertTrue(expected.getMessage().contains("illegal b value"));
    
    expected = expectThrows(IllegalArgumentException.class, () -> {
      new BM25Similarity(1.2f, Float.NaN);
    });
    assertTrue(expected.getMessage().contains("illegal b value"));
  }

  @Override
  protected Similarity getSimilarity(Random random) {
    // term frequency normalization parameter k1
    final float k1;
    switch (random.nextInt(4)) {
      case 0:
        // minimum value
        k1 = 0;
        break;
      case 1:
        // tiny value
        k1 = Float.MIN_VALUE;
        break;
      case 2:
        // maximum value
        // upper bounds on individual term's score is 43.262806 * (k1 + 1) * boost
        // we just limit the test to "reasonable" k1 values but don't enforce this anywhere.
        k1 = Integer.MAX_VALUE;
        break;
      default:
        // random value
        k1 = Integer.MAX_VALUE * random.nextFloat();
        break;
    }
    
    // length normalization parameter b [0 .. 1]
    final float b;
    switch (random.nextInt(4)) {
      case 0:
        // minimum value
        b = 0;
        break;
      case 1:
        // tiny value
        b = Float.MIN_VALUE;
        break;
      case 2:
        // maximum value
        b = 1;
        break;
      default:
        // random value
        b = random.nextFloat();
        break;
    }
    return new BM25Similarity(k1, b);
  }
}
