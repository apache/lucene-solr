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


import org.apache.lucene.util.LuceneTestCase;

public class TestAxiomaticSimilarity extends LuceneTestCase {
  
  public void testSaneNormValues() {
    BM25Similarity sim = new BM25Similarity();
    for (int i = 0; i < 256; i++) {
      float len = sim.decodeNormValue((byte) i);
      assertFalse("negative len: " + len + ", byte=" + i, len < 0.0f);
      assertFalse("inf len: " + len + ", byte=" + i, Float.isInfinite(len));
      assertFalse("nan len for byte=" + i, Float.isNaN(len));
      if (i > 0) {
        assertTrue("len is not decreasing: " + len + ",byte=" + i, len < sim.decodeNormValue((byte)(i-1)));
      }
    }
  }
  
  public void testIllegalS() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new AxiomaticF2EXP(Float.POSITIVE_INFINITY, 0.1f);
    });
    assertTrue(expected.getMessage().contains("illegal s value"));
    
    expected = expectThrows(IllegalArgumentException.class, () -> {
      new AxiomaticF2EXP(-1, 0.1f);
    });
    assertTrue(expected.getMessage().contains("illegal s value"));
    
    expected = expectThrows(IllegalArgumentException.class, () -> {
      new AxiomaticF2EXP(Float.NaN, 0.1f);
    });
    assertTrue(expected.getMessage().contains("illegal s value"));
  }
  
  public void testIllegalK() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new AxiomaticF2EXP(0.35f, 2f);
    });
    assertTrue(expected.getMessage().contains("illegal k value"));
    
    expected = expectThrows(IllegalArgumentException.class, () -> {
      new AxiomaticF2EXP(0.35f, -1f);
    });
    assertTrue(expected.getMessage().contains("illegal k value"));
    
    expected = expectThrows(IllegalArgumentException.class, () -> {
      new AxiomaticF2EXP(0.35f, Float.POSITIVE_INFINITY);
    });
    assertTrue(expected.getMessage().contains("illegal k value"));
    
    expected = expectThrows(IllegalArgumentException.class, () -> {
      new AxiomaticF2EXP(0.35f, Float.NaN);
    });
    assertTrue(expected.getMessage().contains("illegal k value"));
  }

  public void testIllegalQL() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new AxiomaticF3EXP(0.35f, -1);
    });
    assertTrue(expected.getMessage().contains("illegal query length value"));

    expected = expectThrows(IllegalArgumentException.class, () -> {
      new AxiomaticF2EXP(0.35f, Integer.MAX_VALUE+1);
    });
    assertTrue(expected.getMessage().contains("illegal query length value"));
  }
}
