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

public class TestBM25Similarity extends LuceneTestCase {
  
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
}
