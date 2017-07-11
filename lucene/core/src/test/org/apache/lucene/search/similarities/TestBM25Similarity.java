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


import org.apache.lucene.search.Explanation;
import org.apache.lucene.util.LuceneTestCase;

public class TestBM25Similarity extends LuceneTestCase {
  
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

  private static Explanation findExplanation(Explanation expl, String text) {
    if (expl.getDescription().equals(text)) {
      return expl;
    } else {
      for (Explanation sub : expl.getDetails()) {
        Explanation match = findExplanation(sub, text);
        if (match != null) {
          return match;
        }
      }
    }
    return null;
  }
}
