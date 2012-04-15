package org.apache.lucene.search;

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

import java.util.Random;

import org.apache.lucene.index.Term;
import org.apache.lucene.util._TestUtil;

/**
 * random sloppy phrase query tests
 */
public class TestSloppyPhraseQuery2 extends SearchEquivalenceTestBase {
  /** "A B"~N ⊆ "A B"~N+1 */
  public void testIncreasingSloppiness() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    PhraseQuery q1 = new PhraseQuery();
    q1.add(t1);
    q1.add(t2);
    PhraseQuery q2 = new PhraseQuery();
    q2.add(t1);
    q2.add(t2);
    for (int i = 0; i < 10; i++) {
      q1.setSlop(i);
      q2.setSlop(i+1);
      assertSubsetOf(q1, q2);
    }
  }
  
  /** same as the above with posincr */
  public void testIncreasingSloppinessWithHoles() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    PhraseQuery q1 = new PhraseQuery();
    q1.add(t1);
    q1.add(t2, 2);
    PhraseQuery q2 = new PhraseQuery();
    q2.add(t1);
    q2.add(t2, 2);
    for (int i = 0; i < 10; i++) {
      q1.setSlop(i);
      q2.setSlop(i+1);
      assertSubsetOf(q1, q2);
    }
  }
  
  /** "A B C"~N ⊆ "A B C"~N+1 */
  public void testIncreasingSloppiness3() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    Term t3 = randomTerm();
    PhraseQuery q1 = new PhraseQuery();
    q1.add(t1);
    q1.add(t2);
    q1.add(t3);
    PhraseQuery q2 = new PhraseQuery();
    q2.add(t1);
    q2.add(t2);
    q2.add(t3);
    for (int i = 0; i < 10; i++) {
      q1.setSlop(i);
      q2.setSlop(i+1);
      assertSubsetOf(q1, q2);
    }
  }
  
  /** same as the above with posincr */
  public void testIncreasingSloppiness3WithHoles() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    Term t3 = randomTerm();
    int pos1 = 1 + random().nextInt(3);
    int pos2 = pos1 + 1 + random().nextInt(3);
    PhraseQuery q1 = new PhraseQuery();
    q1.add(t1);
    q1.add(t2, pos1);
    q1.add(t3, pos2);
    PhraseQuery q2 = new PhraseQuery();
    q2.add(t1);
    q2.add(t2, pos1);
    q2.add(t3, pos2);
    for (int i = 0; i < 10; i++) {
      q1.setSlop(i);
      q2.setSlop(i+1);
      assertSubsetOf(q1, q2);
    }
  }
  
  /** "A A"~N ⊆ "A A"~N+1 */
  public void testRepetitiveIncreasingSloppiness() throws Exception {
    Term t = randomTerm();
    PhraseQuery q1 = new PhraseQuery();
    q1.add(t);
    q1.add(t);
    PhraseQuery q2 = new PhraseQuery();
    q2.add(t);
    q2.add(t);
    for (int i = 0; i < 10; i++) {
      q1.setSlop(i);
      q2.setSlop(i+1);
      assertSubsetOf(q1, q2);
    }
  }
  
  /** same as the above with posincr */
  public void testRepetitiveIncreasingSloppinessWithHoles() throws Exception {
    Term t = randomTerm();
    PhraseQuery q1 = new PhraseQuery();
    q1.add(t);
    q1.add(t, 2);
    PhraseQuery q2 = new PhraseQuery();
    q2.add(t);
    q2.add(t, 2);
    for (int i = 0; i < 10; i++) {
      q1.setSlop(i);
      q2.setSlop(i+1);
      assertSubsetOf(q1, q2);
    }
  }
  
  /** "A A A"~N ⊆ "A A A"~N+1 */
  public void testRepetitiveIncreasingSloppiness3() throws Exception {
    Term t = randomTerm();
    PhraseQuery q1 = new PhraseQuery();
    q1.add(t);
    q1.add(t);
    q1.add(t);
    PhraseQuery q2 = new PhraseQuery();
    q2.add(t);
    q2.add(t);
    q2.add(t);
    for (int i = 0; i < 10; i++) {
      q1.setSlop(i);
      q2.setSlop(i+1);
      assertSubsetOf(q1, q2);
    }
  }
  
  /** same as the above with posincr */
  public void testRepetitiveIncreasingSloppiness3WithHoles() throws Exception {
    Term t = randomTerm();
    int pos1 = 1 + random().nextInt(3);
    int pos2 = pos1 + 1 + random().nextInt(3);
    PhraseQuery q1 = new PhraseQuery();
    q1.add(t);
    q1.add(t, pos1);
    q1.add(t, pos2);
    PhraseQuery q2 = new PhraseQuery();
    q2.add(t);
    q2.add(t, pos1);
    q2.add(t, pos2);
    for (int i = 0; i < 10; i++) {
      q1.setSlop(i);
      q2.setSlop(i+1);
      assertSubsetOf(q1, q2);
    }
  }
  
  /** MultiPhraseQuery~N ⊆ MultiPhraseQuery~N+1 */
  public void testRandomIncreasingSloppiness() throws Exception {
    long seed = random().nextLong();
    MultiPhraseQuery q1 = randomPhraseQuery(seed);
    MultiPhraseQuery q2 = randomPhraseQuery(seed);
    for (int i = 0; i < 10; i++) {
      q1.setSlop(i);
      q2.setSlop(i+1);
      assertSubsetOf(q1, q2);
    }
  }
  
  private MultiPhraseQuery randomPhraseQuery(long seed) throws Exception {
    Random random = new Random(seed);
    int length = _TestUtil.nextInt(random, 2, 5);
    MultiPhraseQuery pq = new MultiPhraseQuery();
    int position = 0;
    for (int i = 0; i < length; i++) {
      int depth = _TestUtil.nextInt(random, 1, 3);
      Term terms[] = new Term[depth];
      for (int j = 0; j < depth; j++) {
        terms[j] = new Term("field", "" + (char) _TestUtil.nextInt(random, 'a', 'z'));
      }
      pq.add(terms, position);
      position += _TestUtil.nextInt(random, 1, 3);
    }
    return pq;
  }
}
