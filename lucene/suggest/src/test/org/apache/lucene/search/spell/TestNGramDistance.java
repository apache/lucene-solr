package org.apache.lucene.search.spell;

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

import org.apache.lucene.util.LuceneTestCase;

public class TestNGramDistance extends LuceneTestCase {

  
  
  public void testGetDistance1() {
    StringDistance nsd = new NGramDistance(1);
    float d = nsd.getDistance("al", "al");
    assertEquals(d,1.0f,0.001);
    d = nsd.getDistance("a", "a");
    assertEquals(d,1.0f,0.001);
    d = nsd.getDistance("b", "a");
    assertEquals(d,0.0f,0.001);
    d = nsd.getDistance("martha", "marhta");
    assertEquals(d,0.6666,0.001);
    d = nsd.getDistance("jones", "johnson");
    assertEquals(d,0.4285,0.001);
    d = nsd.getDistance("natural", "contrary");
    assertEquals(d,0.25,0.001);
    d = nsd.getDistance("abcvwxyz", "cabvwxyz");
    assertEquals(d,0.75,0.001);    
    d = nsd.getDistance("dwayne", "duane");
    assertEquals(d,0.666,0.001);
    d = nsd.getDistance("dixon", "dicksonx");
    assertEquals(d,0.5,0.001);
    d = nsd.getDistance("six", "ten");
    assertEquals(d,0,0.001);
    float d1 = nsd.getDistance("zac ephron", "zac efron");
    float d2 = nsd.getDistance("zac ephron", "kai ephron");
    assertEquals(d1,d2,0.001);
    d1 = nsd.getDistance("brittney spears", "britney spears");
    d2 = nsd.getDistance("brittney spears", "brittney startzman");
    assertTrue(d1 > d2);
    d1 = nsd.getDistance("12345678", "12890678");
    d2 = nsd.getDistance("12345678", "72385698");
    assertEquals(d1,d2,001);
  }
  
  public void testGetDistance2() {
    StringDistance sd = new NGramDistance(2);
    float d = sd.getDistance("al", "al");
    assertEquals(d,1.0f,0.001);
    d = sd.getDistance("a", "a");
    assertEquals(d,1.0f,0.001);
    d = sd.getDistance("b", "a");
    assertEquals(d,0.0f,0.001);
    d = sd.getDistance("a", "aa");
    assertEquals(d,0.5f,0.001);
    d = sd.getDistance("martha", "marhta");
    assertEquals(d,0.6666,0.001);
    d = sd.getDistance("jones", "johnson");
    assertEquals(d,0.4285,0.001);
    d = sd.getDistance("natural", "contrary");
    assertEquals(d,0.25,0.001);
    d = sd.getDistance("abcvwxyz", "cabvwxyz");
    assertEquals(d,0.625,0.001);    
    d = sd.getDistance("dwayne", "duane");
    assertEquals(d,0.5833,0.001);
    d = sd.getDistance("dixon", "dicksonx");
    assertEquals(d,0.5,0.001);
    d = sd.getDistance("six", "ten");
    assertEquals(d,0,0.001);
    float d1 = sd.getDistance("zac ephron", "zac efron");
    float d2 = sd.getDistance("zac ephron", "kai ephron");
    assertTrue(d1 > d2);
    d1 = sd.getDistance("brittney spears", "britney spears");
    d2 = sd.getDistance("brittney spears", "brittney startzman");
    assertTrue(d1 > d2);
    d1 = sd.getDistance("0012345678", "0012890678");
    d2 = sd.getDistance("0012345678", "0072385698");
    assertEquals(d1,d2,0.001);
  }
  
  public void testGetDistance3() {
    StringDistance sd = new NGramDistance(3);
    float d = sd.getDistance("al", "al");
    assertEquals(d,1.0f,0.001);
    d = sd.getDistance("a", "a");
    assertEquals(d,1.0f,0.001);
    d = sd.getDistance("b", "a");
    assertEquals(d,0.0f,0.001);
    d = sd.getDistance("martha", "marhta");
    assertEquals(d,0.7222,0.001);
    d = sd.getDistance("jones", "johnson");
    assertEquals(d,0.4762,0.001);
    d = sd.getDistance("natural", "contrary");
    assertEquals(d,0.2083,0.001);
    d = sd.getDistance("abcvwxyz", "cabvwxyz");
    assertEquals(d,0.5625,0.001);    
    d = sd.getDistance("dwayne", "duane");
    assertEquals(d,0.5277,0.001);
    d = sd.getDistance("dixon", "dicksonx");
    assertEquals(d,0.4583,0.001);
    d = sd.getDistance("six", "ten");
    assertEquals(d,0,0.001);
    float d1 = sd.getDistance("zac ephron", "zac efron");
    float d2 = sd.getDistance("zac ephron", "kai ephron");
    assertTrue(d1 > d2);
    d1 = sd.getDistance("brittney spears", "britney spears");
    d2 = sd.getDistance("brittney spears", "brittney startzman");
    assertTrue(d1 > d2);
    d1 = sd.getDistance("0012345678", "0012890678");
    d2 = sd.getDistance("0012345678", "0072385698");
    assertTrue(d1 < d2);
  }

  public void testEmpty() throws Exception {
    StringDistance nsd = new NGramDistance(1);
    float d = nsd.getDistance("", "al");
    assertEquals(d,0.0f,0.001);
  }
}
