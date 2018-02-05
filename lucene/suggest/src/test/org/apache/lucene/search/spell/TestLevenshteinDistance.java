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
package org.apache.lucene.search.spell;

import org.apache.lucene.util.LuceneTestCase;

public class TestLevenshteinDistance extends LuceneTestCase {

  private StringDistance sd = new LevenshteinDistance();
  
  public void testGetDistance() {
    float d = sd.getDistance("al", "al");
    assertEquals(d,1.0f,0.001);
    d = sd.getDistance("martha", "marhta");
    assertEquals(d,0.6666,0.001);
    d = sd.getDistance("jones", "johnson");
    assertEquals(d,0.4285,0.001);
    d = sd.getDistance("abcvwxyz", "cabvwxyz");
    assertEquals(d,0.75,0.001);    
    d = sd.getDistance("dwayne", "duane");
    assertEquals(d,0.666,0.001);
    d = sd.getDistance("dixon", "dicksonx");
    assertEquals(d,0.5,0.001);
    d = sd.getDistance("six", "ten");
    assertEquals(d,0,0.001);
    float d1 = sd.getDistance("zac ephron", "zac efron");
    float d2 = sd.getDistance("zac ephron", "kai ephron");
    assertEquals(d1,d2,0.001);
    d1 = sd.getDistance("brittney spears", "britney spears");
    d2 = sd.getDistance("brittney spears", "brittney startzman");
    assertTrue(d1 > d2);
  }
  
  public void testEmpty() throws Exception {
    float d = sd.getDistance("", "al");
    assertEquals(d,0.0f,0.001);
  }

}
