package org.apache.lucene.search.spell;

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

import junit.framework.TestCase;

public class TestLevenshteinDistance extends TestCase {

  private StringDistance sd = new LevensteinDistance();
  
  public void testGetDistance() {
    float d = sd.getDistance("al", "al");
    assertTrue(d == 1.0f);
    d = sd.getDistance("martha", "marhta");
    assertTrue(d > 0.66 && d <0.67);
    d = sd.getDistance("jones", "johnson");
    assertTrue(d > 0.199 && d < 0.201);
    d = sd.getDistance("abcvwxyz", "cabvwxyz");
    assertTrue(d > 0.749 && d < 0.751);
    d = sd.getDistance("dwayne", "duane");
    assertTrue(d > 0.599 && d < 0.601);
    d = sd.getDistance("dixon", "dicksonx");
    assertTrue(d > 0.199 && d < 0.201);
    d = sd.getDistance("six", "ten");
    assertTrue(d == 0f);
    float d1 = sd.getDistance("zac ephron", "zac efron");
    float d2 = sd.getDistance("zac ephron", "kai ephron");
    assertTrue(d1 < d2);
    d1 = sd.getDistance("brittney spears", "britney spears");
    d2 = sd.getDistance("brittney spears", "brittney startzman");
    assertTrue(d1 > d2);    
  }

}
