package org.apache.lucene.util;

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

import java.util.*;

import org.junit.Assert;
import org.junit.Test;

public class TestIdentityHashSet extends LuceneTestCase {
  @Test
  public void testCheck() {
    Random rnd = random();
    Set<Object> jdk = Collections.newSetFromMap(
        new IdentityHashMap<Object,Boolean>());
    RamUsageEstimator.IdentityHashSet<Object> us = new RamUsageEstimator.IdentityHashSet<Object>();

    int max = 100000;
    int threshold = 256;
    for (int i = 0; i < max; i++) {
      // some of these will be interned and some will not so there will be collisions.
      Integer v = rnd.nextInt(threshold);
      
      boolean e1 = jdk.contains(v);
      boolean e2 = us.contains(v);
      Assert.assertEquals(e1, e2);

      e1 = jdk.add(v);
      e2 = us.add(v);
      Assert.assertEquals(e1, e2);
    }
    
    Set<Object> collected = Collections.newSetFromMap(
        new IdentityHashMap<Object,Boolean>());
    for (Object o : us) {
      collected.add(o);
    }
    
    Assert.assertEquals(collected, jdk);
  }
}
