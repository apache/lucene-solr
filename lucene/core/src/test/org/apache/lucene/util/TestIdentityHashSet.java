package org.apache.lucene.util;

import java.util.*;

import org.junit.Assert;
import org.junit.Test;

public class TestIdentityHashSet extends LuceneTestCase {
  @Test
  public void testCheck() {
    Random rnd = random;
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
