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

package org.apache.lucene.util;

import java.util.Map;
import java.util.WeakHashMap;

public class TestWeakIdentityHashMap extends LuceneTestCase {

  public void test() {
    final WeakIdentityHashMap<String,String> map =
      new WeakIdentityHashMap<String,String>();
    // we keep strong references to the keys,
    // so WeakIdentityHashMap will not forget about them:
    String key1 = new String("foo");
    String key2 = new String("foo");
    String key3 = new String("foo");
    
    assertNotSame(key1, key2);
    assertEquals(key1, key2);
    assertNotSame(key1, key3);
    assertEquals(key1, key3);
    assertNotSame(key2, key3);
    assertEquals(key2, key3);

    map.put(key1, "bar1");
    map.put(key2, "bar2");
    map.put(null, "null");
    
    assertEquals("bar1", map.get(key1));
    assertEquals("bar2", map.get(key2));
    assertEquals(null, map.get(key3));
    assertEquals("null", map.get(null));
    
    assertTrue(map.containsKey(key1));
    assertTrue(map.containsKey(key2));
    assertFalse(map.containsKey(key3));
    assertTrue(map.containsKey(null));

    assertEquals(3, map.size());
    map.remove(null);
    assertEquals(2, map.size());
    map.remove(key1);
    assertEquals(1, map.size());
    map.put(key1, "bar1");
    map.put(key2, "bar2");
    map.put(key3, "bar3");
    assertEquals(3, map.size());
    
    // clear strong refs
    key1 = key2 = key3 = null;
    
    // check that GC does not cause problems in reap() method:
    for (int i = 0; !map.isEmpty(); i++) try {
      if (i > 40)
        fail("The garbage collector did not reclaim all keys after 2 seconds, failing test!");
      System.runFinalization();
      System.gc();
      Thread.currentThread().sleep(50L);
    } catch (InterruptedException ie) {}
  }

}
