package org.apache.lucene.util.cache;

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

public class TestSimpleLRUCache extends TestCase {

  public void testLRUCache() throws Exception {
    final int n = 100;
    Object dummy = new Object();
    
    Cache cache = new SimpleLRUCache(n);
    
    for (int i = 0; i < n; i++) {
      cache.put(new Integer(i), dummy);
    }
    
    // access every 2nd item in cache
    for (int i = 0; i < n; i+=2) {
      assertNotNull(cache.get(new Integer(i)));
    }
    
    // add n/2 elements to cache, the ones that weren't
    // touched in the previous loop should now be thrown away
    for (int i = n; i < n + (n / 2); i++) {
      cache.put(new Integer(i), dummy);
    }
    
    // access every 4th item in cache
    for (int i = 0; i < n; i+=4) {
      assertNotNull(cache.get(new Integer(i)));
    }

    // add 3/4n elements to cache, the ones that weren't
    // touched in the previous loops should now be thrown away
    for (int i = n; i < n + (n * 3 / 4); i++) {
      cache.put(new Integer(i), dummy);
    }
    
    // access every 4th item in cache
    for (int i = 0; i < n; i+=4) {
      assertNotNull(cache.get(new Integer(i)));
    }
    
  }
  
}
