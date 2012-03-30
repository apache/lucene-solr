package org.apache.lucene.util;

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

import org.apache.lucene.util.LuceneTestCase;

public class TestDoubleBarrelLRUCache extends LuceneTestCase {

  private void testCache(DoubleBarrelLRUCache<CloneableInteger,Object> cache, int n) throws Exception {
    Object dummy = new Object();
    
    for (int i = 0; i < n; i++) {
      cache.put(new CloneableInteger(i), dummy);
    }
    
    // access every 2nd item in cache
    for (int i = 0; i < n; i+=2) {
      assertNotNull(cache.get(new CloneableInteger(i)));
    }
    
    // add n/2 elements to cache, the ones that weren't
    // touched in the previous loop should now be thrown away
    for (int i = n; i < n + (n / 2); i++) {
      cache.put(new CloneableInteger(i), dummy);
    }
    
    // access every 4th item in cache
    for (int i = 0; i < n; i+=4) {
      assertNotNull(cache.get(new CloneableInteger(i)));
    }

    // add 3/4n elements to cache, the ones that weren't
    // touched in the previous loops should now be thrown away
    for (int i = n; i < n + (n * 3 / 4); i++) {
      cache.put(new CloneableInteger(i), dummy);
    }
    
    // access every 4th item in cache
    for (int i = 0; i < n; i+=4) {
      assertNotNull(cache.get(new CloneableInteger(i)));
    }
  }
    
  public void testLRUCache() throws Exception {
    final int n = 100;
    testCache(new DoubleBarrelLRUCache<CloneableInteger,Object>(n), n);
  }

  private class CacheThread extends Thread {
    private final CloneableObject[] objs;
    private final DoubleBarrelLRUCache<CloneableObject,Object> c;
    private final long endTime;
    volatile boolean failed;

    public CacheThread(DoubleBarrelLRUCache<CloneableObject,Object> c,
                       CloneableObject[] objs, long endTime) {
      this.c = c;
      this.objs = objs;
      this.endTime = endTime;
    }

    @Override
    public void run() {
      try {
        long count = 0;
        long miss = 0;
        long hit = 0;
        final int limit = objs.length;

        while(true) {
          final CloneableObject obj = objs[(int) ((count/2) % limit)];
          Object v = c.get(obj);
          if (v == null) {
            c.put(new CloneableObject(obj), obj);
            miss++;
          } else {
            assert obj == v;
            hit++;
          }
          if ((++count % 10000) == 0) {
            if (System.currentTimeMillis() >= endTime)  {
              break;
            }
          }
        }

        addResults(miss, hit);
      } catch (Throwable t) {
        failed = true;
        throw new RuntimeException(t);
      }
    }
  }

  long totMiss, totHit;
  void addResults(long miss, long hit) {
    totMiss += miss;
    totHit += hit;
  }

  public void testThreadCorrectness() throws Exception {
    final int NUM_THREADS = 4;
    final int CACHE_SIZE = 512;
    final int OBJ_COUNT = 3*CACHE_SIZE;

    DoubleBarrelLRUCache<CloneableObject,Object> c = new DoubleBarrelLRUCache<CloneableObject,Object>(1024);

    CloneableObject[] objs = new CloneableObject[OBJ_COUNT];
    for(int i=0;i<OBJ_COUNT;i++) {
      objs[i] = new CloneableObject(new Object());
    }
    
    final CacheThread[] threads = new CacheThread[NUM_THREADS];
    final long endTime = System.currentTimeMillis()+1000L;
    for(int i=0;i<NUM_THREADS;i++) {
      threads[i] = new CacheThread(c, objs, endTime);
      threads[i].start();
    }
    for(int i=0;i<NUM_THREADS;i++) {
      threads[i].join();
      assert !threads[i].failed;
    }
    //System.out.println("hits=" + totHit + " misses=" + totMiss);
  }
  
  private static class CloneableObject extends DoubleBarrelLRUCache.CloneableKey {
    private Object value;

    public CloneableObject(Object value) {
      this.value = value;
    }

    @Override
    public boolean equals(Object other) {
      return this.value.equals(((CloneableObject) other).value);
    }

    @Override
    public int hashCode() {
      return value.hashCode();
    }

    @Override
    public CloneableObject clone() {
      return new CloneableObject(value);
    }
  }

  protected static class CloneableInteger extends DoubleBarrelLRUCache.CloneableKey {
    private Integer value;

    public CloneableInteger(Integer value) {
      this.value = value;
    }

    @Override
    public boolean equals(Object other) {
      return this.value.equals(((CloneableInteger) other).value);
    }

    @Override
    public int hashCode() {
      return value.hashCode();
    }

    @Override
    public CloneableInteger clone() {
      return new CloneableInteger(value);
    }
  }


}
