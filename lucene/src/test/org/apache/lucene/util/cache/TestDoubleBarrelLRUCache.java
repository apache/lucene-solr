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

public class TestDoubleBarrelLRUCache extends BaseTestLRU {

  public void testLRUCache() throws Exception {
    final int n = 100;
    testCache(new DoubleBarrelLRUCache<Integer,Object>(n), n);
  }

  private class CacheThread extends Thread {
    private final Object[] objs;
    private final Cache<Object,Object> c;
    private final long endTime;
    volatile boolean failed;

    public CacheThread(Cache<Object,Object> c,
                     Object[] objs, long endTime) {
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
          final Object obj = objs[(int) ((count/2) % limit)];
          Object v = c.get(obj);
          if (v == null) {
            c.put(obj, obj);
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

    Cache<Object,Object> c = new DoubleBarrelLRUCache<Object,Object>(1024);

    Object[] objs = new Object[OBJ_COUNT];
    for(int i=0;i<OBJ_COUNT;i++) {
      objs[i] = new Object();
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
  
}
