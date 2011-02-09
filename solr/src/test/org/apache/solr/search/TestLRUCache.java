package org.apache.solr.search;

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

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.common.util.NamedList;

/**
 * Test for <code>org.apache.solr.search.LRUCache</code>
 */
public class TestLRUCache extends LuceneTestCase {

  public void testFullAutowarm() throws IOException {
    LRUCache<Object, Object> lruCache = new LRUCache<Object, Object>();
    Map<String, String> params = new HashMap<String, String>();
    params.put("size", "100");
    params.put("initialSize", "10");
    params.put("autowarmCount", "100%");
    CacheRegenerator cr = createCodeRegenerator();
    Object o = lruCache.init(params, null, cr);
    lruCache.setState(SolrCache.State.LIVE);
    for (int i = 0; i < 101; i++) {
      lruCache.put(i + 1, "" + (i + 1));
    }
    assertEquals("25", lruCache.get(25));
    assertEquals(null, lruCache.get(110));
    assertEquals(null, lruCache.get(1));  // first item put in should be the first out
    LRUCache<Object, Object> lruCacheNew = new LRUCache<Object, Object>();
    lruCacheNew.init(params, o, cr);
    lruCacheNew.warm(null, lruCache);
    lruCacheNew.setState(SolrCache.State.LIVE);
    lruCache.close();
    lruCacheNew.put(103, "103");
    assertEquals("90", lruCacheNew.get(90));
    assertEquals("50", lruCacheNew.get(50));
    lruCacheNew.close();
  }
  
  public void testPercentageAutowarm() throws IOException {
      doTestPercentageAutowarm(100, 50, new int[]{51, 55, 60, 70, 80, 99, 100}, new int[]{1, 2, 3, 5, 10, 20, 30, 40, 50});
      doTestPercentageAutowarm(100, 25, new int[]{76, 80, 99, 100}, new int[]{1, 2, 3, 5, 10, 20, 30, 40, 50, 51, 55, 60, 70});
      doTestPercentageAutowarm(1000, 10, new int[]{901, 930, 950, 999, 1000}, new int[]{1, 5, 100, 200, 300, 400, 800, 899, 900});
      doTestPercentageAutowarm(10, 10, new int[]{10}, new int[]{1, 5, 9, 100, 200, 300, 400, 800, 899, 900});
  }
  
  private void doTestPercentageAutowarm(int limit, int percentage, int[] hits, int[]misses) throws IOException {
    LRUCache<Object, Object> lruCache = new LRUCache<Object, Object>();
    Map<String, String> params = new HashMap<String, String>();
    params.put("size", String.valueOf(limit));
    params.put("initialSize", "10");
    params.put("autowarmCount", percentage + "%");
    CacheRegenerator cr = createCodeRegenerator();
    Object o = lruCache.init(params, null, cr);
    lruCache.setState(SolrCache.State.LIVE);
    for (int i = 1; i <= limit; i++) {
      lruCache.put(i, "" + i);//adds numbers from 1 to 100
    }

    LRUCache<Object, Object> lruCacheNew = new LRUCache<Object, Object>();
    lruCacheNew.init(params, o, cr);
    lruCacheNew.warm(null, lruCache);
    lruCacheNew.setState(SolrCache.State.LIVE);
    lruCache.close();
      
    for(int hit:hits) {
      assertEquals("The value " + hit + " should be on new cache", String.valueOf(hit), lruCacheNew.get(hit));
    }
      
    for(int miss:misses) {
      assertEquals("The value " + miss + " should NOT be on new cache", null, lruCacheNew.get(miss));
    }
    lruCacheNew.close();
  }
  
  @SuppressWarnings("unchecked")
  public void testNoAutowarm() throws IOException {
    LRUCache<Object, Object> lruCache = new LRUCache<Object, Object>();
    Map<String, String> params = new HashMap<String, String>();
    params.put("size", "100");
    params.put("initialSize", "10");
    CacheRegenerator cr = createCodeRegenerator();
    Object o = lruCache.init(params, null, cr);
    lruCache.setState(SolrCache.State.LIVE);
    for (int i = 0; i < 101; i++) {
      lruCache.put(i + 1, "" + (i + 1));
    }
    assertEquals("25", lruCache.get(25));
    assertEquals(null, lruCache.get(110));
    NamedList<Serializable> nl = lruCache.getStatistics();
    assertEquals(2L, nl.get("lookups"));
    assertEquals(1L, nl.get("hits"));
    assertEquals(101L, nl.get("inserts"));
    assertEquals(null, lruCache.get(1));  // first item put in should be the first out
    LRUCache<Object, Object> lruCacheNew = new LRUCache<Object, Object>();
    lruCacheNew.init(params, o, cr);
    lruCacheNew.warm(null, lruCache);
    lruCacheNew.setState(SolrCache.State.LIVE);
    lruCache.close();
    lruCacheNew.put(103, "103");
    assertEquals(null, lruCacheNew.get(90));
    assertEquals(null, lruCacheNew.get(50));
    lruCacheNew.close();
  }
  
  private CacheRegenerator createCodeRegenerator() {
    CacheRegenerator cr = new CacheRegenerator() {
      @SuppressWarnings("unchecked")
      public boolean regenerateItem(SolrIndexSearcher newSearcher, SolrCache newCache,
                                    SolrCache oldCache, Object oldKey, Object oldVal) throws IOException {
        newCache.put(oldKey, oldVal);
        return true;
      }
    };
    return cr;
  }
}
