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
package org.apache.lucene.search;


import java.io.IOException;

import org.apache.lucene.util.LuceneTestCase;

public class TestCachingCollector extends LuceneTestCase {

  private static final double ONE_BYTE = 1.0 / (1024 * 1024); // 1 byte out of MB
  
  private static class MockScorer extends Scorer {
    
    private MockScorer() {
      super((Weight) null);
    }
    
    @Override
    public float score() throws IOException { return 0; }
    
    @Override
    public int freq() throws IOException { return 0; }

    @Override
    public int docID() { return 0; }

    @Override
    public DocIdSetIterator iterator() {
      throw new UnsupportedOperationException();
    }
  }
  
  private static class NoOpCollector extends SimpleCollector {

    @Override
    public void collect(int doc) throws IOException {}
    
    @Override
    public boolean needsScores() {
      return false;
    }

  }

  public void testBasic() throws Exception {
    for (boolean cacheScores : new boolean[] { false, true }) {
      CachingCollector cc = CachingCollector.create(new NoOpCollector(), cacheScores, 1.0);
      LeafCollector acc = cc.getLeafCollector(null);
      acc.setScorer(new MockScorer());

      // collect 1000 docs
      for (int i = 0; i < 1000; i++) {
        acc.collect(i);
      }

      // now replay them
      cc.replay(new SimpleCollector() {
        int prevDocID = -1;

        @Override
        public void collect(int doc) {
          assertEquals(prevDocID + 1, doc);
          prevDocID = doc;
        }
        
        @Override
        public boolean needsScores() {
          return false;
        }
      });
    }
  }
  
  public void testIllegalStateOnReplay() throws Exception {
    CachingCollector cc = CachingCollector.create(new NoOpCollector(), true, 50 * ONE_BYTE);
    LeafCollector acc = cc.getLeafCollector(null);
    acc.setScorer(new MockScorer());
    
    // collect 130 docs, this should be enough for triggering cache abort.
    for (int i = 0; i < 130; i++) {
      acc.collect(i);
    }
    
    assertFalse("CachingCollector should not be cached due to low memory limit", cc.isCached());
    
    try {
      cc.replay(new NoOpCollector());
      fail("replay should fail if CachingCollector is not cached");
    } catch (IllegalStateException e) {
      // expected
    }
  }
  
  public void testCachedArraysAllocation() throws Exception {
    // tests the cached arrays allocation -- if the 'nextLength' was too high,
    // caching would terminate even if a smaller length would suffice.
    
    // set RAM limit enough for 150 docs + random(10000)
    int numDocs = random().nextInt(10000) + 150;
    for (boolean cacheScores : new boolean[] { false, true }) {
      int bytesPerDoc = cacheScores ? 8 : 4;
      CachingCollector cc = CachingCollector.create(new NoOpCollector(),
          cacheScores, bytesPerDoc * ONE_BYTE * numDocs);
      LeafCollector acc = cc.getLeafCollector(null);
      acc.setScorer(new MockScorer());
      for (int i = 0; i < numDocs; i++) acc.collect(i);
      assertTrue(cc.isCached());

      // The 151's document should terminate caching
      acc.collect(numDocs);
      assertFalse(cc.isCached());
    }
  }

  public void testNoWrappedCollector() throws Exception {
    for (boolean cacheScores : new boolean[] { false, true }) {
      // create w/ null wrapped collector, and test that the methods work
      CachingCollector cc = CachingCollector.create(cacheScores, 50 * ONE_BYTE);
      LeafCollector acc = cc.getLeafCollector(null);
      acc.setScorer(new MockScorer());
      acc.collect(0);
      
      assertTrue(cc.isCached());
      cc.replay(new NoOpCollector());
    }
  }
  
}
