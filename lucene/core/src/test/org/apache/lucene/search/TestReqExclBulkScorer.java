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

import org.apache.lucene.util.Bits;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestReqExclBulkScorer extends LuceneTestCase {

  public void testRandom() throws IOException {
    final int iters = atLeast(10);
    for (int iter = 0; iter < iters; ++iter) {
      doTestRandom();
    }
  }

  public void doTestRandom() throws IOException {
    final int maxDoc = TestUtil.nextInt(random(), 1, 1000);
    DocIdSetBuilder reqBuilder = new DocIdSetBuilder(maxDoc);
    DocIdSetBuilder exclBuilder = new DocIdSetBuilder(maxDoc);
    final int numIncludedDocs = TestUtil.nextInt(random(), 1, maxDoc);
    final int numExcludedDocs = TestUtil.nextInt(random(), 1, maxDoc);
    DocIdSetBuilder.BulkAdder reqAdder = reqBuilder.grow(numIncludedDocs);
    for (int i = 0; i < numIncludedDocs; ++i) {
      reqAdder.add(random().nextInt(maxDoc));
    }
    DocIdSetBuilder.BulkAdder exclAdder = exclBuilder.grow(numExcludedDocs);
    for (int i = 0; i < numExcludedDocs; ++i) {
      exclAdder.add(random().nextInt(maxDoc));
    }

    final DocIdSet req = reqBuilder.build();
    final DocIdSet excl = exclBuilder.build();

    final BulkScorer reqBulkScorer = new BulkScorer() {
      final DocIdSetIterator iterator = req.iterator();

      @Override
      public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
        int doc = iterator.docID();
        if (iterator.docID() < min) {
          doc = iterator.advance(min);
        }
        while (doc < max) {
          if (acceptDocs == null || acceptDocs.get(doc)) {
            collector.collect(doc);
          }
          doc = iterator.nextDoc();
        }
        return doc;
      }

      @Override
      public long cost() {
        return iterator.cost();
      }
    };

    ReqExclBulkScorer reqExcl = new ReqExclBulkScorer(reqBulkScorer, excl.iterator());
    final FixedBitSet actualMatches = new FixedBitSet(maxDoc);
    if (random().nextBoolean()) {
      reqExcl.score(new LeafCollector() {
        @Override
        public void setScorer(Scorer scorer) throws IOException {}
        
        @Override
        public void collect(int doc) throws IOException {
          actualMatches.set(doc);
        }
      }, null);
    } else {
      int next = 0;
      while (next < maxDoc) {
        final int min = next;
        final int max = min + random().nextInt(10);
        next = reqExcl.score(new LeafCollector() {
          @Override
          public void setScorer(Scorer scorer) throws IOException {}
          
          @Override
          public void collect(int doc) throws IOException {
            actualMatches.set(doc);
          }
        }, null, min, max);
        assertTrue(next >= max);
      }
    }

    final FixedBitSet expectedMatches = new FixedBitSet(maxDoc);
    expectedMatches.or(req.iterator());
    FixedBitSet excludedSet = new FixedBitSet(maxDoc);
    excludedSet.or(excl.iterator());
    expectedMatches.andNot(excludedSet);

    assertArrayEquals(expectedMatches.getBits(), actualMatches.getBits());
  }

}
