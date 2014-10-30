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

import java.io.IOException;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;

public class TestDocIdSetBuilder extends LuceneTestCase {

  public void testEmpty() throws IOException {
    assertEquals(null, new BitDocIdSet.Builder(1 + random().nextInt(1000)).build());
  }

  private void assertEquals(DocIdSet d1, DocIdSet d2) throws IOException {
    if (d1 == null) {
      if (d2 != null) {
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, d2.iterator().nextDoc());
      }
    } else if (d2 == null) {
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, d1.iterator().nextDoc());
    } else {
      DocIdSetIterator i1 = d1.iterator();
      DocIdSetIterator i2 = d2.iterator();
      for (int doc = i1.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = i1.nextDoc()) {
        assertEquals(doc, i2.nextDoc());
      }
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, i2.nextDoc());
    }
  }

  public void testFull() throws IOException {
    final int maxDoc = 1 + random().nextInt(1000);
    BitDocIdSet.Builder builder = new BitDocIdSet.Builder(maxDoc, true);
    DocIdSet set = builder.build();
    DocIdSetIterator it = set.iterator();
    for (int i = 0; i < maxDoc; ++i) {
      assertEquals(i, it.nextDoc());
    }
  }

  public void testSparse() throws IOException {
    final int maxDoc = 1000000 + random().nextInt(1000000);
    BitDocIdSet.Builder builder = new BitDocIdSet.Builder(maxDoc);
    final int numIterators = 1 + random().nextInt(10);
    final FixedBitSet ref = new FixedBitSet(maxDoc);
    for (int i = 0; i < numIterators; ++i) {
      final int baseInc = 200000 + random().nextInt(10000);
      RoaringDocIdSet.Builder b = new RoaringDocIdSet.Builder(maxDoc);
      for (int doc = random().nextInt(100); doc < maxDoc; doc += baseInc + random().nextInt(10000)) {
        b.add(doc);
        ref.set(doc);
      }
      builder.or(b.build().iterator());
    }
    DocIdSet result = builder.build();
    assertTrue(result instanceof BitDocIdSet);
    assertEquals(new BitDocIdSet(ref), result);
  }

  public void testDense() throws IOException {
    final int maxDoc = 1000000 + random().nextInt(1000000);
    BitDocIdSet.Builder builder = new BitDocIdSet.Builder(maxDoc);
    final int numIterators = 1 + random().nextInt(10);
    final FixedBitSet ref = new FixedBitSet(maxDoc);
    if (random().nextBoolean()) {
      // try upgrades
      final int doc = random().nextInt(maxDoc);
      ref.set(doc);
      builder.or(new RoaringDocIdSet.Builder(maxDoc).add(doc).build().iterator());
    }
    for (int i = 0; i < numIterators; ++i) {
      RoaringDocIdSet.Builder b = new RoaringDocIdSet.Builder(maxDoc);
      for (int doc = random().nextInt(1000); doc < maxDoc; doc += 1 + random().nextInt(1000)) {
        b.add(doc);
        ref.set(doc);
      }
      builder.or(b.build().iterator());
    }
    DocIdSet result = builder.build();
    assertTrue(result instanceof BitDocIdSet);
    assertEquals(new BitDocIdSet(ref), result);
  }

}
