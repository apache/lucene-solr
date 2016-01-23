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
    assertEquals(null, new DocIdSetBuilder(1 + random().nextInt(1000)).build());
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

  public void testSparse() throws IOException {
    final int maxDoc = 1000000 + random().nextInt(1000000);
    DocIdSetBuilder builder = new DocIdSetBuilder(maxDoc);
    final int numIterators = 1 + random().nextInt(10);
    final FixedBitSet ref = new FixedBitSet(maxDoc);
    for (int i = 0; i < numIterators; ++i) {
      final int baseInc = 200000 + random().nextInt(10000);
      RoaringDocIdSet.Builder b = new RoaringDocIdSet.Builder(maxDoc);
      for (int doc = random().nextInt(100); doc < maxDoc; doc += baseInc + random().nextInt(10000)) {
        b.add(doc);
        ref.set(doc);
      }
      builder.add(b.build().iterator());
    }
    DocIdSet result = builder.build();
    assertTrue(result instanceof IntArrayDocIdSet);
    assertEquals(new BitDocIdSet(ref), result);
  }

  public void testDense() throws IOException {
    final int maxDoc = 1000000 + random().nextInt(1000000);
    DocIdSetBuilder builder = new DocIdSetBuilder(maxDoc);
    final int numIterators = 1 + random().nextInt(10);
    final FixedBitSet ref = new FixedBitSet(maxDoc);
    for (int i = 0; i < numIterators; ++i) {
      RoaringDocIdSet.Builder b = new RoaringDocIdSet.Builder(maxDoc);
      for (int doc = random().nextInt(1000); doc < maxDoc; doc += 1 + random().nextInt(100)) {
        b.add(doc);
        ref.set(doc);
      }
      builder.add(b.build().iterator());
    }
    DocIdSet result = builder.build();
    assertTrue(result instanceof BitDocIdSet);
    assertEquals(new BitDocIdSet(ref), result);
  }

  public void testRandom() throws IOException {
    final int maxDoc = TestUtil.nextInt(random(), 1, 10000000);
    for (int i = 1 ; i < maxDoc / 2; i <<=1) {
      final int numDocs = TestUtil.nextInt(random(), 1, i);
      final FixedBitSet docs = new FixedBitSet(maxDoc);
      int c = 0;
      while (c < numDocs) {
        final int d = random().nextInt(maxDoc);
        if (docs.get(d) == false) {
          docs.set(d);
          c += 1;
        }
      }

      final int[] array = new int[numDocs + random().nextInt(100)];
      DocIdSetIterator it = new BitSetIterator(docs, 0L);
      int j = 0;
      for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
        array[j++] = doc;
      }
      assertEquals(numDocs, j);

      // add some duplicates
      while (j < array.length) {
        array[j++] = array[random().nextInt(numDocs)];
      }

      // shuffle
      for (j = array.length - 1; j >= 1; --j) {
        final int k = random().nextInt(j);
        int tmp = array[j];
        array[j] = array[k];
        array[k] = tmp;
      }

      // add docs out of order
      DocIdSetBuilder builder = new DocIdSetBuilder(maxDoc);
      for (j = 0; j < array.length; ) {
        final int l = TestUtil.nextInt(random(), 1, array.length - j);
        if (rarely()) {
          builder.grow(l);
        }
        for (int k = 0; k < l; ++k) {
          builder.add(array[j++]);
        }
      }

      final DocIdSet expected = new BitDocIdSet(docs);
      final DocIdSet actual = builder.build();
      assertEquals(expected, actual);
    }
  }

  public void testMisleadingDISICost() throws IOException {
    final int maxDoc = TestUtil.nextInt(random(), 1000, 10000);
    DocIdSetBuilder builder = new DocIdSetBuilder(maxDoc);
    FixedBitSet expected = new FixedBitSet(maxDoc);

    for (int i = 0; i < 10; ++i) {
      final FixedBitSet docs = new FixedBitSet(maxDoc);
      final int numDocs = random().nextInt(maxDoc / 1000);
      for (int j = 0; j < numDocs; ++j) {
        docs.set(random().nextInt(maxDoc));
      }
      expected.or(docs);
      // We provide a cost of 0 here to make sure the builder can deal with wrong costs
      builder.add(new BitSetIterator(docs, 0L));
    }

    assertEquals(new BitDocIdSet(expected), builder.build());
  }

}
