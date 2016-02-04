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
package org.apache.lucene.util;


import java.io.IOException;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;

public class TestBitDocIdSetBuilder extends LuceneTestCase {

  private static DocIdSet randomSet(int maxDoc, int numDocs) {
    FixedBitSet set = new FixedBitSet(maxDoc);
    for (int i = 0; i < numDocs; ++i) {
      while (true) {
        final int docID = random().nextInt(maxDoc);
        if (set.get(docID) == false) {
          set.set(docID);
          break;
        }
      }
    }
    return new BitDocIdSet(set);
  }

  private void assertEquals(DocIdSet set1, DocIdSet set2) throws IOException {
    DocIdSetIterator it1 = set1.iterator();
    DocIdSetIterator it2 = set2.iterator();
    for (int doc = it1.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it1.nextDoc()) {
      assertEquals(doc, it2.nextDoc());
    }
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, it2.nextDoc());
  }

  public void testOrDense() throws IOException {
    final int maxDoc = TestUtil.nextInt(random(), 10000, 100000);
    BitDocIdSet.Builder builder = new BitDocIdSet.Builder(maxDoc);
    FixedBitSet set = new FixedBitSet(maxDoc);
    DocIdSet other = randomSet(maxDoc, maxDoc / 2);
    builder.or(other.iterator());
    set.or(other.iterator());
    assertTrue(builder.dense());
    other = randomSet(maxDoc, maxDoc / 2);
    builder.or(other.iterator());
    set.or(other.iterator());
    assertEquals(new BitDocIdSet(set), builder.build());
  }

  public void testOrSparse() throws IOException {
    final int maxDoc = TestUtil.nextInt(random(), 10000, 100000);
    BitDocIdSet.Builder builder = new BitDocIdSet.Builder(maxDoc);
    FixedBitSet set = new FixedBitSet(maxDoc);
    DocIdSet other = randomSet(maxDoc, maxDoc / 5000);
    builder.or(other.iterator());
    set.or(other.iterator());
    assertFalse(builder.dense());
    other = randomSet(maxDoc, maxDoc / 5000);
    builder.or(other.iterator());
    set.or(other.iterator());
    assertEquals(new BitDocIdSet(set), builder.build());
  }
  
  public void testAndDense() throws IOException {
    final int maxDoc = TestUtil.nextInt(random(), 10000, 100000);
    BitDocIdSet.Builder builder = new BitDocIdSet.Builder(maxDoc);
    FixedBitSet set = new FixedBitSet(maxDoc);
    DocIdSet other = randomSet(maxDoc, maxDoc / 2);
    builder.or(other.iterator());
    set.or(other.iterator());
    assertTrue(builder.dense());
    other = randomSet(maxDoc, maxDoc / 2);
    builder.and(other.iterator());
    set.and(other.iterator());
    assertEquals(new BitDocIdSet(set), builder.build());
  }
  
  public void testAndSparse() throws IOException {
    final int maxDoc = TestUtil.nextInt(random(), 10000, 100000);
    BitDocIdSet.Builder builder = new BitDocIdSet.Builder(maxDoc);
    FixedBitSet set = new FixedBitSet(maxDoc);
    DocIdSet other = randomSet(maxDoc, maxDoc / 2000);
    builder.or(other.iterator());
    set.or(other.iterator());
    assertFalse(builder.dense());
    other = randomSet(maxDoc, maxDoc / 2);
    builder.and(other.iterator());
    set.and(other.iterator());
    assertEquals(new BitDocIdSet(set), builder.build());
  }
  
  public void testAndNotDense() throws IOException {
    final int maxDoc = TestUtil.nextInt(random(), 10000, 100000);
    BitDocIdSet.Builder builder = new BitDocIdSet.Builder(maxDoc);
    FixedBitSet set = new FixedBitSet(maxDoc);
    DocIdSet other = randomSet(maxDoc, maxDoc / 2);
    builder.or(other.iterator());
    set.or(other.iterator());
    assertTrue(builder.dense());
    other = randomSet(maxDoc, maxDoc / 2);
    builder.andNot(other.iterator());
    set.andNot(other.iterator());
    assertEquals(new BitDocIdSet(set), builder.build());
  }
  
  public void testAndNotSparse() throws IOException {
    final int maxDoc = TestUtil.nextInt(random(), 10000, 100000);
    BitDocIdSet.Builder builder = new BitDocIdSet.Builder(maxDoc);
    FixedBitSet set = new FixedBitSet(maxDoc);
    DocIdSet other = randomSet(maxDoc, maxDoc / 2000);
    builder.or(other.iterator());
    set.or(other.iterator());
    assertFalse(builder.dense());
    other = randomSet(maxDoc, maxDoc / 2);
    builder.andNot(other.iterator());
    set.andNot(other.iterator());
    assertEquals(new BitDocIdSet(set), builder.build());
  }
  
}
