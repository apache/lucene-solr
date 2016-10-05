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
package org.apache.lucene.codecs.lucene70;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestSparseDISI extends LuceneTestCase {

  public void testRandom() throws IOException {
    try (Directory dir = newDirectory()) {
      for (int i = 0; i < 1000; ++i) {
        doTestRandom(dir);
      }
    }
  }

  private void doTestRandom(Directory dir) throws IOException {
    List<Integer> docs = new ArrayList<>();
    final int maxStep = TestUtil.nextInt(random(), 1, 1 << TestUtil.nextInt(random(), 2, 10));
    final int numDocs = TestUtil.nextInt(random(), 1, 1000);
    for (int doc = -1, i = 0; i < numDocs; ++i) {
      doc += TestUtil.nextInt(random(), 1, maxStep);
      docs.add(doc);
    }
    final int maxDoc = docs.get(docs.size() - 1) + TestUtil.nextInt(random(), 1, 100);

    FixedBitSet set = new FixedBitSet(maxDoc);
    for (int doc : docs) {
      set.set(doc);
    }

    try (IndexOutput out = dir.createOutput("foo", IOContext.DEFAULT)) {
      SparseDISI.writeBitSet(new BitSetIterator(set, docs.size()), maxDoc, out);
    }

    try (IndexInput in = dir.openInput("foo", IOContext.DEFAULT)) {
      SparseDISI disi = new SparseDISI(maxDoc, in, 0L, docs.size());
      BitSetIterator disi2 = new BitSetIterator(set, docs.size());
      int i = 0;
      for (int doc = disi2.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = disi2.nextDoc()) {
        assertEquals(doc, disi.nextDoc());
        assertEquals(i++, disi.index());
      }
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, disi.nextDoc());
    }

    for (int step : new int[] {1, 20, maxStep, maxStep * 10}) {
      try (IndexInput in = dir.openInput("foo", IOContext.DEFAULT)) {
        SparseDISI disi = new SparseDISI(maxDoc, in, 0L, docs.size());
        BitSetIterator disi2 = new BitSetIterator(set, docs.size());
        while (true) {
          int target = disi2.docID() + step;
          int doc = disi2.advance(target);
          assertEquals(doc, disi.advance(target));
          if (doc == DocIdSetIterator.NO_MORE_DOCS) {
            break;
          }
          int index = Collections.binarySearch(docs, doc);
          assertEquals(index, disi.index());
        }
      }
    }

    dir.deleteFile("foo");
  }

}
