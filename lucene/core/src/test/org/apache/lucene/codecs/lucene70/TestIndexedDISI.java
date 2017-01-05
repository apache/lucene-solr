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

public class TestIndexedDISI extends LuceneTestCase {

  public void testEmpty() throws IOException {
    int maxDoc = TestUtil.nextInt(random(), 1, 100000);
    FixedBitSet set = new FixedBitSet(maxDoc);
    try (Directory dir = newDirectory()) {
      doTest(set, dir);
    }
  }

  public void testOneDoc() throws IOException {
    int maxDoc = TestUtil.nextInt(random(), 1, 100000);
    FixedBitSet set = new FixedBitSet(maxDoc);
    set.set(random().nextInt(maxDoc));
    try (Directory dir = newDirectory()) {
      doTest(set, dir);
    }
  }

  public void testTwoDocs() throws IOException {
    int maxDoc = TestUtil.nextInt(random(), 1, 100000);
    FixedBitSet set = new FixedBitSet(maxDoc);
    set.set(random().nextInt(maxDoc));
    set.set(random().nextInt(maxDoc));
    try (Directory dir = newDirectory()) {
      doTest(set, dir);
    }
  }

  public void testAllDocs() throws IOException {
    int maxDoc = TestUtil.nextInt(random(), 1, 100000);
    FixedBitSet set = new FixedBitSet(maxDoc);
    set.set(1, maxDoc);
    try (Directory dir = newDirectory()) {
      doTest(set, dir);
    }
  }

  public void testHalfFull() throws IOException {
    int maxDoc = TestUtil.nextInt(random(), 1, 100000);
    FixedBitSet set = new FixedBitSet(maxDoc);
    for (int i = random().nextInt(2); i < maxDoc; i += TestUtil.nextInt(random(), 1, 3)) {
      set.set(i);
    }
    try (Directory dir = newDirectory()) {
      doTest(set, dir);
    }
  }

  public void testDocRange() throws IOException {
    try (Directory dir = newDirectory()) {
      for (int iter = 0; iter < 10; ++iter) {
        int maxDoc = TestUtil.nextInt(random(), 1, 1000000);
        FixedBitSet set = new FixedBitSet(maxDoc);
        final int start = random().nextInt(maxDoc);
        final int end = TestUtil.nextInt(random(), start + 1, maxDoc);
        set.set(start, end);
        doTest(set, dir);
      }
    }
  }

  public void testSparseDenseBoundary() throws IOException {
    try (Directory dir = newDirectory()) {
      FixedBitSet set = new FixedBitSet(200000);
      int start = 65536 + random().nextInt(100);

      // we set MAX_ARRAY_LENGTH bits so the encoding will be sparse
      set.set(start, start + IndexedDISI.MAX_ARRAY_LENGTH);
      long length;
      try (IndexOutput out = dir.createOutput("sparse", IOContext.DEFAULT)) {
        IndexedDISI.writeBitSet(new BitSetIterator(set, IndexedDISI.MAX_ARRAY_LENGTH), out);
        length = out.getFilePointer();
      }
      try (IndexInput in = dir.openInput("sparse", IOContext.DEFAULT)) {
        IndexedDISI disi = new IndexedDISI(in, 0L, length, IndexedDISI.MAX_ARRAY_LENGTH);
        assertEquals(start, disi.nextDoc());
        assertEquals(IndexedDISI.Method.SPARSE, disi.method);
      }
      doTest(set, dir);

      // now we set one more bit so the encoding will be dense
      set.set(start + IndexedDISI.MAX_ARRAY_LENGTH + random().nextInt(100));
      try (IndexOutput out = dir.createOutput("bar", IOContext.DEFAULT)) {
        IndexedDISI.writeBitSet(new BitSetIterator(set, IndexedDISI.MAX_ARRAY_LENGTH + 1), out);
        length = out.getFilePointer();
      }
      try (IndexInput in = dir.openInput("bar", IOContext.DEFAULT)) {
        IndexedDISI disi = new IndexedDISI(in, 0L, length, IndexedDISI.MAX_ARRAY_LENGTH + 1);
        assertEquals(start, disi.nextDoc());
        assertEquals(IndexedDISI.Method.DENSE, disi.method);
      }
      doTest(set, dir);
    }
  }

  public void testOneDocMissing() throws IOException {
    int maxDoc = TestUtil.nextInt(random(), 1, 1000000);
    FixedBitSet set = new FixedBitSet(maxDoc);
    set.set(0, maxDoc);
    set.clear(random().nextInt(maxDoc));
    try (Directory dir = newDirectory()) {
      doTest(set, dir);
    }
  }

  public void testFewMissingDocs() throws IOException {
    try (Directory dir = newDirectory()) {
      for (int iter = 0; iter < 100; ++iter) {
        int maxDoc = TestUtil.nextInt(random(), 1, 100000);
        FixedBitSet set = new FixedBitSet(maxDoc);
        set.set(0, maxDoc);
        final int numMissingDocs = TestUtil.nextInt(random(), 2, 1000);
        for (int i = 0; i < numMissingDocs; ++i) {
          set.clear(random().nextInt(maxDoc));
        }
        doTest(set, dir);
      }
    }
  }

  public void testRandom() throws IOException {
    try (Directory dir = newDirectory()) {
      for (int i = 0; i < 10; ++i) {
        doTestRandom(dir);
      }
    }
  }

  private void doTestRandom(Directory dir) throws IOException {
    List<Integer> docs = new ArrayList<>();
    final int maxStep = TestUtil.nextInt(random(), 1, 1 << TestUtil.nextInt(random(), 2, 20));
    final int numDocs = TestUtil.nextInt(random(), 1, Math.min(100000, Integer.MAX_VALUE / maxStep));
    for (int doc = -1, i = 0; i < numDocs; ++i) {
      doc += TestUtil.nextInt(random(), 1, maxStep);
      docs.add(doc);
    }
    final int maxDoc = docs.get(docs.size() - 1) + TestUtil.nextInt(random(), 1, 100);

    FixedBitSet set = new FixedBitSet(maxDoc);
    for (int doc : docs) {
      set.set(doc);
    }

    doTest(set, dir);
  }

  private void doTest(FixedBitSet set, Directory dir) throws IOException {
    final int cardinality = set.cardinality();
    long length;
    try (IndexOutput out = dir.createOutput("foo", IOContext.DEFAULT)) {
      IndexedDISI.writeBitSet(new BitSetIterator(set, cardinality), out);
      length = out.getFilePointer();
    }

    try (IndexInput in = dir.openInput("foo", IOContext.DEFAULT)) {
      IndexedDISI disi = new IndexedDISI(in, 0L, length, cardinality);
      BitSetIterator disi2 = new BitSetIterator(set, cardinality);
      int i = 0;
      for (int doc = disi2.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = disi2.nextDoc()) {
        assertEquals(doc, disi.nextDoc());
        assertEquals(i++, disi.index());
      }
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, disi.nextDoc());
    }

    for (int step : new int[] {1, 10, 100, 1000, 10000, 100000}) {
      try (IndexInput in = dir.openInput("foo", IOContext.DEFAULT)) {
        IndexedDISI disi = new IndexedDISI(in, 0L, length, cardinality);
        BitSetIterator disi2 = new BitSetIterator(set, cardinality);
        int index = -1;
        while (true) {
          int target = disi2.docID() + step;
          int doc;
          do {
            doc = disi2.nextDoc();
            index++;
          } while (doc < target);
          assertEquals(doc, disi.advance(target));
          if (doc == DocIdSetIterator.NO_MORE_DOCS) {
            break;
          }
          assertEquals(index, disi.index());
        }
      }
    }

    for (int step : new int[] {10, 100, 1000, 10000, 100000}) {
      try (IndexInput in = dir.openInput("foo", IOContext.DEFAULT)) {
        IndexedDISI disi = new IndexedDISI(in, 0L, length, cardinality);
        BitSetIterator disi2 = new BitSetIterator(set, cardinality);
        int index = -1;
        for (int target = 0; target < set.length(); ) {
          target += TestUtil.nextInt(random(), 0, step);
          int doc = disi2.docID();
          while (doc < target) {
            doc = disi2.nextDoc();
            index++;
          }

          boolean exists = disi.advanceExact(target);
          assertEquals(doc == target, exists);
          if (exists) {
            assertEquals(index, disi.index());
          } else if (random().nextBoolean()) {
            assertEquals(doc, disi.nextDoc());
            assertEquals(index, disi.index());
            target = doc;
          }
        }
      }
    }

    dir.deleteFile("foo");
  }

}
