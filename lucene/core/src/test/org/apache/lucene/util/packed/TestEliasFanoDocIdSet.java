package org.apache.lucene.util.packed;

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

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.LuceneTestCase;

public class TestEliasFanoDocIdSet extends LuceneTestCase {
  private static DocIdSetIterator makeDisi(final int[] docIds) {
    class IntArrayDisi extends DocIdSetIterator {
      int i = 0;
      int docId = -1;

      @Override
      public int docID() {
        return docId;
      }

      @Override
      public int nextDoc() {
        if (i >= docIds.length) {
          docId = NO_MORE_DOCS;
          return docId;
        }
        if (docIds[i] < docId) { // Elias-Fano sequence should be non decreasing.
          // The non decreasing condition for Elias-Fano is weaker than normal increasing for DocIdSetIterator
          throw new AssertionError("docIds[] out of order");
        }
        docId = docIds[i++]; // increase i to just after current
        return docId;
      }

      @Override
      public int advance(int target) {
        // ( ((i == 0) and (docId == -1)) or
        //   ((i > 0) and (docIds.length > 0) and (i <= docIds.length) and (docId == docIds[i-1])) )

        // The behavior of this method is undefined when called with target ≤ current, or after the iterator has exhausted.
        // Both cases may result in unpredicted behavior, and may throw an assertion error or an IOOBE here.
        // So when nextDoc() or advance() were called earlier, the target should be bigger than current docId:
        assert (docId == -1) || (docId < target);


        // Do a binary search for the index j for which:
        // ((j >= i)
        //  and ((j < docIds.length) implies (docIds[j] >= target))
        //  and ((j >= 1) implies (docIds[j-1] < target)) )
        int j = docIds.length;
        while (i < j) {
          // ((0 <= i) and (i < j) and (j <= docIds.length)) so (docIds.length > 0)
          int m = i + (j - i) / 2; // (i <= m) and (m < j); avoid overflow for (i + j)
          if (docIds[m] < target) {
            i = m + 1; // (docIds[i-1] <  target) and (i <= j)
          } else {
            j = m; //     (docIds[j] >= target)   and (i <= j)
          }
        } // (i == j)
        docId = (i >= docIds.length)
            ? NO_MORE_DOCS // exhausted
                : docIds[i++]; // increase i to just after current
        return docId;
      }

      @Override
      public long cost() {
        return docIds.length;
      }
    };
    return new IntArrayDisi();
  }

  public void tstEqualDisisNext(DocIdSetIterator disi0, DocIdSetIterator disi1) throws IOException {
    assertEquals(disi0.docID(), disi1.docID());
    int d0 = disi0.nextDoc();
    int d1 = disi1.nextDoc();
    int i = 0;
    while ((d0 != DocIdSetIterator.NO_MORE_DOCS) && (d1 != DocIdSetIterator.NO_MORE_DOCS)) {
      assertEquals("index " + i, d0, d1);
      i++;
      d0 = disi0.nextDoc();
      d1 = disi1.nextDoc();
    }
    assertEquals("at end", d0, d1);
  }

  public void tstEqualDisisAdvanceAsNext(DocIdSetIterator disi0, DocIdSetIterator disi1) throws IOException {
    assertEquals(disi0.docID(), disi1.docID());
    int d0 = disi0.advance(0);
    int d1 = disi1.advance(0);
    int i = 0;
    while ((d0 != DocIdSetIterator.NO_MORE_DOCS) && (d1 != DocIdSetIterator.NO_MORE_DOCS)) {
      assertEquals("index " + i, d0, d1);
      i++;
      d0 = disi0.advance(d1+1);
      d1 = disi1.advance(d1+1);
    }
    assertEquals("at end disi0 " + disi0 + ", disi1 " + disi1, d0, d1);
  }

  public void tstEF(int[] docIds) {
    int maxDoc = -1;
    for (int docId: docIds) {
      assert docId >= maxDoc; // non decreasing
      maxDoc = docId;
    }
    try {
      EliasFanoDocIdSet efd = new EliasFanoDocIdSet(docIds.length, maxDoc);
      efd.encodeFromDisi(makeDisi(docIds));
      tstEqualDisisNext(         makeDisi(docIds), efd.iterator());
      tstEqualDisisAdvanceAsNext(makeDisi(docIds), efd.iterator());
    } catch (IOException ioe) {
      throw new Error(ioe);
    }
  }

  public void testEmpty() { tstEF(new int[] {}); }

  public void testOneElementZero() { tstEF(new int[] {0}); }

  public void testTwoElements() { tstEF(new int[] {0,1}); }

  public void testOneElementOneBit() {
    for (int i = 0; i < (Integer.SIZE-1); i++) {
      tstEF(new int[] {1 << i});
    }
  }

  public void testIncreasingSequences() {
    final int TEST_NUMDOCS = 129;
    int[] docIds = new int[TEST_NUMDOCS];
    for (int f = 1; f <= 1025; f++) {
      for (int i = 0; i < TEST_NUMDOCS; i++) {
        docIds[i] = i*f;
      }
      tstEF(docIds);
    }
  }
}

