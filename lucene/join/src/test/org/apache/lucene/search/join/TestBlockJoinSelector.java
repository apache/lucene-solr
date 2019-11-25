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
package org.apache.lucene.search.join;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LuceneTestCase;

public class TestBlockJoinSelector extends LuceneTestCase {

  public void testDocsWithValue() {
    final BitSet parents = new FixedBitSet(20);
    parents.set(0);
    parents.set(5);
    parents.set(6);
    parents.set(10);
    parents.set(15);
    parents.set(19);

    final BitSet children = new FixedBitSet(20);
    children.set(2);
    children.set(3);
    children.set(4);
    children.set(12);
    children.set(17);

    final BitSet childDocsWithValue = new FixedBitSet(20);
    childDocsWithValue.set(2);
    childDocsWithValue.set(3);
    childDocsWithValue.set(4);
    childDocsWithValue.set(8);
    childDocsWithValue.set(16);

    final Bits docsWithValue = BlockJoinSelector.wrap(childDocsWithValue, parents, children);
    assertFalse(docsWithValue.get(0));
    assertTrue(docsWithValue.get(5));
    assertFalse(docsWithValue.get(6));
    assertFalse(docsWithValue.get(10));
    assertFalse(docsWithValue.get(15));
    assertFalse(docsWithValue.get(19));
  }

  static void assertNoMoreDoc(DocIdSetIterator sdv, int maxDoc) throws IOException{
    Random r = random();
    if(r.nextBoolean()){
      assertEquals(NO_MORE_DOCS, sdv.nextDoc());
    } else {
      if (r.nextBoolean()) {
        assertEquals(NO_MORE_DOCS,  sdv.advance(sdv.docID()+random().nextInt(maxDoc-sdv.docID())));
      } else {
        final int noMatchDoc = sdv.docID()+random().nextInt(maxDoc-sdv.docID()-1)+1;
        assertFalse(advanceExact(sdv,noMatchDoc));
        assertEquals(noMatchDoc, sdv.docID());
        if (r.nextBoolean()){
          assertEquals(NO_MORE_DOCS, sdv.nextDoc());
        }
      }
    }
  }
  
  static int nextDoc(DocIdSetIterator sdv, int docId) throws IOException{
    Random r = random();
    if(r.nextBoolean()){
      return sdv.nextDoc();
    } else {
      if (r.nextBoolean()) {
        return sdv.advance(sdv.docID()+random().nextInt(docId-sdv.docID()-1)+1);
      } else {
        if (r.nextBoolean()){
          final int noMatchDoc = sdv.docID()+random().nextInt(docId-sdv.docID()-1)+1;
          assertFalse(advanceExact(sdv,noMatchDoc));
          assertEquals(noMatchDoc, sdv.docID());
        }
        assertTrue(advanceExact(sdv,docId));
        return sdv.docID();
      }
    }
  }
  
  private static boolean advanceExact(DocIdSetIterator sdv, int target) throws IOException {
    return sdv instanceof SortedDocValues ? ((SortedDocValues) sdv).advanceExact(target)
        : ((NumericDocValues) sdv).advanceExact(target);
  }

  @SuppressWarnings("deprecation")
  public void testSortedSelector() throws IOException {
    final BitSet parents = new FixedBitSet(20);
    parents.set(0);
    parents.set(5);
    parents.set(6);
    parents.set(10);
    parents.set(15);
    parents.set(19);

    final BitSet children = new FixedBitSet(20);
    children.set(2);
    children.set(3);
    children.set(4);
    children.set(12);
    children.set(17);

    final int[] ords = new int[20];
    Arrays.fill(ords, -1);
    ords[2] = 5;
    ords[3] = 7;
    ords[4] = 3;
    ords[12] = 10;
    ords[18] = 10;

    final SortedDocValues mins = BlockJoinSelector.wrap(DocValues.singleton(new CannedSortedDocValues(ords)), BlockJoinSelector.Type.MIN, parents, children);
    assertEquals(5, nextDoc(mins,5));
    assertEquals(3, mins.ordValue());
    assertEquals(15, nextDoc(mins,15));
    assertEquals(10, mins.ordValue());
    assertNoMoreDoc(mins, 20);

    final SortedDocValues maxs = BlockJoinSelector.wrap(DocValues.singleton(new CannedSortedDocValues(ords)), BlockJoinSelector.Type.MAX, parents, children);
    assertEquals(5, nextDoc(maxs,5));
    assertEquals(7, maxs.ordValue());
    assertEquals(15, nextDoc(maxs,15));
    assertEquals(10, maxs.ordValue());
    assertNoMoreDoc( maxs,20);
  }

  private static class CannedSortedDocValues extends SortedDocValues {
    private final int[] ords;
    int docID = -1;

    public CannedSortedDocValues(int[] ords) {
      this.ords = ords;
    }


    @Override
    public int docID() {
      return docID;
    }

    @Override
    public int nextDoc() {
      while (true) {
        docID++;
        if (docID == ords.length) {
          docID = NO_MORE_DOCS;
          break;
        }
        if (ords[docID] != -1) {
          break;
        }
      }
      return docID;
    }

    @Override
    public int advance(int target) {
      if (target >= ords.length) {
        docID = NO_MORE_DOCS;
      } else {
        docID = target;
        if (ords[docID] == -1) {
          nextDoc();
        }
      }
      return docID;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      docID = target;
      return ords[docID] != -1;
    }

    @Override
    public int ordValue() {
      assert ords[docID] != -1;
      return ords[docID];
    }

    @Override
    public long cost() {
      return 5;
    }
        
    @Override
    public BytesRef lookupOrd(int ord) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getValueCount() {
      return 11;
    }
  }

  @SuppressWarnings("deprecation")
  public void testNumericSelector() throws Exception {
    final BitSet parents = new FixedBitSet(20);
    parents.set(0);
    parents.set(5);
    parents.set(6);
    parents.set(10);
    parents.set(15);
    parents.set(19);

    final BitSet children = new FixedBitSet(20);
    children.set(2);
    children.set(3);
    children.set(4);
    children.set(12);
    children.set(17);

    final long[] longs = new long[20];
    final BitSet docsWithValue = new FixedBitSet(20);
    docsWithValue.set(2);
    longs[2] = 5;
    docsWithValue.set(3);
    longs[3] = 7;
    docsWithValue.set(4);
    longs[4] = 3;
    docsWithValue.set(12);
    longs[12] = 10;
    docsWithValue.set(18);
    longs[18] = 10;

    final NumericDocValues mins = BlockJoinSelector.wrap(DocValues.singleton(new CannedNumericDocValues(longs, docsWithValue)), BlockJoinSelector.Type.MIN, parents, children);
    assertEquals(5, nextDoc(mins,5));
    assertEquals(3, mins.longValue());
    assertEquals(15, nextDoc(mins,15));
    assertEquals(10, mins.longValue());
    assertNoMoreDoc(mins, 20);

    final NumericDocValues maxs = BlockJoinSelector.wrap(DocValues.singleton(new CannedNumericDocValues(longs, docsWithValue)), BlockJoinSelector.Type.MAX, parents, children);
    assertEquals(5, nextDoc(maxs, 5));
    assertEquals(7, maxs.longValue());
    assertEquals(15, nextDoc(maxs, 15));
    assertEquals(10, maxs.longValue());
    assertNoMoreDoc(maxs, 20);
  }

  private static class CannedNumericDocValues extends NumericDocValues {
    final Bits docsWithValue;
    final long[] values;
    int docID = -1;

    public CannedNumericDocValues(long[] values, Bits docsWithValue) {
      this.values = values;
      this.docsWithValue = docsWithValue;
    }

    @Override
    public int docID() {
      return docID;
    }

    @Override
    public int nextDoc() {
      while (true) {
        docID++;
        if (docID == values.length) {
          docID = NO_MORE_DOCS;
          break;
        }
        if (docsWithValue.get(docID)) {
          break;
        }
      }
      return docID;
    }

    @Override
    public int advance(int target) {
      if (target >= values.length) {
        docID = NO_MORE_DOCS;
        return docID;
      } else {
        docID = target - 1;
        return nextDoc();
      }
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      docID = target;
      return docsWithValue.get(docID);
    }

    @Override
    public long longValue() {
      return values[docID];
    }

    @Override
    public long cost() {
      return 5;
    }
  }
}
