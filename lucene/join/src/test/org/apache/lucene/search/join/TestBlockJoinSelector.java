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

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LuceneTestCase;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

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
    assertEquals(5, mins.nextDoc());
    assertEquals(3, mins.ordValue());
    assertEquals(15, mins.nextDoc());
    assertEquals(10, mins.ordValue());
    assertEquals(19, mins.nextDoc());
    assertEquals(10, mins.ordValue());
    assertEquals(NO_MORE_DOCS, mins.nextDoc());

    final SortedDocValues maxs = BlockJoinSelector.wrap(DocValues.singleton(new CannedSortedDocValues(ords)), BlockJoinSelector.Type.MAX, parents, children);
    assertEquals(5, maxs.nextDoc());
    assertEquals(7, maxs.ordValue());
    assertEquals(15, maxs.nextDoc());
    assertEquals(10, maxs.ordValue());
    assertEquals(19, maxs.nextDoc());
    assertEquals(10, maxs.ordValue());
    assertEquals(NO_MORE_DOCS, maxs.nextDoc());
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
    assertEquals(5, mins.nextDoc());
    assertEquals(3, mins.longValue());
    assertEquals(15, mins.nextDoc());
    assertEquals(10, mins.longValue());
    assertEquals(NO_MORE_DOCS, mins.nextDoc());

    final NumericDocValues maxs = BlockJoinSelector.wrap(DocValues.singleton(new CannedNumericDocValues(longs, docsWithValue)), BlockJoinSelector.Type.MAX, parents, children);
    assertEquals(5, maxs.nextDoc());
    assertEquals(7, maxs.longValue());
    assertEquals(15, maxs.nextDoc());
    assertEquals(10, maxs.longValue());
    assertEquals(NO_MORE_DOCS, maxs.nextDoc());
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
