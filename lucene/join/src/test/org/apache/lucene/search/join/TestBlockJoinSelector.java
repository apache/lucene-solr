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

import java.util.Arrays;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
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

  public void testSortedSelector() {
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
    final SortedDocValues values = new SortedDocValues() {

      @Override
      public int getOrd(int docID) {
        return ords[docID];
      }

      @Override
      public BytesRef lookupOrd(int ord) {
        throw new UnsupportedOperationException();
      }

      @Override
      public int getValueCount() {
        return 11;
      }

    };

    final SortedDocValues mins = BlockJoinSelector.wrap(DocValues.singleton(values), BlockJoinSelector.Type.MIN, parents, children);
    assertEquals(-1, mins.getOrd(0));
    assertEquals(3, mins.getOrd(5));
    assertEquals(-1, mins.getOrd(6));
    assertEquals(-1, mins.getOrd(10));
    assertEquals(10, mins.getOrd(15));
    assertEquals(-1, mins.getOrd(19));

    final SortedDocValues maxs = BlockJoinSelector.wrap(DocValues.singleton(values), BlockJoinSelector.Type.MAX, parents, children);
    assertEquals(-1, maxs.getOrd(0));
    assertEquals(7, maxs.getOrd(5));
    assertEquals(-1, maxs.getOrd(6));
    assertEquals(-1, maxs.getOrd(10));
    assertEquals(10, maxs.getOrd(15));
    assertEquals(-1, maxs.getOrd(19));
  }

  public void testNumericSelector() {
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
    final NumericDocValues values = new NumericDocValues() {

      @Override
      public long get(int docID) {
        return longs[docID];
      }
      
    };

    final NumericDocValues mins = BlockJoinSelector.wrap(DocValues.singleton(values, docsWithValue), BlockJoinSelector.Type.MIN, parents, children);
    assertEquals(0, mins.get(0));
    assertEquals(3, mins.get(5));
    assertEquals(0, mins.get(6));
    assertEquals(0, mins.get(10));
    assertEquals(10, mins.get(15));
    assertEquals(0, mins.get(19));

    final NumericDocValues maxs = BlockJoinSelector.wrap(DocValues.singleton(values, docsWithValue), BlockJoinSelector.Type.MAX, parents, children);
    assertEquals(0, maxs.get(0));
    assertEquals(7, maxs.get(5));
    assertEquals(0, maxs.get(6));
    assertEquals(0, maxs.get(10));
    assertEquals(10, maxs.get(15));
    assertEquals(0, maxs.get(19));
  }
}
