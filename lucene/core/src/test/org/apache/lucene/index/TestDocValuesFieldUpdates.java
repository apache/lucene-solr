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

package org.apache.lucene.index;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.index.NumericDocValuesFieldUpdates.SingleValueNumericDocValuesFieldUpdates;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.LuceneTestCase;

public class TestDocValuesFieldUpdates extends LuceneTestCase {

  public void testMergeIterator() {
    NumericDocValuesFieldUpdates updates1 = new NumericDocValuesFieldUpdates(0, "test", 6);
    NumericDocValuesFieldUpdates updates2 = new NumericDocValuesFieldUpdates(1, "test", 6);
    NumericDocValuesFieldUpdates updates3 = new NumericDocValuesFieldUpdates(2, "test", 6);
    NumericDocValuesFieldUpdates updates4 = new NumericDocValuesFieldUpdates(2, "test", 6);

    updates1.add(0, 1);
    updates1.add(4, 0);
    updates1.add(1, 4);
    updates1.add(2, 5);
    updates1.add(4, 9);
    assertTrue(updates1.any());

    updates2.add(0, 18);
    updates2.add(1, 7);
    updates2.add(2, 19);
    updates2.add(5, 24);
    assertTrue(updates2.any());

    updates3.add(2, 42);
    assertTrue(updates3.any());
    assertFalse(updates4.any());
    updates1.finish();
    updates2.finish();
    updates3.finish();
    updates4.finish();
    List<DocValuesFieldUpdates.Iterator> iterators = Arrays.asList(updates1.iterator(), updates2.iterator(),
        updates3.iterator(), updates4.iterator());
    Collections.shuffle(iterators, random());
    DocValuesFieldUpdates.Iterator iterator = DocValuesFieldUpdates
        .mergedIterator(iterators.toArray(new DocValuesFieldUpdates.Iterator[0]));
    assertEquals(0, iterator.nextDoc());
    assertEquals(18, iterator.longValue());
    assertEquals(1, iterator.nextDoc());
    assertEquals(7, iterator.longValue());
    assertEquals(2, iterator.nextDoc());
    assertEquals(42, iterator.longValue());
    assertEquals(4, iterator.nextDoc());
    assertEquals(9, iterator.longValue());
    assertEquals(5, iterator.nextDoc());
    assertEquals(24, iterator.longValue());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.nextDoc());
  }

  public void testUpdateAndResetSameDoc() {
    NumericDocValuesFieldUpdates updates = new NumericDocValuesFieldUpdates(0, "test", 2);
    updates.add(0, 1);
    updates.reset(0);
    updates.finish();
    NumericDocValuesFieldUpdates.Iterator iterator = updates.iterator();
    assertEquals(0, iterator.nextDoc());
    assertFalse(iterator.hasValue());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.nextDoc());
  }

  public void testUpdateAndResetUpdateSameDoc() {
    NumericDocValuesFieldUpdates updates = new NumericDocValuesFieldUpdates(0, "test", 3);
    updates.add(0, 1);
    updates.add(0);
    updates.add(0, 2);
    updates.finish();
    NumericDocValuesFieldUpdates.Iterator iterator = updates.iterator();
    assertEquals(0, iterator.nextDoc());
    assertTrue(iterator.hasValue());
    assertEquals(2, iterator.longValue());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.nextDoc());
  }

  public void testUpdatesAndResetRandom() {
    NumericDocValuesFieldUpdates updates = new NumericDocValuesFieldUpdates(0, "test", 10);
    int numUpdates = 10 + random().nextInt(100);
    Integer[] values = new Integer[5];
    for (int i = 0; i < 5; i++) {
      values[i] = random().nextBoolean() ? null : random().nextInt(100);
      if (values[i] == null) {
        updates.reset(i);
      } else {
        updates.add(i, values[i]);
      }
    }
    for (int i = 0; i < numUpdates; i++) {
      int docId = random().nextInt(5);
      values[docId] = random().nextBoolean() ? null : random().nextInt(100);
      if (values[docId] == null) {
        updates.reset(docId);
      } else {
        updates.add(docId, values[docId]);
      }
    }

    updates.finish();
    NumericDocValuesFieldUpdates.Iterator iterator = updates.iterator();
    int idx = 0;
    while (iterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
      assertEquals(idx, iterator.docID());
      if (values[idx] == null) {
        assertFalse(iterator.hasValue());
      } else {
        assertTrue(iterator.hasValue());
        assertEquals(values[idx].longValue(), iterator.longValue());
      }
      idx++;
    }
  }

  public void testSharedValueUpdates() {
    int delGen = random().nextInt();
    int maxDoc = 1 + random().nextInt(1000);
    long value = random().nextLong();
    SingleValueNumericDocValuesFieldUpdates update = new SingleValueNumericDocValuesFieldUpdates(delGen, "foo", maxDoc, value);
    assertEquals(value, update.longValue());
    Boolean[] values = new Boolean[maxDoc];
    boolean any = false;
    boolean noReset = random().nextBoolean(); // sometimes don't reset
    for (int i = 0; i < maxDoc; i++) {
      if (random().nextBoolean()) {
        values[i] = Boolean.TRUE;
        any = true;
        update.add(i, value);
      } else if (random().nextBoolean() && noReset == false) {
        values[i] = null;
        any = true;
        update.reset(i);
      } else {
        values[i] = Boolean.FALSE;
      }
    }
    if (noReset == false) {
      for (int i = 0; i < values.length; i++) {
        if (rarely()) {
          if (values[i] == null) {
            values[i] = Boolean.TRUE;
            update.add(i, value);
          } else if (values[i]) {
            values[i] = null;
            update.reset(i);
          }
        }
      }
    }
    update.finish();
    DocValuesFieldUpdates.Iterator iterator = update.iterator();
    assertEquals(any, update.any());
    assertEquals(delGen, iterator.delGen());
    int index = 0;
    while (iterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
      int doc = iterator.docID();
      if (index < iterator.docID()) {
        for (;index < doc; index++) {
          assertFalse(values[index]);
        }
      }
      if (index == doc) {
        if (values[index++] == null) {
          assertFalse(iterator.hasValue());
        } else {
          assertTrue(iterator.hasValue());
          assertEquals(value, iterator.longValue());
        }
      }
    }
  }
}
