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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestFieldUpdatesBuffer extends LuceneTestCase {

  public void testBasics() throws IOException {
    Counter counter = Counter.newCounter();
    DocValuesUpdate.NumericDocValuesUpdate update =
        new DocValuesUpdate.NumericDocValuesUpdate(new Term("id", "1"), "age", 6);
    FieldUpdatesBuffer buffer = new FieldUpdatesBuffer(counter, update, 15);
    buffer.addUpdate(new Term("id", "10"), 6, 15);
    assertTrue(buffer.hasSingleValue());
    buffer.addUpdate(new Term("id", "8"), 12, 15);
    assertFalse(buffer.hasSingleValue());
    buffer.addUpdate(new Term("some_other_field", "8"), 13, 17);
    assertFalse(buffer.hasSingleValue());
    buffer.addUpdate(new Term("id", "8"), 12, 16);
    assertFalse(buffer.hasSingleValue());
    assertTrue(buffer.isNumeric());
    assertEquals(13, buffer.getMaxNumeric());
    assertEquals(6, buffer.getMinNumeric());
    buffer.finish();
    FieldUpdatesBuffer.BufferedUpdateIterator iterator = buffer.iterator();
    FieldUpdatesBuffer.BufferedUpdate value = iterator.next();
    assertNotNull(value);
    assertEquals("id", value.termField);
    assertEquals("1", value.termValue.utf8ToString());
    assertEquals(6, value.numericValue);
    assertEquals(15, value.docUpTo);

    value = iterator.next();
    assertNotNull(value);
    assertEquals("id", value.termField);
    assertEquals("10", value.termValue.utf8ToString());
    assertEquals(6, value.numericValue);
    assertEquals(15, value.docUpTo);

    value = iterator.next();
    assertNotNull(value);
    assertEquals("id", value.termField);
    assertEquals("8", value.termValue.utf8ToString());
    assertEquals(12, value.numericValue);
    assertEquals(15, value.docUpTo);

    value = iterator.next();
    assertNotNull(value);
    assertEquals("some_other_field", value.termField);
    assertEquals("8", value.termValue.utf8ToString());
    assertEquals(13, value.numericValue);
    assertEquals(17, value.docUpTo);

    value = iterator.next();
    assertNotNull(value);
    assertEquals("id", value.termField);
    assertEquals("8", value.termValue.utf8ToString());
    assertEquals(12, value.numericValue);
    assertEquals(16, value.docUpTo);
    assertNull(iterator.next());
  }

  public void testUpdateShareValues() throws IOException {
    Counter counter = Counter.newCounter();
    int intValue = random().nextInt();
    boolean valueForThree = random().nextBoolean();
    DocValuesUpdate.NumericDocValuesUpdate update =
        new DocValuesUpdate.NumericDocValuesUpdate(new Term("id", "0"), "enabled", intValue);
    FieldUpdatesBuffer buffer = new FieldUpdatesBuffer(counter, update, Integer.MAX_VALUE);
    buffer.addUpdate(new Term("id", "1"), intValue, Integer.MAX_VALUE);
    buffer.addUpdate(new Term("id", "2"), intValue, Integer.MAX_VALUE);
    if (valueForThree) {
      buffer.addUpdate(new Term("id", "3"), intValue, Integer.MAX_VALUE);
    } else {
      buffer.addNoValue(new Term("id", "3"), Integer.MAX_VALUE);
    }
    buffer.addUpdate(new Term("id", "4"), intValue, Integer.MAX_VALUE);
    buffer.finish();
    FieldUpdatesBuffer.BufferedUpdateIterator iterator = buffer.iterator();
    FieldUpdatesBuffer.BufferedUpdate value;
    int count = 0;
    while ((value = iterator.next()) != null) {
      boolean hasValue = count != 3 || valueForThree;
      assertEquals("" + (count++), value.termValue.utf8ToString());
      assertEquals("id", value.termField);
      assertEquals(hasValue, value.hasValue);
      if (hasValue) {
        assertEquals(intValue, value.numericValue);
      } else {
        assertEquals(0, value.numericValue);
      }
      assertEquals(Integer.MAX_VALUE, value.docUpTo);
    }
    assertTrue(buffer.isNumeric());
  }

  public void testUpdateShareValuesBinary() throws IOException {
    Counter counter = Counter.newCounter();
    boolean valueForThree = random().nextBoolean();
    DocValuesUpdate.BinaryDocValuesUpdate update =
        new DocValuesUpdate.BinaryDocValuesUpdate(new Term("id", "0"), "enabled", new BytesRef(""));
    FieldUpdatesBuffer buffer = new FieldUpdatesBuffer(counter, update, Integer.MAX_VALUE);
    buffer.addUpdate(new Term("id", "1"), new BytesRef(""), Integer.MAX_VALUE);
    buffer.addUpdate(new Term("id", "2"), new BytesRef(""), Integer.MAX_VALUE);
    if (valueForThree) {
      buffer.addUpdate(new Term("id", "3"), new BytesRef(""), Integer.MAX_VALUE);
    } else {
      buffer.addNoValue(new Term("id", "3"), Integer.MAX_VALUE);
    }
    buffer.addUpdate(new Term("id", "4"), new BytesRef(""), Integer.MAX_VALUE);
    buffer.finish();
    FieldUpdatesBuffer.BufferedUpdateIterator iterator = buffer.iterator();
    FieldUpdatesBuffer.BufferedUpdate value;
    int count = 0;
    while ((value = iterator.next()) != null) {
      boolean hasValue = count != 3 || valueForThree;
      assertEquals("" + (count++), value.termValue.utf8ToString());
      assertEquals("id", value.termField);
      assertEquals(hasValue, value.hasValue);
      if (hasValue) {
        assertEquals(new BytesRef(""), value.binaryValue);
      } else {
        assertNull(value.binaryValue);
      }
      assertEquals(Integer.MAX_VALUE, value.docUpTo);
    }
    assertFalse(buffer.isNumeric());
  }

  int randomDocUpTo() {
    if (random().nextInt(5) == 0) {
      return Integer.MAX_VALUE;
    } else {
      return random().nextInt(10000);
    }
  }

  DocValuesUpdate.BinaryDocValuesUpdate getRandomBinaryUpdate() {
    String termField = RandomPicks.randomFrom(random(), Arrays.asList("id", "_id", "some_other_field"));
    String docId = "" + random().nextInt(10);
    DocValuesUpdate.BinaryDocValuesUpdate value = new DocValuesUpdate.BinaryDocValuesUpdate(new Term(termField, docId), "binary",
        rarely() ? null : new BytesRef(TestUtil.randomRealisticUnicodeString(random())));
    return rarely() ? value.prepareForApply(randomDocUpTo()) : value;
  }

  DocValuesUpdate.NumericDocValuesUpdate getRandomNumericUpdate() {
    String termField = RandomPicks.randomFrom(random(), Arrays.asList("id", "_id", "some_other_field"));
    String docId = "" + random().nextInt(10);
    DocValuesUpdate.NumericDocValuesUpdate value = new DocValuesUpdate.NumericDocValuesUpdate(new Term(termField, docId), "numeric",
        rarely() ? null : Long.valueOf(random().nextInt(100)));
    return rarely() ? value.prepareForApply(randomDocUpTo()) : value;
  }

  public void testBinaryRandom() throws IOException {
    List<DocValuesUpdate.BinaryDocValuesUpdate> updates = new ArrayList<>();
    int numUpdates = 1 + random().nextInt(1000);
    Counter counter = Counter.newCounter();
    DocValuesUpdate.BinaryDocValuesUpdate randomUpdate = getRandomBinaryUpdate();
    updates.add(randomUpdate);
    FieldUpdatesBuffer buffer = new FieldUpdatesBuffer(counter, randomUpdate, randomUpdate.docIDUpto);
    for (int i = 0; i < numUpdates; i++) {
      randomUpdate = getRandomBinaryUpdate();
      updates.add(randomUpdate);
      if (randomUpdate.hasValue) {
        buffer.addUpdate(randomUpdate.term, randomUpdate.getValue(), randomUpdate.docIDUpto);
      } else {
        buffer.addNoValue(randomUpdate.term, randomUpdate.docIDUpto);
      }
    }
    buffer.finish();
    FieldUpdatesBuffer.BufferedUpdateIterator iterator = buffer.iterator();
    FieldUpdatesBuffer.BufferedUpdate value;

    int count = 0;
    while ((value = iterator.next()) != null) {
      randomUpdate = updates.get(count++);
      assertEquals(randomUpdate.term.bytes.utf8ToString(), value.termValue.utf8ToString());
      assertEquals(randomUpdate.term.field, value.termField);
      assertEquals("count: " + count, randomUpdate.hasValue, value.hasValue);
      if (randomUpdate.hasValue) {
        assertEquals(randomUpdate.getValue(), value.binaryValue);
      } else {
        assertNull(value.binaryValue);
      }
      assertEquals(randomUpdate.docIDUpto, value.docUpTo);
    }
    assertEquals(count, updates.size());
  }

  public void testNumericRandom() throws IOException {
    List<DocValuesUpdate.NumericDocValuesUpdate> updates = new ArrayList<>();
    int numUpdates = 1 + random().nextInt(1000);
    Counter counter = Counter.newCounter();
    DocValuesUpdate.NumericDocValuesUpdate randomUpdate = getRandomNumericUpdate();
    updates.add(randomUpdate);
    FieldUpdatesBuffer buffer = new FieldUpdatesBuffer(counter, randomUpdate, randomUpdate.docIDUpto);
    for (int i = 0; i < numUpdates; i++) {
      randomUpdate = getRandomNumericUpdate();
      updates.add(randomUpdate);
      if (randomUpdate.hasValue) {
        buffer.addUpdate(randomUpdate.term, randomUpdate.getValue(), randomUpdate.docIDUpto);
      } else {
        buffer.addNoValue(randomUpdate.term, randomUpdate.docIDUpto);
      }
    }
    buffer.finish();
    DocValuesUpdate.NumericDocValuesUpdate lastUpdate = randomUpdate;
    boolean termsSorted = lastUpdate.hasValue && updates.stream()
        .allMatch(update -> update.field.equals(lastUpdate.field) &&
            update.hasValue && update.getValue() == lastUpdate.getValue());
    assertBufferUpdates(buffer, updates, termsSorted);
  }

  public void testNoNumericValue() {
    DocValuesUpdate.NumericDocValuesUpdate update =
        new DocValuesUpdate.NumericDocValuesUpdate(new Term("id", "1"), "age", null);
    FieldUpdatesBuffer buffer = new FieldUpdatesBuffer(Counter.newCounter(), update, update.docIDUpto);
    assertEquals(0, buffer.getMinNumeric());
    assertEquals(0, buffer.getMaxNumeric());
  }

  public void testSortAndDedupNumericUpdatesByTerms() throws IOException {
    List<DocValuesUpdate.NumericDocValuesUpdate> updates = new ArrayList<>();
    int numUpdates = 1 + random().nextInt(1000);
    Counter counter = Counter.newCounter();
    String termField = RandomPicks.randomFrom(random(), Arrays.asList("id", "_id", "some_other_field"));
    long docValue = 1 + random().nextInt(1000);
    DocValuesUpdate.NumericDocValuesUpdate randomUpdate = new DocValuesUpdate.NumericDocValuesUpdate(
        new Term(termField, Integer.toString(random().nextInt(1000))), "numeric", docValue);
    randomUpdate = randomUpdate.prepareForApply(randomDocUpTo());
    updates.add(randomUpdate);
    FieldUpdatesBuffer buffer = new FieldUpdatesBuffer(counter, randomUpdate, randomUpdate.docIDUpto);
    for (int i = 0; i < numUpdates; i++) {
      randomUpdate = new DocValuesUpdate.NumericDocValuesUpdate(
          new Term(termField, Integer.toString(random().nextInt(1000))), "numeric", docValue);
      randomUpdate = randomUpdate.prepareForApply(randomDocUpTo());
      updates.add(randomUpdate);
      buffer.addUpdate(randomUpdate.term, randomUpdate.getValue(), randomUpdate.docIDUpto);
    }
    buffer.finish();
    assertBufferUpdates(buffer, updates, true);
  }

  void assertBufferUpdates(FieldUpdatesBuffer buffer,
                           List<DocValuesUpdate.NumericDocValuesUpdate> updates,
                           boolean termSorted) throws IOException {
    if (termSorted) {
      updates.sort(Comparator.comparing(u -> u.term.bytes));
      SortedMap<BytesRef, DocValuesUpdate.NumericDocValuesUpdate> byTerms = new TreeMap<>();
      for (DocValuesUpdate.NumericDocValuesUpdate update : updates) {
        byTerms.compute(update.term.bytes, (k, v) -> v != null && v.docIDUpto >= update.docIDUpto ? v : update);
      }
      updates = new ArrayList<>(byTerms.values());
    }
    FieldUpdatesBuffer.BufferedUpdateIterator iterator = buffer.iterator();
    FieldUpdatesBuffer.BufferedUpdate value;

    int count = 0;
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    boolean hasAtLeastOneValue = false;
    DocValuesUpdate.NumericDocValuesUpdate expectedUpdate;
    while ((value = iterator.next()) != null) {
      long v = buffer.getNumericValue(count);
      expectedUpdate = updates.get(count++);
      assertEquals(expectedUpdate.term.bytes.utf8ToString(), value.termValue.utf8ToString());
      assertEquals(expectedUpdate.term.field, value.termField);
      assertEquals(expectedUpdate.hasValue, value.hasValue);
      if (expectedUpdate.hasValue) {
        assertEquals(expectedUpdate.getValue(), value.numericValue);
        assertEquals(v, value.numericValue);
        min = Math.min(min, v);
        max = Math.max(max, v);
        hasAtLeastOneValue = true;
      } else {
        assertEquals(0, value.numericValue);
        assertEquals(0, v);
      }
      assertEquals(expectedUpdate.docIDUpto, value.docUpTo);
    }
    if (hasAtLeastOneValue) {
      assertEquals(max, buffer.getMaxNumeric());
      assertEquals(min, buffer.getMinNumeric());
    } else {
      assertEquals(0, buffer.getMaxNumeric());
      assertEquals(0, buffer.getMinNumeric());
    }
    assertEquals(count, updates.size());
  }
}
