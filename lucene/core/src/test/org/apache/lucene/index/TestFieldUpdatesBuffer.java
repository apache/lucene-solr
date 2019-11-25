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
import java.util.List;

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

  @SuppressWarnings("unchecked")
  public <T extends DocValuesUpdate> T getRandomUpdate(boolean binary) {
    String termField = RandomPicks.randomFrom(random(), Arrays.asList("id", "_id", "some_other_field"));
    String docId = "" + random().nextInt(10);
    if (binary) {
      DocValuesUpdate.BinaryDocValuesUpdate value =  new DocValuesUpdate.BinaryDocValuesUpdate(new Term(termField, docId), "binary",
          rarely() ? null : new BytesRef(TestUtil.randomRealisticUnicodeString(random())));
      return (T) (rarely() ? value.prepareForApply(random().nextInt(100)) : value);
    } else {
      DocValuesUpdate.NumericDocValuesUpdate value = new DocValuesUpdate.NumericDocValuesUpdate(new Term(termField, docId), "numeric",
          rarely() ? null : Long.valueOf(random().nextInt(100)));

      return (T) (rarely() ? value.prepareForApply(random().nextInt(100)) : value);
    }
  }

  public void testBinaryRandom() throws IOException {
    List<DocValuesUpdate.BinaryDocValuesUpdate> updates = new ArrayList<>();
    int numUpdates = 1 + random().nextInt(1000);
    Counter counter = Counter.newCounter();
    DocValuesUpdate.BinaryDocValuesUpdate randomUpdate = getRandomUpdate(true);
    updates.add(randomUpdate);
    FieldUpdatesBuffer buffer = new FieldUpdatesBuffer(counter, randomUpdate, randomUpdate.docIDUpto);
    for (int i = 0; i < numUpdates; i++) {
      randomUpdate = getRandomUpdate(true);
      updates.add(randomUpdate);
      if (randomUpdate.hasValue) {
        buffer.addUpdate(randomUpdate.term, randomUpdate.getValue(), randomUpdate.docIDUpto);
      } else {
        buffer.addNoValue(randomUpdate.term, randomUpdate.docIDUpto);
      }
    }
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
    DocValuesUpdate.NumericDocValuesUpdate randomUpdate = getRandomUpdate(false);
    updates.add(randomUpdate);
    FieldUpdatesBuffer buffer = new FieldUpdatesBuffer(counter, randomUpdate, randomUpdate.docIDUpto);
    for (int i = 0; i < numUpdates; i++) {
      randomUpdate = getRandomUpdate(false);
      updates.add(randomUpdate);
      if (randomUpdate.hasValue) {
        buffer.addUpdate(randomUpdate.term, randomUpdate.getValue(), randomUpdate.docIDUpto);
      } else {
        buffer.addNoValue(randomUpdate.term, randomUpdate.docIDUpto);
      }
    }
    FieldUpdatesBuffer.BufferedUpdateIterator iterator = buffer.iterator();
    FieldUpdatesBuffer.BufferedUpdate value;

    int count = 0;
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    boolean hasAtLeastOneValue = false;
    while ((value = iterator.next()) != null) {
      long v = buffer.getNumericValue(count);
      randomUpdate = updates.get(count++);
      assertEquals(randomUpdate.term.bytes.utf8ToString(), value.termValue.utf8ToString());
      assertEquals(randomUpdate.term.field, value.termField);
      assertEquals(randomUpdate.hasValue, value.hasValue);
      if (randomUpdate.hasValue) {
        assertEquals(randomUpdate.getValue(), value.numericValue);
        assertEquals(v, value.numericValue);
        min = Math.min(min, v);
        max = Math.max(max, v);
        hasAtLeastOneValue = true;
      } else {
        assertEquals(0, value.numericValue);
        assertEquals(0, v);
      }
      assertEquals(randomUpdate.docIDUpto, value.docUpTo);
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

  public void testNoNumericValue() {
    DocValuesUpdate.NumericDocValuesUpdate update =
        new DocValuesUpdate.NumericDocValuesUpdate(new Term("id", "1"), "age", null);
    FieldUpdatesBuffer buffer = new FieldUpdatesBuffer(Counter.newCounter(), update, update.docIDUpto);
    assertEquals(0, buffer.getMinNumeric());
    assertEquals(0, buffer.getMaxNumeric());
  }
}
