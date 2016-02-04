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
package org.apache.lucene.search.suggest;

import java.util.AbstractMap.SimpleEntry;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestInputIterator extends LuceneTestCase {
  
  public void testEmpty() throws Exception {
    InputArrayIterator iterator = new InputArrayIterator(new Input[0]);
    InputIterator wrapper = new SortedInputIterator(iterator, BytesRef.getUTF8SortedAsUnicodeComparator());
    assertNull(wrapper.next());
    wrapper = new UnsortedInputIterator(iterator);
    assertNull(wrapper.next());
  }
  
  public void testTerms() throws Exception {
    Random random = random();
    int num = atLeast(10000);
    
    Comparator<BytesRef> comparator = random.nextBoolean() ? BytesRef.getUTF8SortedAsUnicodeComparator() : BytesRef.getUTF8SortedAsUTF16Comparator();
    TreeMap<BytesRef, SimpleEntry<Long, BytesRef>> sorted = new TreeMap<>(comparator);
    TreeMap<BytesRef, Long> sortedWithoutPayload = new TreeMap<>(comparator);
    TreeMap<BytesRef, SimpleEntry<Long, Set<BytesRef>>> sortedWithContext = new TreeMap<>(comparator);
    TreeMap<BytesRef, SimpleEntry<Long, SimpleEntry<BytesRef, Set<BytesRef>>>> sortedWithPayloadAndContext = new TreeMap<>(comparator);
    Input[] unsorted = new Input[num];
    Input[] unsortedWithoutPayload = new Input[num];
    Input[] unsortedWithContexts = new Input[num];
    Input[] unsortedWithPayloadAndContext = new Input[num];
    Set<BytesRef> ctxs;
    for (int i = 0; i < num; i++) {
      BytesRef key;
      BytesRef payload;
      ctxs = new HashSet<>();
      do {
        key = new BytesRef(TestUtil.randomUnicodeString(random));
        payload = new BytesRef(TestUtil.randomUnicodeString(random));
        for(int j = 0; j < atLeast(2); j++) {
          ctxs.add(new BytesRef(TestUtil.randomUnicodeString(random)));
        }
      } while (sorted.containsKey(key));
      long value = random.nextLong();
      sortedWithoutPayload.put(key, value);
      sorted.put(key, new SimpleEntry<>(value, payload));
      sortedWithContext.put(key, new SimpleEntry<>(value, ctxs));
      sortedWithPayloadAndContext.put(key, new SimpleEntry<>(value, new SimpleEntry<>(payload, ctxs)));
      unsorted[i] = new Input(key, value, payload);
      unsortedWithoutPayload[i] = new Input(key, value);
      unsortedWithContexts[i] = new Input(key, value, ctxs);
      unsortedWithPayloadAndContext[i] = new Input(key, value, payload, ctxs);
    }
    
    // test the sorted iterator wrapper with payloads
    InputIterator wrapper = new SortedInputIterator(new InputArrayIterator(unsorted), comparator);
    Iterator<Map.Entry<BytesRef, SimpleEntry<Long, BytesRef>>> expected = sorted.entrySet().iterator();
    while (expected.hasNext()) {
      Map.Entry<BytesRef,SimpleEntry<Long, BytesRef>> entry = expected.next();
      
      assertEquals(entry.getKey(), wrapper.next());
      assertEquals(entry.getValue().getKey().longValue(), wrapper.weight());
      assertEquals(entry.getValue().getValue(), wrapper.payload());
    }
    assertNull(wrapper.next());
    
    // test the sorted iterator wrapper with contexts
    wrapper = new SortedInputIterator(new InputArrayIterator(unsortedWithContexts), comparator);
    Iterator<Map.Entry<BytesRef, SimpleEntry<Long, Set<BytesRef>>>> actualEntries = sortedWithContext.entrySet().iterator();
    while (actualEntries.hasNext()) {
      Map.Entry<BytesRef, SimpleEntry<Long, Set<BytesRef>>> entry = actualEntries.next();
      assertEquals(entry.getKey(), wrapper.next());
      assertEquals(entry.getValue().getKey().longValue(), wrapper.weight());
      Set<BytesRef> actualCtxs = entry.getValue().getValue();
      assertEquals(actualCtxs, wrapper.contexts());
    }
    assertNull(wrapper.next());
    
    // test the sorted iterator wrapper with contexts and payload
    wrapper = new SortedInputIterator(new InputArrayIterator(unsortedWithPayloadAndContext), comparator);
    Iterator<Map.Entry<BytesRef, SimpleEntry<Long, SimpleEntry<BytesRef, Set<BytesRef>>>>> expectedPayloadContextEntries = sortedWithPayloadAndContext.entrySet().iterator();
    while (expectedPayloadContextEntries.hasNext()) {
      Map.Entry<BytesRef, SimpleEntry<Long, SimpleEntry<BytesRef, Set<BytesRef>>>> entry = expectedPayloadContextEntries.next();
      assertEquals(entry.getKey(), wrapper.next());
      assertEquals(entry.getValue().getKey().longValue(), wrapper.weight());
      Set<BytesRef> actualCtxs = entry.getValue().getValue().getValue();
      assertEquals(actualCtxs, wrapper.contexts());
      BytesRef actualPayload = entry.getValue().getValue().getKey();
      assertEquals(actualPayload, wrapper.payload());
    }
    assertNull(wrapper.next());
    
    // test the unsorted iterator wrapper with payloads
    wrapper = new UnsortedInputIterator(new InputArrayIterator(unsorted));
    TreeMap<BytesRef, SimpleEntry<Long, BytesRef>> actual = new TreeMap<>();
    BytesRef key;
    while ((key = wrapper.next()) != null) {
      long value = wrapper.weight();
      BytesRef payload = wrapper.payload();
      actual.put(BytesRef.deepCopyOf(key), new SimpleEntry<>(value, BytesRef.deepCopyOf(payload)));
    }
    assertEquals(sorted, actual);

    // test the sorted iterator wrapper without payloads
    InputIterator wrapperWithoutPayload = new SortedInputIterator(new InputArrayIterator(unsortedWithoutPayload), comparator);
    Iterator<Map.Entry<BytesRef, Long>> expectedWithoutPayload = sortedWithoutPayload.entrySet().iterator();
    while (expectedWithoutPayload.hasNext()) {
      Map.Entry<BytesRef, Long> entry = expectedWithoutPayload.next();
      
      assertEquals(entry.getKey(), wrapperWithoutPayload.next());
      assertEquals(entry.getValue().longValue(), wrapperWithoutPayload.weight());
      assertNull(wrapperWithoutPayload.payload());
    }
    assertNull(wrapperWithoutPayload.next());
    
    // test the unsorted iterator wrapper without payloads
    wrapperWithoutPayload = new UnsortedInputIterator(new InputArrayIterator(unsortedWithoutPayload));
    TreeMap<BytesRef, Long> actualWithoutPayload = new TreeMap<>();
    while ((key = wrapperWithoutPayload.next()) != null) {
      long value = wrapperWithoutPayload.weight();
      assertNull(wrapperWithoutPayload.payload());
      actualWithoutPayload.put(BytesRef.deepCopyOf(key), value);
    }
    assertEquals(sortedWithoutPayload, actualWithoutPayload);
  }
  
  public static long asLong(BytesRef b) {
    return (((long) asIntInternal(b, b.offset) << 32) | asIntInternal(b,
        b.offset + 4) & 0xFFFFFFFFL);
  }

  private static int asIntInternal(BytesRef b, int pos) {
    return ((b.bytes[pos++] & 0xFF) << 24) | ((b.bytes[pos++] & 0xFF) << 16)
        | ((b.bytes[pos++] & 0xFF) << 8) | (b.bytes[pos] & 0xFF);
  }
}
