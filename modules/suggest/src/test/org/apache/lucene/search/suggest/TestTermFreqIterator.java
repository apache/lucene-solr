package org.apache.lucene.search.suggest;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.lucene.search.spell.TermFreqIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

public class TestTermFreqIterator extends LuceneTestCase {
  public void testEmpty() throws Exception {
    TermFreqArrayIterator iterator = new TermFreqArrayIterator(new TermFreq[0]);
    TermFreqIterator wrapper = new SortedTermFreqIteratorWrapper(iterator, BytesRef.getUTF8SortedAsUnicodeComparator());
    assertNull(wrapper.next());
    wrapper = new UnsortedTermFreqIteratorWrapper(iterator);
    assertNull(wrapper.next());
  }
  
  public void testTerms() throws Exception {
    int num = atLeast(10000);
    
    TreeMap<BytesRef,Float> sorted = new TreeMap<BytesRef,Float>();
    TermFreq[] unsorted = new TermFreq[num];

    for (int i = 0; i < num; i++) {
      BytesRef key;
      do {
        key = new BytesRef(_TestUtil.randomUnicodeString(random));
      } while (sorted.containsKey(key));
      float value = random.nextFloat();
      sorted.put(key, value);
      unsorted[i] = new TermFreq(key, value);
    }
    
    // test the sorted iterator wrapper
    TermFreqIterator wrapper = new SortedTermFreqIteratorWrapper(new TermFreqArrayIterator(unsorted), BytesRef.getUTF8SortedAsUnicodeComparator());
    Iterator<Map.Entry<BytesRef,Float>> expected = sorted.entrySet().iterator();
    while (expected.hasNext()) {
      Map.Entry<BytesRef,Float> entry = expected.next();
      
      assertEquals(entry.getKey(), wrapper.next());
      assertEquals(entry.getValue().floatValue(), wrapper.freq(), 0F);
    }
    assertNull(wrapper.next());
    
    // test the unsorted iterator wrapper
    wrapper = new UnsortedTermFreqIteratorWrapper(new TermFreqArrayIterator(unsorted));
    TreeMap<BytesRef,Float> actual = new TreeMap<BytesRef,Float>();
    BytesRef key;
    while ((key = wrapper.next()) != null) {
      float value = wrapper.freq();
      actual.put(BytesRef.deepCopyOf(key), value);
    }
    assertEquals(sorted, actual);
  }
}
