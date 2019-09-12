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
package org.apache.lucene.util;

import java.io.IOException;
import java.util.*;

public class TestBytesRefArray extends LuceneTestCase {

  public void testAppend() throws IOException {
    Random random = random();
    BytesRefArray list = new BytesRefArray(Counter.newCounter());
    List<String> stringList = new ArrayList<>();
    for (int j = 0; j < 2; j++) {
      if (j > 0 && random.nextBoolean()) {
        list.clear();
        stringList.clear();
      }
      int entries = atLeast(500);
      BytesRefBuilder spare = new BytesRefBuilder();
      int initSize = list.size();
      for (int i = 0; i < entries; i++) {
        String randomRealisticUnicodeString = TestUtil
            .randomRealisticUnicodeString(random);
        spare.copyChars(randomRealisticUnicodeString);
        assertEquals(i+initSize, list.append(spare.get()));
        stringList.add(randomRealisticUnicodeString);
      }
      for (int i = 0; i < entries; i++) {
        assertNotNull(list.get(spare, i));
        assertEquals("entry " + i + " doesn't match", stringList.get(i),
            spare.get().utf8ToString());
      }
      
      // check random
      for (int i = 0; i < entries; i++) {
        int e = random.nextInt(entries);
        assertNotNull(list.get(spare, e));
        assertEquals("entry " + i + " doesn't match", stringList.get(e),
            spare.get().utf8ToString());
      }
      for (int i = 0; i < 2; i++) {
        
        BytesRefIterator iterator = list.iterator();
        for (String string : stringList) {
          assertEquals(string, iterator.next().utf8ToString());
        }
      }
    }
  }

  public void testSort() throws IOException {
    Random random = random();
    BytesRefArray list = new BytesRefArray(Counter.newCounter());
    List<String> stringList = new ArrayList<>();

    for (int j = 0; j < 2; j++) {
      if (j > 0 && random.nextBoolean()) {
        list.clear();
        stringList.clear();
      }
      int entries = atLeast(500);
      BytesRefBuilder spare = new BytesRefBuilder();
      final int initSize = list.size();
      for (int i = 0; i < entries; i++) {
        String randomRealisticUnicodeString = TestUtil
            .randomRealisticUnicodeString(random);
        spare.copyChars(randomRealisticUnicodeString);
        assertEquals(initSize + i, list.append(spare.get()));
        stringList.add(randomRealisticUnicodeString);
      }
      
      Collections.sort(stringList, TestUtil.STRING_CODEPOINT_COMPARATOR);
      BytesRefIterator iter = list.iterator(Comparator.naturalOrder());
      int i = 0;
      BytesRef next;
      while ((next = iter.next()) != null) {
        assertEquals("entry " + i + " doesn't match", stringList.get(i),
            next.utf8ToString());
        i++;
      }
      assertNull(iter.next());
      assertEquals(i, stringList.size());
    }
    
  }
  
}
