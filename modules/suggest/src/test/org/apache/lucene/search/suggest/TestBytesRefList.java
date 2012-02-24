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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.search.suggest.BytesRefList;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

public class TestBytesRefList extends LuceneTestCase {

  public void testAppend() throws IOException {
    BytesRefList list = new BytesRefList();
    List<String> stringList = new ArrayList<String>();
    int entries = atLeast(500);
    BytesRef spare = new BytesRef();
    for (int i = 0; i < entries; i++) {
      String randomRealisticUnicodeString = _TestUtil
          .randomRealisticUnicodeString(random);
      spare.copyChars(randomRealisticUnicodeString);
      list.append(spare);
      stringList.add(randomRealisticUnicodeString);
    }
    for (int i = 0; i < entries; i++) {
      assertNotNull(list.get(spare, i));
      assertEquals("entry " + i + " doesn't match", stringList.get(i),
          spare.utf8ToString());
    }

    // check random
    for (int i = 0; i < entries; i++) {
      int e = random.nextInt(entries);
      assertNotNull(list.get(spare, e));
      assertEquals("entry " + i + " doesn't match", stringList.get(e),
          spare.utf8ToString());
    }
    for (int i = 0; i < 2; i++) {

      BytesRefIterator iterator = list.iterator();
      for (String string : stringList) {
        assertEquals(string, iterator.next().utf8ToString());
      }
    }
  }

  public void testSort() {
    BytesRefList list = new BytesRefList();
    List<String> stringList = new ArrayList<String>();
    int entries = atLeast(500);
    BytesRef spare = new BytesRef();
    for (int i = 0; i < entries; i++) {
      String randomRealisticUnicodeString = _TestUtil.randomRealisticUnicodeString(random);
      spare.copyChars(randomRealisticUnicodeString);
      list.append(spare);
      stringList.add(randomRealisticUnicodeString);
    }
    Collections.sort(stringList);
    int[] sortedOrds = list.sort(BytesRef.getUTF8SortedAsUTF16Comparator());
    for (int i = 0; i < entries; i++) {
      assertNotNull(list.get(spare, sortedOrds[i]));
      assertEquals("entry " + i + " doesn't match", stringList.get(i),
          spare.utf8ToString());
    }
    
  }
}
