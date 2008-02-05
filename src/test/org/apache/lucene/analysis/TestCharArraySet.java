package org.apache.lucene.analysis;

/**
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

import java.util.Arrays;

import org.apache.lucene.util.LuceneTestCase;

public class TestCharArraySet extends LuceneTestCase
{
  public void testRehash() throws Exception {
    CharArraySet cas = new CharArraySet(0, true);
    for(int i=0;i<StopAnalyzer.ENGLISH_STOP_WORDS.length;i++)
      cas.add(StopAnalyzer.ENGLISH_STOP_WORDS[i]);
    assertEquals(StopAnalyzer.ENGLISH_STOP_WORDS.length, cas.size());
    for(int i=0;i<StopAnalyzer.ENGLISH_STOP_WORDS.length;i++)
      assertTrue(cas.contains(StopAnalyzer.ENGLISH_STOP_WORDS[i]));
  }

  public void testNonZeroOffset() {
    String[] words={"Hello","World","this","is","a","test"};
    char[] findme="xthisy".toCharArray();   
    CharArraySet set=new CharArraySet(10,true);
    set.addAll(Arrays.asList(words));
    assertTrue(set.contains(findme, 1, 4));
    assertTrue(set.contains(new String(findme,1,4)));
  }
}
