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
package org.apache.lucene.analysis.ko.util;

import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

public class UnknownDictionaryTest extends LuceneTestCase {

  @Test
  public void testPutCharacterCategory() {
    UnknownDictionaryWriter unkDic = new UnknownDictionaryWriter(10 * 1024 * 1024);

    expectThrows(Exception.class, () -> unkDic.putCharacterCategory(0, "DUMMY_NAME"));

    expectThrows(Exception.class, () -> unkDic.putCharacterCategory(-1, "HANGUL"));
    
    unkDic.putCharacterCategory(0, "DEFAULT");
    unkDic.putCharacterCategory(1, "GREEK");
    unkDic.putCharacterCategory(2, "HANJA");
    unkDic.putCharacterCategory(3, "HANGUL");
    unkDic.putCharacterCategory(4, "KANJI");
  }
  
  @Test
  public void testPut() {
    UnknownDictionaryWriter unkDic = new UnknownDictionaryWriter(10 * 1024 * 1024);
    expectThrows(NumberFormatException.class, () ->
        unkDic.put(CSVUtil.parse("HANGUL,1800,3562,UNKNOWN,*,*,*,*,*,*,*")));

    String entry1 = "ALPHA,1793,3533,795,SL,*,*,*,*,*,*,*";
    String entry2 = "HANGUL,1800,3562,10247,UNKNOWN,*,*,*,*,*,*,*";
    String entry3 = "HANJA,1792,3554,-821,SH,*,*,*,*,*,*,*";

    unkDic.putCharacterCategory(0, "ALPHA");
    unkDic.putCharacterCategory(1, "HANGUL");
    unkDic.putCharacterCategory(2, "HANJA");
    
    unkDic.put(CSVUtil.parse(entry1));
    unkDic.put(CSVUtil.parse(entry2));
    unkDic.put(CSVUtil.parse(entry3));
  }
}
