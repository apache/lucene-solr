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
package org.apache.lucene.analysis.ja.util;

import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

public class UnknownDictionaryTest extends LuceneTestCase {
  public static final String FILENAME = "unk-tokeninfo-dict.obj";
  
  @Test
  public void testPutCharacterCategory() {
    UnknownDictionaryWriter unkDic = new UnknownDictionaryWriter(10 * 1024 * 1024);
    
    expectThrows(Exception.class, () -> unkDic.putCharacterCategory(0, "DUMMY_NAME"));
    
    expectThrows(Exception.class, () -> unkDic.putCharacterCategory(-1, "KATAKANA"));
    
    unkDic.putCharacterCategory(0, "DEFAULT");
    unkDic.putCharacterCategory(1, "GREEK");
    unkDic.putCharacterCategory(2, "HIRAGANA");
    unkDic.putCharacterCategory(3, "KATAKANA");
    unkDic.putCharacterCategory(4, "KANJI");
  }
  
  @Test
  public void testPut() {
    UnknownDictionaryWriter unkDic = new UnknownDictionaryWriter(10 * 1024 * 1024);
    expectThrows(NumberFormatException.class, () ->
                 unkDic.put(CSVUtil.parse("KANJI,1285,11426,名詞,一般,*,*,*,*,*,*,*")));

    String entry1 = "ALPHA,1285,1285,13398,名詞,一般,*,*,*,*,*,*,*";
    String entry2 = "HIRAGANA,1285,1285,13069,名詞,一般,*,*,*,*,*,*,*";
    String entry3 = "KANJI,1285,1285,11426,名詞,一般,*,*,*,*,*,*,*";

    unkDic.putCharacterCategory(0, "ALPHA");
    unkDic.putCharacterCategory(1, "HIRAGANA");
    unkDic.putCharacterCategory(2, "KANJI");
    
    unkDic.put(CSVUtil.parse(entry1));
    unkDic.put(CSVUtil.parse(entry2));
    unkDic.put(CSVUtil.parse(entry3));
  }
}
