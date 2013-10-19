package org.apache.lucene.analysis.ko.dic;

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

import org.apache.lucene.analysis.ko.dic.HanjaMapper;
import org.apache.lucene.util.LuceneTestCase;

public class TestHanjaMapper extends LuceneTestCase {
  
  public void testOneToOne() {
    assertEquals("구",  new String(HanjaMapper.convertToHangul('㐀')));
    assertEquals("인",  new String(HanjaMapper.convertToHangul('印')));
  }
  
  public void testOneToMany() {
    assertEquals("기지",  new String(HanjaMapper.convertToHangul('枳')));
  }
  
  public void testOutOfBounds() {
    assertEquals("\u33FF", new String(HanjaMapper.convertToHangul('\u33FF')));
    assertEquals("A", new String(HanjaMapper.convertToHangul('A')));
    assertEquals("\uFF09", new String(HanjaMapper.convertToHangul('\uFF09')));
  }
  
  public void testEitherHangulOrItselfBack() {
    for (int i = 0; i <= 0xFFFF; i++) {
      char res[] = HanjaMapper.convertToHangul((char)i);
      if (res.length == 1 && res[0] == i) {
        continue;
      } else {
        assert res.length > 0;
        for (int j = 0; j < res.length; j++) {
          assertEquals(Character.UnicodeBlock.HANGUL_SYLLABLES, Character.UnicodeBlock.of(res[j]));
        }
      }
    }
  }
}
