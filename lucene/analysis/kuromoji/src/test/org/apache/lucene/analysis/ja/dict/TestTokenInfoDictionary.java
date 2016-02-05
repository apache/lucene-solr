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
package org.apache.lucene.analysis.ja.dict;


import org.apache.lucene.analysis.ja.util.ToStringUtil;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.IntsRefFSTEnum;
import org.apache.lucene.util.fst.IntsRefFSTEnum.InputOutput;

public class TestTokenInfoDictionary extends LuceneTestCase {

  /** enumerates the entire FST/lookup data and just does basic sanity checks */
  public void testEnumerateAll() throws Exception {
    // just for debugging
    int numTerms = 0;
    int numWords = 0;
    int lastWordId = -1;
    int lastSourceId = -1;
    TokenInfoDictionary tid = TokenInfoDictionary.getInstance();
    ConnectionCosts matrix = ConnectionCosts.getInstance();
    FST<Long> fst = tid.getFST().getInternalFST();
    IntsRefFSTEnum<Long> fstEnum = new IntsRefFSTEnum<>(fst);
    InputOutput<Long> mapping;
    IntsRef scratch = new IntsRef();
    while ((mapping = fstEnum.next()) != null) {
      numTerms++;
      IntsRef input = mapping.input;
      char chars[] = new char[input.length];
      for (int i = 0; i < chars.length; i++) {
        chars[i] = (char)input.ints[input.offset+i];
      }
      assertTrue(UnicodeUtil.validUTF16String(new String(chars)));
      
      Long output = mapping.output;
      int sourceId = output.intValue();
      // we walk in order, terms, sourceIds, and wordIds should always be increasing
      assertTrue(sourceId > lastSourceId);
      lastSourceId = sourceId;
      tid.lookupWordIds(sourceId, scratch);
      for (int i = 0; i < scratch.length; i++) {
        numWords++;
        int wordId = scratch.ints[scratch.offset+i];
        assertTrue(wordId > lastWordId);
        lastWordId = wordId;
         
        String baseForm = tid.getBaseForm(wordId, chars, 0, chars.length);
        assertTrue(baseForm == null || UnicodeUtil.validUTF16String(baseForm));
        
        String inflectionForm = tid.getInflectionForm(wordId);
        assertTrue(inflectionForm == null || UnicodeUtil.validUTF16String(inflectionForm));
        if (inflectionForm != null) {
          // check that it's actually an ipadic inflection form
          assertNotNull(ToStringUtil.getInflectedFormTranslation(inflectionForm));          
        }
        
        String inflectionType = tid.getInflectionType(wordId);
        assertTrue(inflectionType == null || UnicodeUtil.validUTF16String(inflectionType));
        if (inflectionType != null) {
          // check that it's actually an ipadic inflection type
          assertNotNull(ToStringUtil.getInflectionTypeTranslation(inflectionType));
        }
        
        int leftId = tid.getLeftId(wordId);
        int rightId = tid.getRightId(wordId);
        
        matrix.get(rightId, leftId);
        
        tid.getWordCost(wordId);
        
        String pos = tid.getPartOfSpeech(wordId);
        assertNotNull(pos);
        assertTrue(UnicodeUtil.validUTF16String(pos));
        // check that it's actually an ipadic pos tag
        assertNotNull(ToStringUtil.getPOSTranslation(pos));
        
        String pronunciation = tid.getPronunciation(wordId, chars, 0, chars.length);
        assertNotNull(pronunciation);
        assertTrue(UnicodeUtil.validUTF16String(pronunciation));
        
        String reading = tid.getReading(wordId, chars, 0, chars.length);
        assertNotNull(reading);
        assertTrue(UnicodeUtil.validUTF16String(reading));
      }
    }
    if (VERBOSE) {
      System.out.println("checked " + numTerms + " terms, " + numWords + " words.");
    }
  }
}
