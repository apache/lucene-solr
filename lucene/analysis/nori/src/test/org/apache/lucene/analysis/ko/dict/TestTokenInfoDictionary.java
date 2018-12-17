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
package org.apache.lucene.analysis.ko.dict;

import org.apache.lucene.analysis.ko.POS;
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
    CharacterDefinition charDef = CharacterDefinition.getInstance();
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
      String surfaceForm = new String(chars);
      assertFalse(surfaceForm.isEmpty());
      assertEquals(surfaceForm.trim(), surfaceForm);
      assertTrue(UnicodeUtil.validUTF16String(surfaceForm));
      
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

        int leftId = tid.getLeftId(wordId);
        int rightId = tid.getRightId(wordId);

        matrix.get(rightId, leftId);

        tid.getWordCost(wordId);

        POS.Type type = tid.getPOSType(wordId);
        POS.Tag leftPOS = tid.getLeftPOS(wordId);
        POS.Tag rightPOS = tid.getRightPOS(wordId);

        if (type == POS.Type.MORPHEME) {
          assertTrue(leftPOS == rightPOS);
          String reading = tid.getReading(wordId);
          boolean isHanja = charDef.isHanja(surfaceForm.charAt(0));
          if (isHanja) {
            assertTrue(reading != null);
            for (int j = 0; j < reading.length(); j++) {
              assertTrue(charDef.isHangul(reading.charAt(j)));
            }
          }
          if (reading != null) {
            assertTrue(UnicodeUtil.validUTF16String(reading));
          }
        } else {
          if (type == POS.Type.COMPOUND) {
            assertTrue(leftPOS == rightPOS);
            assertTrue(leftPOS == POS.Tag.NNG || rightPOS == POS.Tag.NNP);
          }
          Dictionary.Morpheme[] decompound = tid.getMorphemes(wordId,  chars, 0, chars.length);
          if (decompound != null) {
            int offset = 0;
            for (Dictionary.Morpheme morph : decompound) {
              assertTrue(UnicodeUtil.validUTF16String(morph.surfaceForm));
              assertFalse(morph.surfaceForm.isEmpty());
              assertEquals(morph.surfaceForm.trim(), morph.surfaceForm);
              if (type != POS.Type.INFLECT) {
                assertEquals(morph.surfaceForm, surfaceForm.substring(offset, offset + morph.surfaceForm.length()));
                offset += morph.surfaceForm.length();
              }
            }
            assertTrue(offset <= surfaceForm.length());
          }
        }
      }
    }
    if (VERBOSE) {
      System.out.println("checked " + numTerms + " terms, " + numWords + " words.");
    }
  }
}
