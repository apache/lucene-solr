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
package org.apache.lucene.analysis.synonym;


import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.Util;

/**
 * Base class for testing synonym parsers.
 */
public abstract class BaseSynonymParserTestCase extends BaseTokenStreamTestCase {
  /**
   * Helper method to validate synonym parsing.
   *
   * @param synonynMap  the generated synonym map after parsing
   * @param word        word (phrase) we are validating the synonyms for. Should be the value that comes out of the analyzer.
   *                    All spaces will be replaced by word separators.
   * @param includeOrig if synonyms should include original
   * @param synonyms    actual synonyms. All word separators are replaced with a single space.
   */
  public static void assertEntryEquals(SynonymMap synonynMap, String word, boolean includeOrig, String[] synonyms)
      throws Exception {
    word = word.replace(' ', SynonymMap.WORD_SEPARATOR);
    BytesRef value = Util.get(synonynMap.fst, Util.toUTF32(new CharsRef(word), new IntsRefBuilder()));
    assertNotNull("No synonyms found for: " + word, value);

    ByteArrayDataInput bytesReader = new ByteArrayDataInput(value.bytes, value.offset, value.length);
    final int code = bytesReader.readVInt();

    final boolean keepOrig = (code & 0x1) == 0;
    assertEquals("Include original different than expected. Expected " + includeOrig + " was " + keepOrig,
        includeOrig, keepOrig);

    final int count = code >>> 1;
    assertEquals("Invalid synonym count. Expected " + synonyms.length + " was " + count,
        synonyms.length, count);

    Set<String> synonymSet = new HashSet<>(Arrays.asList(synonyms));

    BytesRef scratchBytes = new BytesRef();
    for (int i = 0; i < count; i++) {
      synonynMap.words.get(bytesReader.readVInt(), scratchBytes);
      String synonym = scratchBytes.utf8ToString().replace(SynonymMap.WORD_SEPARATOR, ' ');
      assertTrue("Unexpected synonym found: " + synonym, synonymSet.contains(synonym));
    }
  }

  /**
   * Validates that there are no synonyms for the given word.
   * @param synonynMap  the generated synonym map after parsing
   * @param word        word (phrase) we are validating the synonyms for. Should be the value that comes out of the analyzer.
   *                    All spaces will be replaced by word separators.
   */
  public static void assertEntryAbsent(SynonymMap synonynMap, String word) throws IOException {
    word = word.replace(' ', SynonymMap.WORD_SEPARATOR);
    BytesRef value = Util.get(synonynMap.fst, Util.toUTF32(new CharsRef(word), new IntsRefBuilder()));
    assertNull("There should be no synonyms for: " + word, value);
  }

  public static void assertEntryEquals(SynonymMap synonynMap, String word, boolean includeOrig, String synonym)
      throws Exception {
    assertEntryEquals(synonynMap, word, includeOrig, new String[]{synonym});
  }

  public static void assertAnalyzesToPositions(Analyzer a, String input, String[] output, String[] types, int[] posIncrements, int[] posLengths) throws IOException {
    assertAnalyzesTo(a, input, output, null, null, types, posIncrements, posLengths);
  }
}
