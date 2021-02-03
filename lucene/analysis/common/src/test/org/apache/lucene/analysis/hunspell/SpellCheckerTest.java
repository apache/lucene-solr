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
package org.apache.lucene.analysis.hunspell;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.IOUtils;

public class SpellCheckerTest extends StemmerTestBase {

  public void testBase() throws Exception {
    doTest("base");
  }

  public void testBaseUtf() throws Exception {
    doTest("base_utf");
  }

  public void testKeepcase() throws Exception {
    doTest("keepcase");
  }

  public void testAllcaps() throws Exception {
    doTest("allcaps");
  }

  public void testRepSuggestions() throws Exception {
    doTest("rep");
  }

  public void testPhSuggestions() throws Exception {
    doTest("ph");
  }

  public void testPhSuggestions2() throws Exception {
    doTest("ph2");
  }

  public void testForceUCase() throws Exception {
    doTest("forceucase");
  }

  public void testCheckSharpS() throws Exception {
    doTest("checksharps");
  }

  public void testIJ() throws Exception {
    doTest("IJ");
  }

  public void testI53643_numbersWithSeparators() throws Exception {
    doTest("i53643");
  }

  public void testCheckCompoundPattern() throws Exception {
    doTest("checkcompoundpattern");
  }

  public void testCheckCompoundPattern2() throws Exception {
    doTest("checkcompoundpattern2");
  }

  public void testCheckCompoundPattern3() throws Exception {
    doTest("checkcompoundpattern3");
  }

  public void testDotless_i() throws Exception {
    doTest("dotless_i");
  }

  public void testNeedAffixOnAffixes() throws Exception {
    doTest("needaffix5");
  }

  public void testCompoundFlag() throws Exception {
    doTest("compoundflag");
  }

  public void testCheckCompoundCase() throws Exception {
    doTest("checkcompoundcase");
  }

  public void testCheckCompoundDup() throws Exception {
    doTest("checkcompounddup");
  }

  public void testCheckCompoundTriple() throws Exception {
    doTest("checkcompoundtriple");
  }

  public void testSimplifiedTriple() throws Exception {
    doTest("simplifiedtriple");
  }

  public void testCompoundForbid() throws Exception {
    doTest("compoundforbid");
  }

  public void testBreak() throws Exception {
    doTest("break");
  }

  public void testBreakDefault() throws Exception {
    doTest("breakdefault");
  }

  public void testBreakOff() throws Exception {
    doTest("breakoff");
  }

  public void testCheckCompoundRep() throws Exception {
    doTest("checkcompoundrep");
  }

  public void testCompoundrule() throws Exception {
    doTest("compoundrule");
  }

  public void testCompoundrule2() throws Exception {
    doTest("compoundrule2");
  }

  public void testCompoundrule3() throws Exception {
    doTest("compoundrule3");
  }

  public void testCompoundrule4() throws Exception {
    doTest("compoundrule4");
  }

  public void testCompoundrule5() throws Exception {
    doTest("compoundrule5");
  }

  public void testCompoundrule6() throws Exception {
    doTest("compoundrule6");
  }

  public void testCompoundrule7() throws Exception {
    doTest("compoundrule7");
  }

  public void testCompoundrule8() throws Exception {
    doTest("compoundrule8");
  }

  public void testDisallowCompoundOnlySuffixesAtTheVeryEnd() throws Exception {
    doTest("onlyincompound2");
  }

  public void testGermanCompounding() throws Exception {
    doTest("germancompounding");
  }

  public void testModifyingSuggestions() throws Exception {
    doTest("sug");
  }

  public void testModifyingSuggestions2() throws Exception {
    doTest("sug2");
  }

  protected void doTest(String name) throws Exception {
    checkSpellCheckerExpectations(
        Path.of(getClass().getResource(name + ".aff").toURI()).getParent().resolve(name), true);
  }

  static void checkSpellCheckerExpectations(Path basePath, boolean checkSuggestions)
      throws IOException, ParseException {
    InputStream affixStream = Files.newInputStream(Path.of(basePath.toString() + ".aff"));
    InputStream dictStream = Files.newInputStream(Path.of(basePath.toString() + ".dic"));

    SpellChecker speller;
    try {
      Dictionary dictionary =
          new Dictionary(new ByteBuffersDirectory(), "dictionary", affixStream, dictStream);
      speller = new SpellChecker(dictionary);
    } finally {
      IOUtils.closeWhileHandlingException(affixStream);
      IOUtils.closeWhileHandlingException(dictStream);
    }

    Path good = Path.of(basePath + ".good");
    if (Files.exists(good)) {
      for (String word : Files.readAllLines(good)) {
        assertTrue("Unexpectedly considered misspelled: " + word, speller.spell(word.trim()));
      }
    }

    Path wrong = Path.of(basePath + ".wrong");
    Path sug = Path.of(basePath + ".sug");
    if (Files.exists(wrong)) {
      List<String> wrongWords = Files.readAllLines(wrong);
      for (String word : wrongWords) {
        assertFalse("Unexpectedly considered correct: " + word, speller.spell(word.trim()));
      }
      if (Files.exists(sug) && checkSuggestions) {
        String suggestions =
            wrongWords.stream()
                .map(s -> String.join(", ", speller.suggest(s)))
                .filter(s -> !s.isEmpty())
                .collect(Collectors.joining("\n"));
        assertEquals(Files.readString(sug).trim(), suggestions);
      }
    } else {
      assertFalse(".sug file without .wrong file!", Files.exists(sug));
    }
  }
}
