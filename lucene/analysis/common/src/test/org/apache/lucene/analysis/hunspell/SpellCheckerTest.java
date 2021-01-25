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

import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.IOUtils;
import org.junit.Test;

public class SpellCheckerTest extends StemmerTestBase {
  @Test
  public void base() throws Exception {
    doTest("base");
  }

  @Test
  public void keepcase() throws Exception {
    doTest("keepcase");
  }

  @Test
  public void allcaps() throws Exception {
    doTest("allcaps");
  }

  @Test
  public void i53643_numbersWithSeparators() throws Exception {
    doTest("i53643");
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

  protected void doTest(String name) throws Exception {
    InputStream affixStream =
        Objects.requireNonNull(getClass().getResourceAsStream(name + ".aff"), name);
    InputStream dictStream =
        Objects.requireNonNull(getClass().getResourceAsStream(name + ".dic"), name);

    SpellChecker speller;
    try {
      Dictionary dictionary =
          new Dictionary(new ByteBuffersDirectory(), "dictionary", affixStream, dictStream);
      speller = new SpellChecker(dictionary);
    } finally {
      IOUtils.closeWhileHandlingException(affixStream);
      IOUtils.closeWhileHandlingException(dictStream);
    }

    URL good = StemmerTestBase.class.getResource(name + ".good");
    if (good != null) {
      for (String word : Files.readAllLines(Path.of(good.toURI()))) {
        assertTrue("Unexpectedly considered misspelled: " + word, speller.spell(word));
      }
    }

    URL wrong = StemmerTestBase.class.getResource(name + ".wrong");
    if (wrong != null) {
      for (String word : Files.readAllLines(Path.of(wrong.toURI()))) {
        assertFalse("Unexpectedly considered correct: " + word, speller.spell(word));
      }
    }
  }
}
