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

public class SpellCheckerTest extends StemmerTestBase {

  public void testBreak() throws Exception {
    doTest("break");
  }

  public void testBreakDefault() throws Exception {
    doTest("breakdefault");
  }

  public void testBreakOff() throws Exception {
    doTest("breakoff");
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
