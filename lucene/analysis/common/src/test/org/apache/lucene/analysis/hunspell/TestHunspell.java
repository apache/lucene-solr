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

import static org.apache.lucene.analysis.hunspell.StemmerTestBase.loadDictionary;
import static org.apache.lucene.analysis.hunspell.TimeoutPolicy.NO_TIMEOUT;
import static org.apache.lucene.analysis.hunspell.TimeoutPolicy.RETURN_PARTIAL_RESULT;
import static org.apache.lucene.analysis.hunspell.TimeoutPolicy.THROW_EXCEPTION;

import java.io.IOException;
import java.text.ParseException;
import java.util.Collections;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

public class TestHunspell extends LuceneTestCase {
  public void testCheckCanceled() throws Exception {
    AtomicBoolean canceled = new AtomicBoolean();
    Runnable checkCanceled =
        () -> {
          if (canceled.get()) {
            throw new CancellationException();
          }
        };
    Dictionary dictionary = loadDictionary(false, "simple.aff", "simple.dic");
    Hunspell hunspell = new Hunspell(dictionary, NO_TIMEOUT, checkCanceled);

    assertTrue(hunspell.spell("apache"));
    assertEquals(Collections.singletonList("apach"), hunspell.suggest("apac"));

    canceled.set(true);
    assertThrows(CancellationException.class, () -> hunspell.spell("apache"));
    assertThrows(CancellationException.class, () -> hunspell.suggest("apac"));
  }

  public void testSuggestionTimeLimit() throws IOException, ParseException {
    int timeLimitMs = 10;

    Dictionary dictionary = loadDictionary(false, "timelimit.aff", "simple.dic");
    String word = "qazwsxedcrfvtgbyhnujm";

    Hunspell incomplete = new Hunspell(dictionary, RETURN_PARTIAL_RESULT, () -> {});
    assertFalse(incomplete.spell(word));
    assertEquals(Collections.emptyList(), incomplete.suggest(word, timeLimitMs));

    Hunspell throwing = new Hunspell(dictionary, THROW_EXCEPTION, () -> {});
    assertFalse(throwing.spell(word));

    SuggestionTimeoutException exception =
        assertThrows(SuggestionTimeoutException.class, () -> throwing.suggest(word, timeLimitMs));
    assertEquals(Collections.emptyList(), exception.getPartialResult());

    Hunspell noLimit = new Hunspell(dictionary, NO_TIMEOUT, () -> {});
    assertEquals(Collections.emptyList(), noLimit.suggest(word.substring(0, 5), timeLimitMs));
  }

  @Test
  public void testStemmingApi() throws Exception {
    Dictionary dictionary = loadDictionary(false, "simple.aff", "simple.dic");
    Hunspell hunspell = new Hunspell(dictionary, TimeoutPolicy.NO_TIMEOUT, () -> {});
    assertEquals(Collections.singletonList("apach"), hunspell.getRoots("apache"));
    assertEquals(Collections.singletonList("foo"), hunspell.getRoots("foo"));
  }
}
