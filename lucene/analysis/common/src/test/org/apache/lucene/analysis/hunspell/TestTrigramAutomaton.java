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

import java.util.stream.Collectors;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.LuceneTestCase;

public class TestTrigramAutomaton extends LuceneTestCase {
  public void testSameScore() {
    checkScores("look", "looked");
    checkScores("look", "cool");
    checkScores("abracadabra", "abraham");
  }

  public void testRandomized() {
    String[] alphabet = {
      "a",
      "b",
      "c",
      "aa",
      "ab",
      "abc",
      "ccc",
      "\uD800\uDFD1",
      "\uD800\uDFD2",
      "\uD800\uDFD2\uD800\uDFD1",
      "\uD800\uDFD2\uD800\uDFD2"
    };
    for (int i = 0; i < 100; i++) {
      checkScores(randomConcatenation(alphabet), randomConcatenation(alphabet));
    }
  }

  private String randomConcatenation(String[] alphabet) {
    return random()
        .ints(0, alphabet.length)
        .limit(random().nextInt(20) + 1)
        .mapToObj(i -> alphabet[i])
        .collect(Collectors.joining());
  }

  private void checkScores(String s1, String s2) {
    String message = "Fails: checkScores(\"" + s1 + "\", \"" + s2 + "\")";
    assertEquals(
        message,
        GeneratingSuggester.ngramScore(3, s1, s2, false),
        new TrigramAutomaton(s1).ngramScore(new CharsRef(s2)));
    assertEquals(
        message,
        GeneratingSuggester.ngramScore(3, s2, s1, false),
        new TrigramAutomaton(s2).ngramScore(new CharsRef(s1)));
  }
}
