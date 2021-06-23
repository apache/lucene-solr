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

import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.Operations;

/**
 * An automaton allowing to achieve the same results as non-weighted {@link
 * GeneratingSuggester#ngramScore}, but faster (in O(s2.length) time).
 */
class TrigramAutomaton {
  private static final int N = 3;
  private final CharacterRunAutomaton automaton;
  private final int[] state2Score;
  private final FixedBitSet countedSubstrings;
  private final char minChar;

  TrigramAutomaton(String s1) {
    Map<String, Integer> substringCounts = new HashMap<>();

    Automaton.Builder builder = new Automaton.Builder(s1.length() * N, s1.length() * N);
    int initialState = builder.createState();

    minChar = (char) s1.chars().min().orElseThrow(AssertionError::new);

    for (int start = 0; start < s1.length(); start++) {
      int limit = Math.min(s1.length(), start + N);
      for (int end = start + 1; end <= limit; end++) {
        substringCounts.merge(s1.substring(start, end), 1, Integer::sum);
      }

      int state = initialState;
      for (int i = start; i < limit; i++) {
        int next = builder.createState();
        builder.addTransition(state, next, s1.charAt(i) - minChar);
        state = next;
      }
    }

    automaton =
        new CharacterRunAutomaton(
            Operations.determinize(builder.finish(), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT));

    state2Score = new int[automaton.getSize()];
    for (Map.Entry<String, Integer> entry : substringCounts.entrySet()) {
      int state = runAutomatonOnStringChars(entry.getKey());
      assert state2Score[state] == 0;
      state2Score[state] = entry.getValue();
    }
    countedSubstrings = new FixedBitSet(state2Score.length);
  }

  private int runAutomatonOnStringChars(String s) {
    int state = 0;
    for (int i = 0; i < s.length(); i++) {
      state = automaton.step(state, s.charAt(i) - minChar);
    }
    return state;
  }

  int ngramScore(CharsRef s2) {
    countedSubstrings.clear(0, countedSubstrings.length());

    int score1 = 0, score2 = 0, score3 = 0; // scores for substrings of length 1, 2 and 3

    // states of running the automaton on substrings [i-1, i) and [i-2, i)
    int state1 = -1, state2 = -1;

    int limit = s2.length + s2.offset;
    for (int i = s2.offset; i < limit; i++) {
      char c = transformChar(s2.chars[i]);
      if (c < minChar) {
        state1 = state2 = -1;
        continue;
      }
      c -= minChar;

      int state3 = state2 <= 0 ? 0 : automaton.step(state2, c);
      if (state3 > 0) {
        score3 += substringScore(state3, countedSubstrings);
      }

      state2 = state1 <= 0 ? 0 : automaton.step(state1, c);
      if (state2 > 0) {
        score2 += substringScore(state2, countedSubstrings);
      }

      state1 = automaton.step(0, c);
      if (state1 > 0) {
        score1 += substringScore(state1, countedSubstrings);
      }
    }

    int score = score1;
    if (score1 >= 2) {
      score += score2;
      if (score2 >= 2) {
        score += score3;
      }
    }
    return score;
  }

  char transformChar(char c) {
    return c;
  }

  private int substringScore(int state, FixedBitSet countedSubstrings) {
    if (countedSubstrings.getAndSet(state)) return 0;

    int score = state2Score[state];
    assert score > 0;
    return score;
  }
}
