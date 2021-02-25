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

package org.apache.lucene.search;

import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.LevenshteinAutomata;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;

/**
 * Builds a set of CompiledAutomaton for fuzzy matching on a given term, with specified maximum edit
 * distance, fixed prefix and whether or not to allow transpositions.
 */
class FuzzyAutomatonBuilder {

  private final String term;
  private final int maxEdits;
  private final LevenshteinAutomata levBuilder;
  private final String prefix;
  private final int termLength;

  FuzzyAutomatonBuilder(String term, int maxEdits, int prefixLength, boolean transpositions) {
    if (maxEdits < 0 || maxEdits > LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE) {
      throw new IllegalArgumentException(
          "max edits must be 0.."
              + LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE
              + ", inclusive; got: "
              + maxEdits);
    }
    if (prefixLength < 0) {
      throw new IllegalArgumentException("prefixLength cannot be less than 0");
    }
    this.term = term;
    this.maxEdits = maxEdits;
    int[] codePoints = stringToUTF32(term);
    this.termLength = codePoints.length;
    prefixLength = Math.min(prefixLength, codePoints.length);
    int[] suffix = new int[codePoints.length - prefixLength];
    System.arraycopy(codePoints, prefixLength, suffix, 0, suffix.length);
    this.levBuilder = new LevenshteinAutomata(suffix, Character.MAX_CODE_POINT, transpositions);
    this.prefix = UnicodeUtil.newString(codePoints, 0, prefixLength);
  }

  CompiledAutomaton[] buildAutomatonSet() {
    CompiledAutomaton[] compiled = new CompiledAutomaton[maxEdits + 1];
    for (int i = 0; i <= maxEdits; i++) {
      try {
        compiled[i] = new CompiledAutomaton(levBuilder.toAutomaton(i, prefix), true, false);
      } catch (TooComplexToDeterminizeException e) {
        throw new FuzzyTermsEnum.FuzzyTermsException(term, e);
      }
    }
    return compiled;
  }

  CompiledAutomaton buildMaxEditAutomaton() {
    try {
      return new CompiledAutomaton(levBuilder.toAutomaton(maxEdits, prefix), true, false);
    } catch (TooComplexToDeterminizeException e) {
      throw new FuzzyTermsEnum.FuzzyTermsException(term, e);
    }
  }

  int getTermLength() {
    return this.termLength;
  }

  private static int[] stringToUTF32(String text) {
    int[] termText = new int[text.codePointCount(0, text.length())];
    for (int cp, i = 0, j = 0; i < text.length(); i += Character.charCount(cp)) {
      termText[j++] = cp = text.codePointAt(i);
    }
    return termText;
  }
}
