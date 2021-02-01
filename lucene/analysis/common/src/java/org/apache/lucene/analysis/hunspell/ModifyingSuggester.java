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

import java.util.Arrays;
import java.util.LinkedHashSet;

class ModifyingSuggester {
  private final LinkedHashSet<String> result = new LinkedHashSet<>();
  private final char[] tryChars;
  private final SpellChecker speller;

  ModifyingSuggester(SpellChecker speller) {
    this.speller = speller;
    tryChars = speller.dictionary.tryChars.toCharArray();
  }

  LinkedHashSet<String> suggest(String word) {
    tryRep(word);
    tryAddingChar(word);
    return result;
  }

  private void tryRep(String word) {
    for (RepEntry entry : speller.dictionary.repTable) {
      for (String candidate : entry.substitute(word)) {
        if (trySuggestion(candidate)) {
          continue;
        }

        if (candidate.contains(" ")
            && Arrays.stream(candidate.split(" ")).allMatch(speller::checkWord)) {
          result.add(candidate);
        }
      }
    }
  }

  private void tryAddingChar(String word) {
    for (int i = 0; i <= word.length(); i++) {
      String prefix = word.substring(0, i);
      String suffix = word.substring(i);
      for (char toInsert : tryChars) {
        trySuggestion(prefix + toInsert + suffix);
      }
    }
  }

  private boolean trySuggestion(String candidate) {
    if (speller.checkWord(candidate)) {
      result.add(candidate);
      return true;
    }
    return false;
  }
}
