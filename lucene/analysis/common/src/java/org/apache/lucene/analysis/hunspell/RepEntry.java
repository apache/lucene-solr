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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class RepEntry {
  private final String pattern;
  private final String replacement;
  private final boolean mustStart;
  private final boolean mustEnd;
  private final int patternLen;

  RepEntry(String rawPattern, String rawReplacement) {
    mustStart = rawPattern.startsWith("^");
    mustEnd = rawPattern.endsWith("$");
    pattern = rawPattern.substring(mustStart ? 1 : 0, rawPattern.length() - (mustEnd ? 1 : 0));
    replacement = rawReplacement.replace('_', ' ');
    patternLen = pattern.length();
  }

  boolean isMiddle() {
    return !mustStart && !mustEnd;
  }

  List<String> substitute(String word) {
    if (mustStart) {
      boolean matches = mustEnd ? word.equals(pattern) : word.startsWith(pattern);
      return matches
          ? Collections.singletonList(replacement + word.substring(patternLen))
          : Collections.emptyList();
    }

    if (mustEnd) {
      return word.endsWith(pattern)
          ? Collections.singletonList(word.substring(0, word.length() - patternLen) + replacement)
          : Collections.emptyList();
    }

    int pos = word.indexOf(pattern);
    if (pos < 0) return Collections.emptyList();

    List<String> result = new ArrayList<>();
    while (pos >= 0) {
      result.add(word.substring(0, pos) + replacement + word.substring(pos + patternLen));
      pos = word.indexOf(pattern, pos + 1);
    }
    return result;
  }

  @Override
  public String toString() {
    return (mustStart ? "^" : "") + pattern + (mustEnd ? "$" : "") + "->" + replacement;
  }
}
