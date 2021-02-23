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

import static org.apache.lucene.analysis.hunspell.AffixKind.PREFIX;
import static org.apache.lucene.analysis.hunspell.AffixKind.SUFFIX;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.RegExp;

/**
 * Checks the "condition" part of affix definition, as in
 *
 * <pre>PFX flag stripping prefix [condition [morphological_fields...]]</pre>
 */
interface AffixCondition {
  String ALWAYS_TRUE_KEY = ".*";
  AffixCondition ALWAYS_TRUE = (word, offset, length) -> true;
  AffixCondition ALWAYS_FALSE = (word, offset, length) -> false;

  default boolean acceptsStem(String stem) {
    return acceptsStem(stem.toCharArray(), 0, stem.length());
  }

  /**
   * @return whether the given word matches this condition as a stem with both "strip" and "affix"
   *     removed
   */
  boolean acceptsStem(char[] word, int offset, int length);

  /**
   * @return a key used to deduplicate same condition+strip+kind triples. For trivial conditions
   *     that need no check, {@link #ALWAYS_TRUE_KEY} is returned.
   */
  static String uniqueKey(AffixKind kind, String strip, String condition) {
    if (".".equals(condition)
        || kind == PREFIX && strip.startsWith(condition)
        || kind == SUFFIX && strip.endsWith(condition) && !isRegexp(condition)) {
      return ALWAYS_TRUE_KEY;
    }
    return kind == SUFFIX ? ".*" + condition : condition + ".*";
  }

  /**
   * Analyzes the given affix kind, strip and condition and returns an object able to efficiently
   * check that condition.
   */
  static AffixCondition compile(AffixKind kind, String strip, String condition, String line) {
    String stemCondition = removeStrip(strip, condition, line, kind);
    if (stemCondition == null) {
      return ALWAYS_FALSE;
    }

    if (stemCondition.isEmpty()) {
      return ALWAYS_TRUE;
    }

    if (!isRegexp(stemCondition)) {
      return substringCondition(kind, stemCondition);
    }

    try {
      return regexpCondition(kind, escapeDash(stemCondition));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("On line: " + line, e);
    }
  }

  private static boolean isRegexp(String condition) {
    return condition.contains("[") || condition.contains(".") || condition.contains("-");
  }

  /** Removes the "strip" from "condition", to check only the remaining stem part */
  private static String removeStrip(String strip, String condition, String line, AffixKind kind) {
    if (!isRegexp(condition)) {
      if (kind == SUFFIX && condition.endsWith(strip)) {
        return condition.substring(0, condition.length() - strip.length());
      }
      if (kind == PREFIX && condition.startsWith(strip)) {
        return condition.substring(strip.length());
      }
    }

    List<String> charPatterns = parse(condition);
    try {
      if (charPatterns.size() < strip.length()) {
        String regexp = unitePatterns(charPatterns);
        return strip.matches(kind == SUFFIX ? ".*" + regexp : regexp + ".*") ? "" : null;
      }

      int stripRangeStart = kind == PREFIX ? 0 : charPatterns.size() - strip.length();
      int stripRangeEnd = kind == PREFIX ? strip.length() : charPatterns.size();
      if (!strip.isEmpty()
          && !strip.matches(unitePatterns(charPatterns.subList(stripRangeStart, stripRangeEnd)))) {
        return null;
      }

      if (kind == PREFIX) {
        return unitePatterns(charPatterns.subList(stripRangeEnd, charPatterns.size()));
      }
      return unitePatterns(charPatterns.subList(0, stripRangeStart));
    } catch (PatternSyntaxException e) {
      throw new IllegalArgumentException("On line " + line, e);
    }
  }

  /** Produces a regexp from groups returned by {@link #parse} */
  private static String unitePatterns(List<String> charPatterns) {
    return charPatterns.stream()
        .map(s -> s.length() == 1 && "()?$^{}*+|\\".indexOf(s.charAt(0)) >= 0 ? "\\" + s : s)
        .collect(Collectors.joining());
  }

  /** Splits condition into small regexps, each standing for a single char to be matched. */
  private static List<String> parse(String condition) {
    List<String> groups = new ArrayList<>();
    for (int i = 0; i < condition.length(); i++) {
      char c = condition.charAt(i);
      if (c == '[') {
        int closing = condition.indexOf(']', i + 1);
        if (closing <= 0) {
          groups.add(condition.substring(i) + "]");
          break;
        }

        groups.add(condition.substring(i, closing + 1));
        i = closing;
      } else if (c == '.') {
        groups.add(".");
      } else {
        groups.add(String.valueOf(c));
      }
    }
    return groups;
  }

  private static AffixCondition substringCondition(AffixKind kind, String stemCondition) {
    boolean forSuffix = kind == AffixKind.SUFFIX;
    int condLength = stemCondition.length();
    return (word, offset, length) -> {
      if (length < condLength) {
        return false;
      }
      int matchStart = forSuffix ? offset + length - condLength : offset;
      for (int i = 0; i < condLength; i++) {
        if (stemCondition.charAt(i) != word[matchStart + i]) {
          return false;
        }
      }
      return true;
    };
  }

  private static AffixCondition regexpCondition(AffixKind kind, String stemCondition) {
    boolean forSuffix = kind == AffixKind.SUFFIX;
    int condLength = parse(stemCondition).size();
    CharacterRunAutomaton automaton =
        new CharacterRunAutomaton(conditionRegexp(stemCondition).toAutomaton());
    return (word, offset, length) ->
        length >= condLength
            && automaton.run(word, forSuffix ? offset + length - condLength : offset, condLength);
  }

  private static RegExp conditionRegexp(String regex) {
    try {
      return new RegExp(regex, RegExp.NONE);
    } catch (IllegalArgumentException e) {
      if (e.getMessage().contains("expected ']'")) {
        return conditionRegexp(regex + "]");
      }
      throw e;
    }
  }

  // "dash hasn't got special meaning" (we must escape it)
  private static String escapeDash(String re) {
    if (!re.contains("-")) return re;

    // we have to be careful, even though dash doesn't have a special meaning,
    // some dictionaries already escape it (e.g. pt_PT), so we don't want to nullify it
    StringBuilder escaped = new StringBuilder();
    for (int i = 0; i < re.length(); i++) {
      char c = re.charAt(i);
      if (c == '-') {
        escaped.append("\\-");
      } else {
        escaped.append(c);
        if (c == '\\' && i + 1 < re.length()) {
          escaped.append(re.charAt(i + 1));
          i++;
        }
      }
    }
    return escaped.toString();
  }
}
