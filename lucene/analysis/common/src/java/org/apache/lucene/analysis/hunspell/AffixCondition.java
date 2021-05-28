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

import java.util.regex.PatternSyntaxException;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.RegExp;

/**
 * Checks the "condition" part of affix definition, as in
 *
 * <pre>PFX flag stripping prefix [condition [morphological_fields...]]</pre>
 */
abstract class AffixCondition {
  public static final String ALWAYS_TRUE_KEY = ".*";
  public static final AffixCondition ALWAYS_TRUE = new AffixCondition() {
    @Override
    public boolean acceptsStem(char[] word, int offset, int length) {
      return true;
    }
  };
  public static final AffixCondition ALWAYS_FALSE = new AffixCondition() {
    @Override
    public boolean acceptsStem(char[] word, int offset, int length) {
      return false;
    }
  };

  public boolean acceptsStem(String stem) {
    return acceptsStem(stem.toCharArray(), 0, stem.length());
  }

  /**
   * @return whether the given word matches this condition as a stem with both "strip" and "affix"
   *     removed
   */
  public abstract boolean acceptsStem(char[] word, int offset, int length);

  /**
   * @return a key used to deduplicate same condition+strip+kind triples. For trivial conditions
   *     that need no check, {@link #ALWAYS_TRUE_KEY} is returned.
   */
  public static String uniqueKey(AffixKind kind, String strip, String condition) {
    if (".".equals(condition)
        || kind == PREFIX && strip.startsWith(condition)
        || kind == SUFFIX && strip.endsWith(condition) && !isRegexp(condition)) {
      return ALWAYS_TRUE_KEY;
    }
    return condition + " " + kind + " " + strip;
  }

  /**
   * Analyzes the given affix kind, strip and condition and returns an object able to efficiently
   * check that condition.
   */
  public static AffixCondition compile(AffixKind kind, String strip, String condition, String line) {
    if (!isRegexp(condition)) {
      if (kind == SUFFIX && condition.endsWith(strip)) {
        return substringCondition(
            kind, condition.substring(0, condition.length() - strip.length()));
      }
      if (kind == PREFIX && condition.startsWith(strip)) {
        return substringCondition(kind, condition.substring(strip.length()));
      }
      return ALWAYS_FALSE;
    }

    int lastBracket = condition.lastIndexOf('[');
    if (lastBracket >= 0 && condition.indexOf(']', lastBracket + 1) < 0) {
      // unclosed [ is tolerated by Hunspell and occurs in some dictionaries
      condition = condition + "]";
    }

    try {
      int conditionChars = countCharPatterns(condition);
      if (conditionChars <= strip.length()) {
        String regex = kind == PREFIX ? ".*" + condition : condition + ".*";
        return strip.matches(regex) ? ALWAYS_TRUE : ALWAYS_FALSE;
      }

      if (kind == PREFIX) {
        int split = skipCharPatterns(condition, strip.length());
        if (!strip.matches(condition.substring(0, split))) {
          return ALWAYS_FALSE;
        }
        return regexpCondition(kind, condition.substring(split), conditionChars - strip.length());
      }

      int split = skipCharPatterns(condition, conditionChars - strip.length());
      if (!strip.matches(condition.substring(split))) {
        return ALWAYS_FALSE;
      }
      return regexpCondition(kind, condition.substring(0, split), conditionChars - strip.length());
    } catch (
        @SuppressWarnings("unused")
        PatternSyntaxException e) {
      return ALWAYS_FALSE;
    } catch (Throwable e) {
      throw new IllegalArgumentException("On line: " + line, e);
    }
  }

  public static int skipCharPatterns(String condition, int count) {
    int pos = 0;
    for (int i = 0; i < count; i++) pos = skipCharPattern(condition, pos);
    return pos;
  }

  public static int countCharPatterns(String condition) {
    int conditionChars = 0;
    for (int i = 0; i < condition.length(); i = skipCharPattern(condition, i)) conditionChars++;
    return conditionChars;
  }

  public static int skipCharPattern(String condition, int pos) {
    if (condition.charAt(pos) == '[') {
      pos = condition.indexOf(']', pos + 1);
      if (pos < 0) {
        throw new AssertionError("Malformed condition " + condition);
      }
    }
    return pos + 1;
  }

  public static boolean isRegexp(String condition) {
    return condition.contains("[") || condition.contains(".") || condition.contains("-");
  }

  public static AffixCondition substringCondition(AffixKind kind, String stemCondition) {
    boolean forSuffix = kind == AffixKind.SUFFIX;
    int condLength = stemCondition.length();
    return new AffixCondition() {
      @Override
      public boolean acceptsStem(char[] word, int offset, int length) {
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
      }
    };
  }

  public static AffixCondition regexpCondition(AffixKind kind, String condition, int charCount) {
    boolean forSuffix = kind == AffixKind.SUFFIX;
    CharacterRunAutomaton automaton =
        new CharacterRunAutomaton(new RegExp(escapeDash(condition), RegExp.NONE).toAutomaton());
    return new AffixCondition() {
      @Override
      public boolean acceptsStem(char[] word, int offset, int length) {
        return length >= charCount
            && automaton.run(word, forSuffix ? offset + length - charCount : offset, charCount);
      }
    };
  }

  // "dash hasn't got special meaning" (we must escape it)
  public static String escapeDash(String re) {
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
