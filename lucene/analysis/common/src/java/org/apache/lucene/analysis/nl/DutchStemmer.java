package org.apache.lucene.analysis.nl;

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

import java.util.Map;
import java.util.Locale;

/**
 * A stemmer for Dutch words. 
 * <p>
 * The algorithm is an implementation of
 * the <a href="http://snowball.tartarus.org/algorithms/dutch/stemmer.html">dutch stemming</a>
 * algorithm in Martin Porter's snowball project.
 * </p>
 * @deprecated (3.1) Use {@link org.tartarus.snowball.ext.DutchStemmer} instead, 
 * which has the same functionality. This filter will be removed in Lucene 5.0
 */
@Deprecated
public class DutchStemmer {
  private static final Locale locale = new Locale("nl", "NL");

  /**
   * Buffer for the terms while stemming them.
   */
  private StringBuilder sb = new StringBuilder();
  private boolean _removedE;
  private Map _stemDict;

  private int _R1;
  private int _R2;

  //TODO convert to internal
  /*
   * Stems the given term to an unique <tt>discriminator</tt>.
   *
   * @param term The term that should be stemmed.
   * @return Discriminator for <tt>term</tt>
   */
  public String stem(String term) {
    term = term.toLowerCase(locale);
    if (!isStemmable(term))
      return term;
    if (_stemDict != null && _stemDict.containsKey(term))
      if (_stemDict.get(term) instanceof String)
        return (String) _stemDict.get(term);
      else
        return null;

    // Reset the StringBuilder.
    sb.delete(0, sb.length());
    sb.insert(0, term);
    // Stemming starts here...
    substitute(sb);
    storeYandI(sb);
    _R1 = getRIndex(sb, 0);
    _R1 = Math.max(3, _R1);
    step1(sb);
    step2(sb);
    _R2 = getRIndex(sb, _R1);
    step3a(sb);
    step3b(sb);
    step4(sb);
    reStoreYandI(sb);
    return sb.toString();
  }

  private boolean enEnding(StringBuilder sb) {
    String[] enend = new String[]{"ene", "en"};
    for (int i = 0; i < enend.length; i++) {
      String end = enend[i];
      String s = sb.toString();
      int index = s.length() - end.length();
      if (s.endsWith(end) &&
          index >= _R1 &&
          isValidEnEnding(sb, index - 1)
      ) {
        sb.delete(index, index + end.length());
        unDouble(sb, index);
        return true;
      }
    }
    return false;
  }


  private void step1(StringBuilder sb) {
    if (_R1 >= sb.length())
      return;

    String s = sb.toString();
    int lengthR1 = sb.length() - _R1;
    int index;

    if (s.endsWith("heden")) {
      sb.replace(_R1, lengthR1 + _R1, sb.substring(_R1, lengthR1 + _R1).replaceAll("heden", "heid"));
      return;
    }

    if (enEnding(sb))
      return;

    if (s.endsWith("se") &&
        (index = s.length() - 2) >= _R1 &&
        isValidSEnding(sb, index - 1)
    ) {
      sb.delete(index, index + 2);
      return;
    }
    if (s.endsWith("s") &&
        (index = s.length() - 1) >= _R1 &&
        isValidSEnding(sb, index - 1)) {
      sb.delete(index, index + 1);
    }
  }

  /**
   * Delete suffix e if in R1 and
   * preceded by a non-vowel, and then undouble the ending
   *
   * @param sb String being stemmed
   */
  private void step2(StringBuilder sb) {
    _removedE = false;
    if (_R1 >= sb.length())
      return;
    String s = sb.toString();
    int index = s.length() - 1;
    if (index >= _R1 &&
        s.endsWith("e") &&
        !isVowel(sb.charAt(index - 1))) {
      sb.delete(index, index + 1);
      unDouble(sb);
      _removedE = true;
    }
  }

  /**
   * Delete "heid"
   *
   * @param sb String being stemmed
   */
  private void step3a(StringBuilder sb) {
    if (_R2 >= sb.length())
      return;
    String s = sb.toString();
    int index = s.length() - 4;
    if (s.endsWith("heid") && index >= _R2 && sb.charAt(index - 1) != 'c') {
      sb.delete(index, index + 4); //remove heid
      enEnding(sb);
    }
  }

  /**
   * <p>A d-suffix, or derivational suffix, enables a new word,
   * often with a different grammatical category, or with a different
   * sense, to be built from another word. Whether a d-suffix can be
   * attached is discovered not from the rules of grammar, but by
   * referring to a dictionary. So in English, ness can be added to
   * certain adjectives to form corresponding nouns (littleness,
   * kindness, foolishness ...) but not to all adjectives
   * (not for example, to big, cruel, wise ...) d-suffixes can be
   * used to change meaning, often in rather exotic ways.</p>
   * Remove "ing", "end", "ig", "lijk", "baar" and "bar"
   *
   * @param sb String being stemmed
   */
  private void step3b(StringBuilder sb) {
    if (_R2 >= sb.length())
      return;
    String s = sb.toString();
    int index = 0;

    if ((s.endsWith("end") || s.endsWith("ing")) &&
        (index = s.length() - 3) >= _R2) {
      sb.delete(index, index + 3);
      if (sb.charAt(index - 2) == 'i' &&
          sb.charAt(index - 1) == 'g') {
        if (sb.charAt(index - 3) != 'e' & index - 2 >= _R2) {
          index -= 2;
          sb.delete(index, index + 2);
        }
      } else {
        unDouble(sb, index);
      }
      return;
    }
    if (s.endsWith("ig") &&
        (index = s.length() - 2) >= _R2
    ) {
      if (sb.charAt(index - 1) != 'e')
        sb.delete(index, index + 2);
      return;
    }
    if (s.endsWith("lijk") &&
        (index = s.length() - 4) >= _R2
    ) {
      sb.delete(index, index + 4);
      step2(sb);
      return;
    }
    if (s.endsWith("baar") &&
        (index = s.length() - 4) >= _R2
    ) {
      sb.delete(index, index + 4);
      return;
    }
    if (s.endsWith("bar") &&
        (index = s.length() - 3) >= _R2
    ) {
      if (_removedE)
        sb.delete(index, index + 3);
      return;
    }
  }

  /**
   * undouble vowel
   * If the words ends CVD, where C is a non-vowel, D is a non-vowel other than I, and V is double a, e, o or u, remove one of the vowels from V (for example, maan -> man, brood -> brod).
   *
   * @param sb String being stemmed
   */
  private void step4(StringBuilder sb) {
    if (sb.length() < 4)
      return;
    String end = sb.substring(sb.length() - 4, sb.length());
    char c = end.charAt(0);
    char v1 = end.charAt(1);
    char v2 = end.charAt(2);
    char d = end.charAt(3);
    if (v1 == v2 &&
        d != 'I' &&
        v1 != 'i' &&
        isVowel(v1) &&
        !isVowel(d) &&
        !isVowel(c)) {
      sb.delete(sb.length() - 2, sb.length() - 1);
    }
  }

  /**
   * Checks if a term could be stemmed.
   *
   * @return true if, and only if, the given term consists in letters.
   */
  private boolean isStemmable(String term) {
    for (int c = 0; c < term.length(); c++) {
      if (!Character.isLetter(term.charAt(c))) return false;
    }
    return true;
  }

  /**
   * Substitute ä, ë, ï, ö, ü, á , é, í, ó, ú
   */
  private void substitute(StringBuilder buffer) {
    for (int i = 0; i < buffer.length(); i++) {
      switch (buffer.charAt(i)) {
        case 'ä':
        case 'á':
          {
            buffer.setCharAt(i, 'a');
            break;
          }
        case 'ë':
        case 'é':
          {
            buffer.setCharAt(i, 'e');
            break;
          }
        case 'ü':
        case 'ú':
          {
            buffer.setCharAt(i, 'u');
            break;
          }
        case 'ï':
        case 'i':
          {
            buffer.setCharAt(i, 'i');
            break;
          }
        case 'ö':
        case 'ó':
          {
            buffer.setCharAt(i, 'o');
            break;
          }
      }
    }
  }

  /*private boolean isValidSEnding(StringBuilder sb) {
    return isValidSEnding(sb, sb.length() - 1);
  }*/

  private boolean isValidSEnding(StringBuilder sb, int index) {
    char c = sb.charAt(index);
    if (isVowel(c) || c == 'j')
      return false;
    return true;
  }

  /*private boolean isValidEnEnding(StringBuilder sb) {
    return isValidEnEnding(sb, sb.length() - 1);
  }*/

  private boolean isValidEnEnding(StringBuilder sb, int index) {
    char c = sb.charAt(index);
    if (isVowel(c))
      return false;
    if (c < 3)
      return false;
    // ends with "gem"?
    if (c == 'm' && sb.charAt(index - 2) == 'g' && sb.charAt(index - 1) == 'e')
      return false;
    return true;
  }

  private void unDouble(StringBuilder sb) {
    unDouble(sb, sb.length());
  }

  private void unDouble(StringBuilder sb, int endIndex) {
    String s = sb.substring(0, endIndex);
    if (s.endsWith("kk") || s.endsWith("tt") || s.endsWith("dd") || s.endsWith("nn") || s.endsWith("mm") || s.endsWith("ff")) {
      sb.delete(endIndex - 1, endIndex);
    }
  }

  private int getRIndex(StringBuilder sb, int start) {
    if (start == 0)
      start = 1;
    int i = start;
    for (; i < sb.length(); i++) {
      //first non-vowel preceded by a vowel
      if (!isVowel(sb.charAt(i)) && isVowel(sb.charAt(i - 1))) {
        return i + 1;
      }
    }
    return i + 1;
  }

  private void storeYandI(StringBuilder sb) {
    if (sb.charAt(0) == 'y')
      sb.setCharAt(0, 'Y');

    int last = sb.length() - 1;

    for (int i = 1; i < last; i++) {
      switch (sb.charAt(i)) {
        case 'i':
          {
            if (isVowel(sb.charAt(i - 1)) &&
                isVowel(sb.charAt(i + 1))
            )
              sb.setCharAt(i, 'I');
            break;
          }
        case 'y':
          {
            if (isVowel(sb.charAt(i - 1)))
              sb.setCharAt(i, 'Y');
            break;
          }
      }
    }
    if (last > 0 && sb.charAt(last) == 'y' && isVowel(sb.charAt(last - 1)))
      sb.setCharAt(last, 'Y');
  }

  private void reStoreYandI(StringBuilder sb) {
    String tmp = sb.toString();
    sb.delete(0, sb.length());
    sb.insert(0, tmp.replaceAll("I", "i").replaceAll("Y", "y"));
  }

  private boolean isVowel(char c) {
    switch (c) {
      case 'e':
      case 'a':
      case 'o':
      case 'i':
      case 'u':
      case 'y':
      case 'è':
        {
          return true;
        }
    }
    return false;
  }

  void setStemDictionary(Map dict) {
    _stemDict = dict;
  }

}
