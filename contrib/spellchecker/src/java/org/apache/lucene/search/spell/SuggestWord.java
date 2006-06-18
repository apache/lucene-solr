package org.apache.lucene.search.spell;


/**
 * Copyright 2002-2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 *  SuggestWord, used in suggestSimilar method in SpellChecker class.
 * 
 *  @author Nicolas Maisonneuve
 */
final class SuggestWord {
  /**
   * the score of the word
   */
  public float score;

  /**
   * The freq of the word
   */
  public int freq;

  /**
   * the suggested word
   */
  public String string;

  public final int compareTo (SuggestWord a) {
    //first criteria: the edit distance
    if (score > a.score) {
      return 1;
    }
    if (score < a.score) {
      return -1;
    }

    //second criteria (if first criteria is equal): the popularity
    if (freq > a.freq) {
      return 1;
    }

    if (freq < a.freq) {
      return -1;
    }
    return 0;
  }
}
