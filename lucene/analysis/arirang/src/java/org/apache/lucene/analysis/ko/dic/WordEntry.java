package org.apache.lucene.analysis.ko.dic;

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

import java.util.List;

public class WordEntry {

  static final int NOUN =     1 << 3;
  static final int VERB =     1 << 4;
  static final int BUSA =     1 << 5;
  static final int DOV =      1 << 6;
  static final int BEV =      1 << 7;
  static final int NE  =      1 << 8;
  static final int COMPOUND = 1 << 9;
  static final int COMPOUND_IRREGULAR = 1 << 10;
  
  /** Regular verb type */
  public static final int VERB_TYPE_REGULAR = 0;
  
  /** Irregular verb type (ㅂ-final) */
  public static final int VERB_TYPE_BIUP = 1;
  
  /** Irregular verb type (ㅎ-final) */
  public static final int VERB_TYPE_HIOOT = 2;
  
  /** Irregular verb type (ㄹ-final) */
  public static final int VERB_TYPE_LIUL = 3;
  
  /** Irregular verb type (르-final) */
  public static final int VERB_TYPE_LOO = 4;

  /** Irregular verb type (ㅅ-final) */
  public static final int VERB_TYPE_SIUT = 5;
  
  /** Irregular verb type (ㄷ-final) */
  public static final int VERB_TYPE_DI = 6;
  
  /** Irregular verb type (러-final) */
  public static final int VERB_TYPE_RU = 7;
  
  /**
   * 단어
   */
  private final String word;
  
  /**
   * 단어특성
   */
  private final char features;
  
  private final byte clazz;
  
  WordEntry(String word, char features, byte clazz) {
    if (features < 0 || features >= 2048) {
      throw new IllegalArgumentException("Invalid features: " + Integer.toHexString(features));
    }
    this.word = word;
    this.features = (char) features;
    this.clazz = clazz;
    // make sure compound nouns are also nouns
    assert !isCompoundNoun() || isNoun();
  }
  
  public String getWord() {
    return word;
  }
  
  /** Returns true if the entry is a noun (or compound noun) */
  public boolean isNoun() {
    return (features & NOUN) != 0;
  }
  
  /** Returns true if entry is a compound noun */
  public boolean isCompoundNoun() {
    return (features & COMPOUND) != 0;
  }
  
  /** Returns List of compounds for word */
  public CompoundEntry[] getCompounds() {
    assert isCompoundNoun();
    // TODO: should we cache this here? see if someone is calling this repeatedly? i hope not.
    if ((features & COMPOUND_IRREGULAR) != 0) {
      return DictionaryUtil.getIrregularCompounds(clazz);
    } else {
      return DictionaryUtil.getCompounds(word, clazz);
    }
  }
  
  /** Returns true if entry is verb */
  public boolean isVerb() {
    return (features & VERB) != 0;
  }
  
  /** Returns verb type (VERB_TYPE_REGULAR or irregular ending type) */
  public int getVerbType() {
    return features & 0x7;
  }
  
  /** Returns true if entry is busa (adverb) */
  public boolean isAdverb() {
    return (features & BUSA) != 0;
  }
  
  /** allows noun analysis with -하 verb suffix */
  public boolean hasDOV() {
    return (features & DOV) != 0;
  }
  
  /** allows noun analysis with -되 verb suffix */
  public boolean hasBEV() {
    return (features & BEV) != 0;
  }
  
  /** allows noun analysis with -내 verb suffix */
  public boolean hasNE() {
    return (features & NE) != 0;
  }
}
