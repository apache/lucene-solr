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

import java.util.Collections;
import java.util.List;

public class WordEntry {

  private static final int IDX_NOUN = 0;
  private static final int IDX_VERB = 1;
  private static final int IDX_BUSA = 2;
  private static final int IDX_DOV = 3;
  private static final int IDX_BEV = 4;
  private static final int IDX_NE = 5;
  private static final int IDX_REGURA = 9;
  
  /** Irregular verb type (ㅂ-final) */
  public static final int VERB_TYPE_BIUP = 'B';
  
  /** Irregular verb type (ㅎ-final) */
  public static final int VERB_TYPE_HIOOT = 'H';
  
  /** Irregular verb type (ㄹ-final) */
  public static final int VERB_TYPE_LIUL = 'U';
  
  /** Irregular verb type (르-final) */
  public static final int VERB_TYPE_LOO = 'L';

  /** Irregular verb type (ㅅ-final) */
  public static final int VERB_TYPE_SIUT = 'S';
  
  /** Irregular verb type (ㄷ-final) */
  public static final int VERB_TYPE_DI = 'D';
  
  /** Irregular verb type (러-final) */
  public static final int VERB_TYPE_RU = 'R';
  
  /** Regular verb type */
  public static final int VERB_TYPE_REGULAR = 'X';
  
  /**
   * 단어
   */
  private final String word;
  
  /**
   * 단어특성
   */
  private final char[] features;
  
  private final List<CompoundEntry> compounds;
  
  public WordEntry(String word, char[] cs, List<CompoundEntry> compounds) {
    if (cs.length != 10) {
      throw new IllegalArgumentException("invalid features for word: " + word + ", got:" + new String(cs));
    } 
    this.word = word;
    this.features = cs;
    this.compounds = compounds == null ? null : Collections.unmodifiableList(compounds);
    // has compound list iff compound feature is set
    assert (features[IDX_NOUN] == '2' && compounds != null && compounds.size() > 1) 
        || (features[IDX_NOUN] != '2' && compounds == null) : "inconsistent compound data for word: " + word;
  }
  
  public String getWord() {
    return word;
  }
  
  /** Returns true if the entry is a noun (or compound noun) */
  public boolean isNoun() {
    return features[IDX_NOUN] != '0';
  }
  
  /** Returns true if entry is a compound noun */
  public boolean isCompoundNoun() {
    return features[IDX_NOUN] == '2';
  }
  
  /** Returns List of compounds for word */
  public List<CompoundEntry> getCompounds() {
    assert isCompoundNoun();
    return compounds;
  }
  
  /** Returns true if entry is verb */
  public boolean isVerb() {
    return features[IDX_VERB] == '1';
  }
  
  /** Returns verb type (IRR_TYPE_REGULAR or irregular type) */
  public int getVerbType() {
    return features[IDX_REGURA];
  }
  
  /** Returns true if entry is busa (adverb) */
  public boolean isAdverb() {
    return features[IDX_BUSA] == '1';
  }
  
  /** allows noun analysis with -하 verb suffix */
  public boolean hasDOV() {
    return features[IDX_DOV] == '1';
  }
  
  /** allows noun analysis with -되 verb suffix */
  public boolean hasBEV() {
    return features[IDX_BEV] == '1';
  }
  
  /** allows noun analysis with -내 verb suffix */
  public boolean hasNE() {
    return features[IDX_NE] == '1';
  }
}
