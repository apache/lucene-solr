package org.apache.lucene.analysis.hunspell2;

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

import java.util.ArrayList;
import java.util.List;

/**
 * Stem represents all information known about a stem of a word.  This includes the stem, and the prefixes and suffixes
 * that were used to change the word into the stem.
 */
final class Stem {
  final List<Affix> prefixes = new ArrayList<Affix>();
  final List<Affix> suffixes = new ArrayList<Affix>();
  final char stem[];
  final int stemLength;

  /**
   * Creates a new Stem wrapping the given word stem
   *
   * @param stem Stem of a word
   */
  public Stem(char stem[], int stemLength) {
    this.stem = stem;
    this.stemLength = stemLength;
  }

  /**
   * Adds a prefix to the list of prefixes used to generate this stem.  Because it is assumed that prefixes are added
   * depth first, the prefix is added to the front of the list
   *
   * @param prefix Prefix to add to the list of prefixes for this stem
   */
  public void addPrefix(Affix prefix) {
    prefixes.add(0, prefix);
  }

  /**
   * Adds a suffix to the list of suffixes used to generate this stem.  Because it is assumed that suffixes are added
   * depth first, the suffix is added to the end of the list
   *
   * @param suffix Suffix to add to the list of suffixes for this stem
   */
  public void addSuffix(Affix suffix) {
    suffixes.add(suffix);
  }

  /**
   * Returns the list of prefixes used to generate the stem
   *
   * @return List of prefixes used to generate the stem or an empty list if no prefixes were required
   */
  public List<Affix> getPrefixes() {
    return prefixes;
  }

  /**
   * Returns the list of suffixes used to generate the stem
   * 
   * @return List of suffixes used to generate the stem or an empty list if no suffixes were required
   */
  public List<Affix> getSuffixes() {
    return suffixes;
  }

  /**
   * Returns the text of the word's stem.
   * @see #getStemLength()
   */
  public char[] getStem() {
    return stem;
  }

  /** Returns the valid length of the text in {@link #getStem()} */
  public int getStemLength() {
    return stemLength;
  }
  
  /** Only use this if you really need a string (e.g. for testing) */
  public String getStemString() {
    return new String(stem, 0, stemLength);
  }
}