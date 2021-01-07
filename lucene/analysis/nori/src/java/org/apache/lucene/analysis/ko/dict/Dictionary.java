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
package org.apache.lucene.analysis.ko.dict;

import org.apache.lucene.analysis.ko.POS.Tag;
import org.apache.lucene.analysis.ko.POS.Type;

/** Dictionary interface for retrieving morphological data by id. */
public interface Dictionary {
  /** A morpheme extracted from a compound token. */
  class Morpheme {
    public final Tag posTag;
    public final String surfaceForm;

    public Morpheme(Tag posTag, String surfaceForm) {
      this.posTag = posTag;
      this.surfaceForm = surfaceForm;
    }
  }

  /** Get left id of specified word */
  int getLeftId(int wordId);

  /** Get right id of specified word */
  int getRightId(int wordId);

  /** Get word cost of specified word */
  int getWordCost(int wordId);

  /** Get the {@link Type} of specified word (morpheme, compound, inflect or pre-analysis) */
  Type getPOSType(int wordId);

  /**
   * Get the left {@link Tag} of specfied word.
   *
   * <p>For {@link Type#MORPHEME} and {@link Type#COMPOUND} the left and right POS are the same.
   */
  Tag getLeftPOS(int wordId);

  /**
   * Get the right {@link Tag} of specfied word.
   *
   * <p>For {@link Type#MORPHEME} and {@link Type#COMPOUND} the left and right POS are the same.
   */
  Tag getRightPOS(int wordId);

  /** Get the reading of specified word (mainly used for Hanja to Hangul conversion). */
  String getReading(int wordId);

  /** Get the morphemes of specified word (e.g. 가깝으나: 가깝 + 으나). */
  Morpheme[] getMorphemes(int wordId, char[] surfaceForm, int off, int len);
}
