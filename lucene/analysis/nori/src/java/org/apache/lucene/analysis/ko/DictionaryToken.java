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
package org.apache.lucene.analysis.ko;

import org.apache.lucene.analysis.ko.dict.Dictionary;

/** A token stored in a {@link Dictionary}. */
public class DictionaryToken extends Token {
  private final int wordId;
  private final KoreanTokenizer.Type type;
  private final Dictionary dictionary;

  public DictionaryToken(
      KoreanTokenizer.Type type,
      Dictionary dictionary,
      int wordId,
      char[] surfaceForm,
      int offset,
      int length,
      int startOffset,
      int endOffset) {
    super(surfaceForm, offset, length, startOffset, endOffset);
    this.type = type;
    this.dictionary = dictionary;
    this.wordId = wordId;
  }

  @Override
  public String toString() {
    return "DictionaryToken(\""
        + getSurfaceFormString()
        + "\" pos="
        + getStartOffset()
        + " length="
        + getLength()
        + " posLen="
        + getPositionLength()
        + " type="
        + type
        + " wordId="
        + wordId
        + " leftID="
        + dictionary.getLeftId(wordId)
        + ")";
  }

  /**
   * Returns the type of this token
   *
   * @return token type, not null
   */
  public KoreanTokenizer.Type getType() {
    return type;
  }

  /**
   * Returns true if this token is known word
   *
   * @return true if this token is in standard dictionary. false if not.
   */
  public boolean isKnown() {
    return type == KoreanTokenizer.Type.KNOWN;
  }

  /**
   * Returns true if this token is unknown word
   *
   * @return true if this token is unknown word. false if not.
   */
  public boolean isUnknown() {
    return type == KoreanTokenizer.Type.UNKNOWN;
  }

  /**
   * Returns true if this token is defined in user dictionary
   *
   * @return true if this token is in user dictionary. false if not.
   */
  public boolean isUser() {
    return type == KoreanTokenizer.Type.USER;
  }

  @Override
  public POS.Type getPOSType() {
    return dictionary.getPOSType(wordId);
  }

  @Override
  public POS.Tag getLeftPOS() {
    return dictionary.getLeftPOS(wordId);
  }

  @Override
  public POS.Tag getRightPOS() {
    return dictionary.getRightPOS(wordId);
  }

  @Override
  public String getReading() {
    return dictionary.getReading(wordId);
  }

  @Override
  public Dictionary.Morpheme[] getMorphemes() {
    return dictionary.getMorphemes(wordId, getSurfaceForm(), getOffset(), getLength());
  }
}
