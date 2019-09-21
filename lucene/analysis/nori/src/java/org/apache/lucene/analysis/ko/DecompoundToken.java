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

/**
 * A token that was generated from a compound.
 */
public class DecompoundToken extends Token {
  private final POS.Tag posTag;

  /**
   *  Creates a new DecompoundToken
   * @param posTag The part of speech of the token.
   * @param surfaceForm The surface form of the token.
   * @param startOffset The start offset of the token in the analyzed text.
   * @param endOffset The end offset of the token in the analyzed text.
   */
  public DecompoundToken(POS.Tag posTag, String surfaceForm, int startOffset, int endOffset) {
    super(surfaceForm.toCharArray(), 0, surfaceForm.length(), startOffset, endOffset);
    this.posTag = posTag;
  }

  @Override
  public String toString() {
    return "DecompoundToken(\"" + getSurfaceFormString() + "\" pos=" + getStartOffset() + " length=" + getLength() +
        " startOffset=" + getStartOffset() + " endOffset=" + getEndOffset() + ")";
  }

  @Override
  public POS.Type getPOSType() {
    return POS.Type.MORPHEME;
  }

  @Override
  public POS.Tag getLeftPOS() {
    return posTag;
  }

  @Override
  public POS.Tag getRightPOS() {
    return posTag;
  }

  @Override
  public String getReading() {
    return null;
  }

  @Override
  public Dictionary.Morpheme[] getMorphemes() {
    return null;
  }
}
