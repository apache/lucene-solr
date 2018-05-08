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

import org.apache.lucene.analysis.ko.dict.Dictionary.Morpheme;

/**
 * Analyzed token with morphological data.
 */
public abstract class Token {
  private final char[] surfaceForm;
  private final int offset;
  private final int length;

  private final int startOffset;
  private final int endOffset;
  private int posIncr = 1;
  private int posLen = 1;

  public Token(char[] surfaceForm, int offset, int length, int startOffset, int endOffset) {
    this.surfaceForm = surfaceForm;
    this.offset = offset;
    this.length = length;

    this.startOffset = startOffset;
    this.endOffset = endOffset;
  }

  /**
   * @return surfaceForm
   */
  public char[] getSurfaceForm() {
    return surfaceForm;
  }

  /**
   * @return offset into surfaceForm
   */
  public int getOffset() {
    return offset;
  }

  /**
   * @return length of surfaceForm
   */
  public int getLength() {
    return length;
  }

  /**
   * @return surfaceForm as a String
   */
  public String getSurfaceFormString() {
    return new String(surfaceForm, offset, length);
  }

  /**
   * Get the {@link POS.Type} of the token.
   */
  public abstract POS.Type getPOSType();

  /**
   * Get the left part of speech of the token.
   */
  public abstract POS.Tag getLeftPOS();

  /**
   * Get the right part of speech of the token.
   */
  public abstract POS.Tag getRightPOS();

  /**
   * Get the reading of the token.
   */
  public abstract String getReading();

  /**
   * Get the {@link Morpheme} decomposition of the token.
   */
  public abstract Morpheme[] getMorphemes();

  /**
   * Get the start offset of the term in the analyzed text.
   */
  public int getStartOffset() {
    return startOffset;
  }

  /**
   * Get the end offset of the term in the analyzed text.
   */
  public int getEndOffset() {
    return endOffset;
  }

  public void setPositionIncrement(int posIncr) {
    this.posIncr = posIncr;
  }

  public int getPositionIncrement() {
    return posIncr;
  }

  public void setPositionLength(int posLen) {
    this.posLen = posLen;
  }

  public int getPositionLength() {
    return posLen;
  }
}
