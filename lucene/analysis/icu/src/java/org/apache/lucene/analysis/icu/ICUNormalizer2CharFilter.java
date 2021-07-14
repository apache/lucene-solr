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
package org.apache.lucene.analysis.icu;


import java.io.IOException;
import java.io.Reader;
import java.util.Objects;

import org.apache.lucene.analysis.CharacterUtils;
import org.apache.lucene.analysis.charfilter.BaseCharFilter;

import com.ibm.icu.text.Normalizer2;

/**
 * Normalize token text with ICU's {@link Normalizer2}.
 */
public final class ICUNormalizer2CharFilter extends BaseCharFilter {

  private final Normalizer2 normalizer;
  private final StringBuilder inputBuffer = new StringBuilder();
  private final StringBuilder resultBuffer = new StringBuilder();

  private boolean inputFinished;
  private boolean afterQuickCheckYes;
  private int checkedInputBoundary;
  private int charCount;


  /**
   * Create a new Normalizer2CharFilter that combines NFKC normalization, Case
   * Folding, and removes Default Ignorables (NFKC_Casefold)
   */
  public ICUNormalizer2CharFilter(Reader in) {
    this(in, Normalizer2.getInstance(null, "nfkc_cf", Normalizer2.Mode.COMPOSE));
  }

  /**
   * Create a new Normalizer2CharFilter with the specified Normalizer2
   * @param in text
   * @param normalizer normalizer to use
   */
  public ICUNormalizer2CharFilter(Reader in, Normalizer2 normalizer) {
    this(in, normalizer, 128);
  }
  
  // for testing ONLY
  ICUNormalizer2CharFilter(Reader in, Normalizer2 normalizer, int bufferSize) {
    super(in);
    this.normalizer = Objects.requireNonNull(normalizer);
    this.tmpBuffer = CharacterUtils.newCharacterBuffer(bufferSize);
  }

  @Override
  public int read(char[] cbuf, int off, int len) throws IOException {
    if (off < 0) throw new IllegalArgumentException("off < 0");
    if (off >= cbuf.length) throw new IllegalArgumentException("off >= cbuf.length");
    if (len <= 0) throw new IllegalArgumentException("len <= 0");

    while (!inputFinished || inputBuffer.length() > 0 || resultBuffer.length() > 0) {
      int retLen;

      if (resultBuffer.length() > 0) {
        retLen = outputFromResultBuffer(cbuf, off, len);
        if (retLen > 0) {
          return retLen;
        }
      }

      int resLen = readAndNormalizeFromInput();
      if (resLen > 0) {
        retLen = outputFromResultBuffer(cbuf, off, len);
        if (retLen > 0) {
          return retLen;
        }
      }

      readInputToBuffer();
    }

    return -1;
  }

  private final CharacterUtils.CharacterBuffer tmpBuffer;

  private void readInputToBuffer() throws IOException {
    // CharacterUtils.fill is supplementary char aware
    if (!CharacterUtils.fill(tmpBuffer, input)) {
      inputFinished = true;
    }

    assert tmpBuffer.getOffset() == 0;
    inputBuffer.append(tmpBuffer.getBuffer(), 0, tmpBuffer.getLength());

    // if checkedInputBoundary was at the end of a buffer, we need to check that char again
    checkedInputBoundary = Math.max(checkedInputBoundary - 1, 0);
  }

  private int readAndNormalizeFromInput() {
    if (inputBuffer.length() <= 0) {
      afterQuickCheckYes = false;
      return 0;
    }
    if (!afterQuickCheckYes) {
      int resLen = readFromInputWhileSpanQuickCheckYes();
      if (resLen > 0) return resLen;
    }
    int resLen = readFromIoNormalizeUptoBoundary();
    if(resLen > 0){
      afterQuickCheckYes = false;
    }
    return resLen;
  }

  private int readFromInputWhileSpanQuickCheckYes() {
    afterQuickCheckYes = true;
    int end = normalizer.spanQuickCheckYes(inputBuffer);
    if (end > 0) {
      int cp;
      if (end == inputBuffer.length()
          && !normalizer.hasBoundaryAfter(cp = inputBuffer.codePointBefore(end))) {
        /*
        our quickCheckYes result is valid thru the end of current buffer, but we need to back off
        because we're not at a normalization boundary. At a minimum this is relevant wrt imposing
        canonical ordering of combining characters across the buffer boundary.
         */
        afterQuickCheckYes = false;
        end -= Character.charCount(cp);
        // NOTE: for the loop, we pivot to using `hasBoundaryBefore()` because per the docs for
        // `Normalizer2.hasBoundaryAfter()`:
        // "Note that this operation may be significantly slower than hasBoundaryBefore()"
        while (end > 0 && !normalizer.hasBoundaryBefore(cp)) {
          cp = inputBuffer.codePointBefore(end);
          end -= Character.charCount(cp);
        }
        if (end == 0) {
          return 0;
        }
      }
      resultBuffer.append(inputBuffer.subSequence(0, end));
      inputBuffer.delete(0, end);
      checkedInputBoundary = Math.max(checkedInputBoundary - end, 0);
      charCount += end;
    }
    return end;
  }

  private int readFromIoNormalizeUptoBoundary() {
    // if there's no buffer to normalize, return 0
    if (inputBuffer.length() <= 0) {
      return 0;
    }

    boolean foundBoundary = false;
    final int bufLen = inputBuffer.length();

    while (checkedInputBoundary <= bufLen - 1) {
      int charLen = Character.charCount(inputBuffer.codePointAt(checkedInputBoundary));
      checkedInputBoundary += charLen;
      if (checkedInputBoundary < bufLen && normalizer.hasBoundaryBefore(inputBuffer
        .codePointAt(checkedInputBoundary))) {
        foundBoundary = true;
        break;
      }
    }
    if (!foundBoundary && checkedInputBoundary >= bufLen && inputFinished) {
      foundBoundary = true;
      checkedInputBoundary = bufLen;
    }

    if (!foundBoundary) {
      return 0;
    }

    return normalizeInputUpto(checkedInputBoundary);
  }

  private int normalizeInputUpto(final int length) {
    final int destOrigLen = resultBuffer.length();
    normalizer.normalizeSecondAndAppend(resultBuffer,
      inputBuffer.subSequence(0, length));
    inputBuffer.delete(0, length);
    checkedInputBoundary = Math.max(checkedInputBoundary - length, 0);
    final int resultLength = resultBuffer.length() - destOrigLen;
    recordOffsetDiff(length, resultLength);
    return resultLength;
  }

  private void recordOffsetDiff(int inputLength, int outputLength) {
    if (inputLength == outputLength) {
      charCount += outputLength;
      return;
    }
    final int diff = inputLength - outputLength;
    final int cumuDiff = getLastCumulativeDiff();
    if (diff < 0) {
      for (int i = 1;  i <= -diff; ++i) {
        addOffCorrectMap(charCount + i, cumuDiff - i);
      }
    } else {
      addOffCorrectMap(charCount + outputLength, cumuDiff + diff);
    }
    charCount += outputLength;
  }

  private int outputFromResultBuffer(char[] cbuf, int begin, int len) {
    len = Math.min(resultBuffer.length(), len);
    resultBuffer.getChars(0, len, cbuf, begin);
    if (len > 0) {
      resultBuffer.delete(0, len);
    }
    return len;
  }
}
