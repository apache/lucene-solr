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

import com.ibm.icu.text.ReplaceableString;
import com.ibm.icu.text.Transliterator;
import com.ibm.icu.text.Transliterator.Position;
import com.ibm.icu.text.UTF16;

import org.apache.lucene.analysis.CharFilter;
import org.apache.lucene.analysis.charfilter.BaseCharFilter;
import org.apache.lucene.util.ArrayUtil;

/**
 * A {@link CharFilter} that transforms text with ICU.
 * <p>
 * ICU provides text-transformation functionality via its Transliteration API.
 * Although script conversion is its most common use, a Transliterator can
 * actually perform a more general class of tasks. In fact, Transliterator
 * defines a very general API which specifies only that a segment of the input
 * text is replaced by new text. The particulars of this conversion are
 * determined entirely by subclasses of Transliterator.
 * </p>
 * <p>
 * Some useful transformations for search are built-in:
 * <ul>
 * <li>Conversion from Traditional to Simplified Chinese characters
 * <li>Conversion from Hiragana to Katakana
 * <li>Conversion from Fullwidth to Halfwidth forms.
 * <li>Script conversions, for example Serbian Cyrillic to Latin
 * </ul>
 * <p>
 * Example usage: <blockquote>stream = new ICUTransformCharFilter(reader,
 * Transliterator.getInstance("Traditional-Simplified"));</blockquote>
 * <br>
 * For more details, see the <a
 * href="http://userguide.icu-project.org/transforms/general">ICU User
 * Guide</a>.
 */
public final class ICUTransformCharFilter extends BaseCharFilter {

  // Transliterator to transform the text
  private final Transliterator transform;

  // Reusable position object
  private final Position position = new Position();

  private static final int READ_BUFFER_SIZE = 1024;
  private final char[] tmpBuffer = new char[READ_BUFFER_SIZE];

  private static final int INITIAL_TRANSLITERATE_BUFFER_SIZE = 1024;
  private final StringBuffer buffer = new StringBuffer(INITIAL_TRANSLITERATE_BUFFER_SIZE);
  private final ReplaceableString replaceable = new ReplaceableString(buffer);

  private static final int BUFFER_PRUNE_THRESHOLD = 1024;

  private int outputCursor = 0;
  private boolean inputFinished = false;
  private int charCount = 0;
  private int offsetDiffAdjust = 0;

  static final int DEFAULT_MAX_ROLLBACK_BUFFER_CAPACITY = 8192;
  private final int maxRollbackBufferCapacity;

  private static final int DEFAULT_INITIAL_ROLLBACK_BUFFER_CAPACITY = 4; // must be power of 2
  private char[] rollbackBuffer;
  private int rollbackBufferSize = 0;

  ICUTransformCharFilter(Reader in, Transliterator transform) {
    this(in, transform, DEFAULT_MAX_ROLLBACK_BUFFER_CAPACITY);
  }

  /**
   * Construct new {@link ICUTransformCharFilter} with the specified {@link Transliterator}, backed by
   * the specified {@link Reader}.
   * @param in input source
   * @param transform used to perform transliteration
   * @param maxRollbackBufferCapacityHint used to control the maximum size to which this
   * {@link ICUTransformCharFilter} will buffer and rollback partial transliteration of input sequences.
   * The provided hint will be converted to an enforced limit of "the greatest power of 2 (excluding '1')
   * less than or equal to the specified value". Specifying a negative value allows the rollback buffer to
   * grow indefinitely (equivalent to specifying {@link Integer#MAX_VALUE}). Specifying "0" (or "1", in practice)
   * disables rollback. Larger values can in some cases yield more accurate transliteration, at the cost of
   * performance and resolution/accuracy of offset correction.
   * This is intended primarily as a failsafe, with a relatively large default value of {@value ICUTransformCharFilter#DEFAULT_MAX_ROLLBACK_BUFFER_CAPACITY}.
   * See comments "To understand the need for rollback" in private method:
   * {@link Transliterator#filteredTransliterate(com.ibm.icu.text.Replaceable, Position, boolean, boolean)}
   */
  ICUTransformCharFilter(Reader in, Transliterator transform, int maxRollbackBufferCapacityHint) {
    super(in);
    this.transform = ICUTransformFilter.optimizeForCommonCase(transform);
    if (maxRollbackBufferCapacityHint < 0) {
      this.maxRollbackBufferCapacity = Integer.MAX_VALUE;
      this.rollbackBuffer = new char[DEFAULT_INITIAL_ROLLBACK_BUFFER_CAPACITY];
    } else {
      // greatest power of 2 (excluding "1") less than or equal to the specified hint
      this.maxRollbackBufferCapacity = Integer.highestOneBit(maxRollbackBufferCapacityHint - 1) << 1;
      if (this.maxRollbackBufferCapacity == 0) {
        this.rollbackBuffer = null;
      } else {
        this.rollbackBuffer = new char[DEFAULT_INITIAL_ROLLBACK_BUFFER_CAPACITY];
      }
    }
  }

  /**
   * Reads characters into a portion of an array. This method will block until some input is available, an I/O error
   * occurs, or the end of the stream is reached.
   *
   * @param cbuf
   *          Destination buffer
   * @param off
   *          Offset at which to start storing characters
   * @param len
   *          Maximum number of characters to read
   * @return The number of characters read, or -1 if the end of the stream has been reached
   * @throws IOException
   *           If an I/O error occurs
   */
  @Override
  public int read(char[] cbuf, int off, int len) throws IOException {
    if (off < 0) throw new IndexOutOfBoundsException("specified negative array offset");
    if (off >= cbuf.length) throw new IndexOutOfBoundsException("specified offset exceeds buffer length");
    if (len <= 0) throw new IndexOutOfBoundsException("non-positive length specified");
    if (len > cbuf.length - off) throw new IndexOutOfBoundsException("requested end array index exceeds buffer length");

    // !inputFinished || output remains to be flushed
    while (!inputFinished || position.start > outputCursor) {
      if (position.start > outputCursor) {
        return outputFromResultBuffer(cbuf, off, len);
      }

      int resLen = readFromIoNormalizeUptoBoundary();
      if (resLen > 0) {
        return outputFromResultBuffer(cbuf, off, len);
      }

      if (!readInputToBuffer()) {
        final int preStart = position.start;
        final int preLimit;
        final int bufferLength = buffer.length();
        cursorUpdate:
        {
          if (preStart < bufferLength) {
            // if last char is a lead surrogate, transform won't handle it properly anyway
            preLimit = UTF16.isLeadSurrogate(buffer.charAt(bufferLength - 1)) ? bufferLength - 1 : bufferLength;
            position.contextLimit = preLimit;
            position.limit = preLimit;
            transform.finishTransliteration(replaceable, position);
          } else if (offsetDiffAdjust == 0) {
            break cursorUpdate;
          } else {
            preLimit = bufferLength;
          }
          cursorAdvanced(preStart, preLimit);
        }
      }
    }

    return -1;
  }

  private void rollback(int preStart, int preLimit) {
    buffer.delete(preStart, position.limit); // delete uncommitted chars
    buffer.insert(preStart, rollbackBuffer, 0, rollbackBufferSize);
    position.start = preStart;
    position.contextLimit = preLimit;
    position.limit = preLimit;
  }

  /**
   * Grow rollback buffer if necessary, within constraints of {@link #maxRollbackBufferCapacity}.
   * This imposes an arbitrary failsafe to prevent the possibility that the rollback buffer could grow
   * indefinitely.
   *
   * @return true if upon return, rollback buffer has sufficient capacity for new input chars, otherwise false.
   */
  private boolean ensureRollbackBufferCapacity() {
    // ensure space for at least 2 chars (surrogate pair, max possible space needed)
    if (rollbackBuffer.length - rollbackBufferSize < 2) {
      if (rollbackBuffer.length < maxRollbackBufferCapacity) {
        rollbackBuffer = ArrayUtil.growExact(rollbackBuffer, rollbackBuffer.length << 1);
      } else {
        // hit threshold; not going to increase the buffer size
        return false;
      }
    }
    return true;
  }

  private int pushRollbackBuffer(int idx, int bufferLength) {
    if (idx >= bufferLength) {
      return 0;
    } else {
      // note: we have already ensured sufficient buffer capacity
      final char candidate = buffer.charAt(idx);
      if (!UTF16.isLeadSurrogate(candidate)) {
        if (rollbackBuffer != null) {
          rollbackBuffer[rollbackBufferSize++] = candidate;
        }
        return 1;
      } else {
        if (++idx < bufferLength) {
          if (rollbackBuffer != null) {
            rollbackBuffer[rollbackBufferSize++] = candidate;
            rollbackBuffer[rollbackBufferSize++] = buffer.charAt(idx);
          }
          return 2;
        } else {
          // we don't yet have the high surrogate
          if (inputFinished) {
            // no more input available; proceed with the char we have; rollback not needed
            return 1;
          } else {
            return 0; // wait for more input to be available
          }
        }
      }
    }
  }

  private int readFromIoNormalizeUptoBoundary() {
    int nextCharLength = pushRollbackBuffer(position.limit, buffer.length());
    if (nextCharLength == 0) {
      return 0;
    }
    final int preCharCount = charCount;
    int preStart = position.start;
    int preLimit;
    do {
      position.limit += nextCharLength;
      preLimit = position.limit;
      position.contextLimit = preLimit;
      transform.filteredTransliterate(replaceable, position, true);
      boolean rollbackSizeWithinBounds = true;
      if (rollbackBuffer != null && position.start < position.limit && (rollbackSizeWithinBounds = ensureRollbackBufferCapacity())) {
        // complete pass not transliterated, and not yet at rollback buffer threshold cap.
        // N.b.: rollback buffer threshold cap is arbitrary, so we check here (somewhat
        // counterintuitively, *before* rolling back, as opposed to before pushing to
        // the buffer) so that we can fall through to the else clause, clear the rollback
        // buffer, and proceed.
        rollback(preStart, preLimit);
      } else {
        rollbackBufferSize = 0;
        if (position.start > preStart) {
          // cursor advanced
          cursorAdvanced(preStart, preLimit);

          // N.b.: advancing contextStart precludes support for quantifiers, but is crucial for streaming,
          // so we'll do it anyway.
          // See comments in Transliterator source code:
               // TODO
               // This doesn't work once we add quantifier support.  Need to rewrite
               // this code to support quantifiers and 'use maximum backup <n>;'.
               //
               //         index.contextStart = Math.max(index.start - getMaximumContextLength(),
               //                                       originalStart);
          position.contextStart = Math.max(position.start - transform.getMaximumContextLength(), preStart);
          preStart = position.start;
          if (!rollbackSizeWithinBounds) {
            // prepopulate newly cleared rollback buffer with all top-level uncommitted characters
            rollbackBufferSize = position.limit - preStart;
            if (rollbackBuffer.length - rollbackBufferSize < 2) {
              // even after flushing all committed text, there's not enough space in the rollback buffer.
              // This is an edge case of an edge case, when the last char32 read into the rollback buffer
              // is a surrogate pair (completely filling the rollback buffer), *and* the last
              // transliteration pass advanced position.start by exactly one char16 (not a surrogate pair).
              preStart = forceAdvance(preStart, preLimit);
              rollbackBufferSize = position.limit - preStart;
            }
            buffer.getChars(preStart, position.limit, rollbackBuffer, 0);
          }
        } else if (preLimit != position.limit) {
          // cursor hasn't advanced; incoming characters have probably been deleted
          offsetDiffAdjust += preLimit - position.limit;
          // edge case of !rollbackSizeWithinBounds needs no special handling here, since input characters *are* being
          // processed (deleted) -- we *are* progressing through the input stream, although the output stream hasn't changed.
        } else if (!rollbackSizeWithinBounds) {
          // cursor hasn't advanced, no incoming characters have been deleted, and the rollback buffer is full.
          preStart = forceAdvance(preStart, preLimit);
        }
      }
    } while ((nextCharLength = pushRollbackBuffer(position.limit, buffer.length())) > 0);

    return charCount - preCharCount;
  }

  private static final int FORCE_THRESHOLD = 2; // conservative; the length of a surrogate pair

  private int forceAdvance(int preStart, int preLimit) {
    int shift;
    if (maxRollbackBufferCapacity == 2) {
      rollbackBufferSize = 0;
      shift = 2;
    } else {
      shift = 0;
      do {
        shift += UTF16.isLeadSurrogate(rollbackBuffer[shift]) ? 2 : 1;
      } while (shift < FORCE_THRESHOLD && shift < maxRollbackBufferCapacity);
      rollbackBufferSize -= shift;
      System.arraycopy(rollbackBuffer, shift, rollbackBuffer, 0, rollbackBufferSize);
    }
    position.start += shift; // mock transliterator advance
    cursorAdvanced(preStart, preLimit);
    position.contextStart = Math.max(position.start - transform.getMaximumContextLength(), preStart);
    return position.start;
  }

  private void cursorAdvanced(int preStart, int preLimit) {
    final int outputLength = position.start - preStart;
    final int diff = preLimit - position.limit + offsetDiffAdjust;
    offsetDiffAdjust = 0;
    if (diff == 0) {
      // increment charCount; no offset correction necessary
      charCount += outputLength;
    } else {
      // limit change indicates change in length of replacement text; correct offsets accordingly
      recordOffsetDiff(diff, outputLength);
    }
  }

  private void recordOffsetDiff(int diff, int outputLength) {
    final int cumuDiff = getLastCumulativeDiff();
    if (diff < 0) {
      for (int i = 1; i <= -diff; ++i) {
        addOffCorrectMap(charCount + i, cumuDiff - i);
      }
    } else {
      addOffCorrectMap(charCount + outputLength, cumuDiff + diff);
    }
    charCount += outputLength;
  }

  private boolean readInputToBuffer() throws IOException {
    int res = input.read(tmpBuffer, 0, tmpBuffer.length);
    if (res == -1) {
      inputFinished = true;
      return false;
    } else {
      buffer.append(tmpBuffer, 0, res);
      return true;
    }
  }

  private int outputFromResultBuffer(char[] cbuf, int begin, int len) {
    len = Math.min(position.start - outputCursor, len);
    buffer.getChars(outputCursor, outputCursor + len, cbuf, begin);
    outputCursor += len;
    if (outputCursor > BUFFER_PRUNE_THRESHOLD) {
      buffer.delete(0, outputCursor);
      position.contextStart -= outputCursor;
      position.start -= outputCursor;
      position.limit -= outputCursor;
      position.contextLimit -= outputCursor;
      outputCursor = 0;
    }
    return len;
  }
}
