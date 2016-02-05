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
package org.apache.lucene.analysis.util;


import java.text.BreakIterator; // javadoc
import java.text.CharacterIterator;
import java.util.Locale;

/** 
 * A CharacterIterator used internally for use with {@link BreakIterator}
 * @lucene.internal
 */
public abstract class CharArrayIterator implements CharacterIterator {
  private char array[];
  private int start;
  private int index;
  private int length;
  private int limit;

  public char [] getText() {
    return array;
  }
  
  public int getStart() {
    return start;
  }
  
  public int getLength() {
    return length;
  }
  
  /**
   * Set a new region of text to be examined by this iterator
   * 
   * @param array text buffer to examine
   * @param start offset into buffer
   * @param length maximum length to examine
   */
  public void setText(final char array[], int start, int length) {
    this.array = array;
    this.start = start;
    this.index = start;
    this.length = length;
    this.limit = start + length;
  }

  @Override
  public char current() {
    return (index == limit) ? DONE : jreBugWorkaround(array[index]);
  }
  
  protected abstract char jreBugWorkaround(char ch);

  @Override
  public char first() {
    index = start;
    return current();
  }

  @Override
  public int getBeginIndex() {
    return 0;
  }

  @Override
  public int getEndIndex() {
    return length;
  }

  @Override
  public int getIndex() {
    return index - start;
  }

  @Override
  public char last() {
    index = (limit == start) ? limit : limit - 1;
    return current();
  }

  @Override
  public char next() {
    if (++index >= limit) {
      index = limit;
      return DONE;
    } else {
      return current();
    }
  }

  @Override
  public char previous() {
    if (--index < start) {
      index = start;
      return DONE;
    } else {
      return current();
    }
  }

  @Override
  public char setIndex(int position) {
    if (position < getBeginIndex() || position > getEndIndex())
      throw new IllegalArgumentException("Illegal Position: " + position);
    index = start + position;
    return current();
  }
  
  @Override
  public CharArrayIterator clone() {
    try {
      return (CharArrayIterator)super.clone();
    } catch (CloneNotSupportedException e) {
      // CharacterIterator does not allow you to throw CloneNotSupported
      throw new RuntimeException(e);
    }
  }
  
  /**
   * Create a new CharArrayIterator that works around JRE bugs
   * in a manner suitable for {@link BreakIterator#getSentenceInstance()}
   */
  public static CharArrayIterator newSentenceInstance() {
    if (HAS_BUGGY_BREAKITERATORS) {
      return new CharArrayIterator() {
        // work around this for now by lying about all surrogates to 
        // the sentence tokenizer, instead we treat them all as 
        // SContinue so we won't break around them.
        @Override
        protected char jreBugWorkaround(char ch) {
          return ch >= 0xD800 && ch <= 0xDFFF ? 0x002C : ch;
        }
      };
    } else {
      return new CharArrayIterator() {
        // no bugs
        @Override
        protected char jreBugWorkaround(char ch) {
          return ch;
        }
      };
    }
  }
  
  /**
   * Create a new CharArrayIterator that works around JRE bugs
   * in a manner suitable for {@link BreakIterator#getWordInstance()}
   */
  public static CharArrayIterator newWordInstance() {
    if (HAS_BUGGY_BREAKITERATORS) {
      return new CharArrayIterator() {
        // work around this for now by lying about all surrogates to the word, 
        // instead we treat them all as ALetter so we won't break around them.
        @Override
        protected char jreBugWorkaround(char ch) {
          return ch >= 0xD800 && ch <= 0xDFFF ? 0x0041 : ch;
        }
      };
    } else {
      return new CharArrayIterator() {
        // no bugs
        @Override
        protected char jreBugWorkaround(char ch) {
          return ch;
        }
      };
    }
  }
  
  /**
   * True if this JRE has a buggy BreakIterator implementation
   */
  public static final boolean HAS_BUGGY_BREAKITERATORS;
  static {
    boolean v;
    try {
      BreakIterator bi = BreakIterator.getSentenceInstance(Locale.US);
      bi.setText("\udb40\udc53");
      bi.next();
      v = false;
    } catch (Exception e) {
      v = true;
    }
    HAS_BUGGY_BREAKITERATORS = v;
  }
}
