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
package org.apache.lucene.search.matchhighlight;

import java.text.CharacterIterator;

/** A {@link CharacterIterator} over a {@link CharSequence}. */
final class CharSequenceIterator implements CharacterIterator {
  private final CharSequence text;

  private int begin;
  private int end;
  private int pos;

  public CharSequenceIterator(CharSequence text) {
    this.text = text;
    this.begin = 0;
    this.end = text.length();
  }

  public char first() {
    pos = begin;
    return current();
  }

  public char last() {
    if (end != begin) {
      pos = end - 1;
    } else {
      pos = end;
    }
    return current();
  }

  public char setIndex(int p) {
    if (p < begin || p > end) throw new IllegalArgumentException("Invalid index");
    pos = p;
    return current();
  }

  public char current() {
    if (pos >= begin && pos < end) {
      return text.charAt(pos);
    } else {
      return DONE;
    }
  }

  public char next() {
    if (pos < end - 1) {
      pos++;
      return text.charAt(pos);
    } else {
      pos = end;
      return DONE;
    }
  }

  public char previous() {
    if (pos > begin) {
      pos--;
      return text.charAt(pos);
    } else {
      return DONE;
    }
  }

  public int getBeginIndex() {
    return begin;
  }

  public int getEndIndex() {
    return end;
  }

  public int getIndex() {
    return pos;
  }

  @Override
  public Object clone() {
    try {
      return super.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }
}
