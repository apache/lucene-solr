package org.apache.lucene.index;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import org.apache.lucene.store.IndexInput;

final class TermBuffer implements Cloneable {
  private static final char[] NO_CHARS = new char[0];

  private String field;
  private char[] text = NO_CHARS;
  private int textLength;
  private Term term;                            // cached

  public final int compareTo(TermBuffer other) {
    if (field == other.field)			  // fields are interned
      return compareChars(text, textLength, other.text, other.textLength);
    else
      return field.compareTo(other.field);
  }

  private static final int compareChars(char[] v1, int len1,
                                        char[] v2, int len2) {
    int end = Math.min(len1, len2);
    for (int k = 0; k < end; k++) {
      char c1 = v1[k];
      char c2 = v2[k];
      if (c1 != c2) {
        return c1 - c2;
      }
    }
    return len1 - len2;
  }

  private final void setTextLength(int newLength) {
    if (text.length < newLength) {
      char[] newText = new char[newLength];
      System.arraycopy(text, 0, newText, 0, textLength);
      text = newText;
    }
    textLength = newLength;
  }

  public final void read(IndexInput input, FieldInfos fieldInfos)
    throws IOException {
    this.term = null;                           // invalidate cache
    int start = input.readVInt();
    int length = input.readVInt();
    int totalLength = start + length;
    setTextLength(totalLength);
    input.readChars(this.text, start, length);
    this.field = fieldInfos.fieldName(input.readVInt());
  }

  public final void set(Term term) {
    if (term == null) {
      reset();
      return;
    }

    // copy text into the buffer
    setTextLength(term.text().length());
    term.text().getChars(0, term.text().length(), text, 0);

    this.field = term.field();
    this.term = term;
  }

  public final void set(TermBuffer other) {
    setTextLength(other.textLength);
    System.arraycopy(other.text, 0, text, 0, textLength);

    this.field = other.field;
    this.term = other.term;
  }

  public void reset() {
    this.field = null;
    this.textLength = 0;
    this.term = null;
  }

  public Term toTerm() {
    if (field == null)                            // unset
      return null;

    if (term == null)
      term = new Term(field, new String(text, 0, textLength), false);

    return term;
  }

  protected Object clone() {
    TermBuffer clone = null;
    try {
      clone = (TermBuffer)super.clone();
    } catch (CloneNotSupportedException e) {}

    clone.text = new char[text.length];
    System.arraycopy(text, 0, clone.text, 0, textLength);

    return clone;
  }
}
