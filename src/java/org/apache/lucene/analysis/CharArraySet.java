package org.apache.lucene.analysis;

/**
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


/**
 * A simple class that can store & retrieve char[]'s in a
 * hash table.  Note that this is not a general purpose
 * class.  For example, it cannot remove char[]'s from the
 * set, nor does it resize its hash table to be smaller,
 * etc.  It is designed for use with StopFilter to enable
 * quick filtering based on the char[] termBuffer in a
 * Token.
 */

final class CharArraySet {

  private final static int INIT_SIZE = 8;
  private final static double MAX_LOAD_FACTOR = 0.75;
  private int mask;
  private char[][] entries;
  private int count;
  private boolean ignoreCase;

  /** Create set with enough capacity to hold startSize
   *  terms */
  public CharArraySet(int startSize, boolean ignoreCase) {
    this.ignoreCase = ignoreCase;
    int size = INIT_SIZE;
    while(((double) startSize)/size >= MAX_LOAD_FACTOR)
      size *= 2;
    mask = size-1;
    entries = new char[size][];
  }

  /** Returns true if the characters in text up to length
   *  len is present in the set. */
  public boolean contains(char[] text, int len) {
    int code = getHashCode(text, len);
    int pos = code & mask;
    char[] text2 = entries[pos];
    if (text2 != null && !equals(text, len, text2)) {
      final int inc = ((code>>8)+code)|1;
      do {
        code += inc;
        pos = code & mask;
        text2 = entries[pos];
      } while (text2 != null && !equals(text, len, text2));
    }
    return text2 != null;
  }

  /** Add this String into the set */
  public void add(String text) {
    add(text.toCharArray());
  }

  /** Add this text into the set */
  public void add(char[] text) {
    if (ignoreCase)
      for(int i=0;i<text.length;i++)
        text[i] = Character.toLowerCase(text[i]);
    int code = getHashCode(text, text.length);
    int pos = code & mask;
    char[] text2 = entries[pos];
    if (text2 != null) {
      final int inc = ((code>>8)+code)|1;
      do {
        code += inc;
        pos = code & mask;
        text2 = entries[pos];
      } while (text2 != null);
    }
    entries[pos] = text;
    count++;

    if (((double) count)/entries.length > MAX_LOAD_FACTOR) {
      rehash();
    }
  }

  private boolean equals(char[] text1, int len, char[] text2) {
    if (len != text2.length)
      return false;
    for(int i=0;i<len;i++) {
      if (ignoreCase) {
        if (Character.toLowerCase(text1[i]) != text2[i])
          return false;
      } else {
        if (text1[i] != text2[i])
          return false;
      }
    }
    return true;
  }

  private void rehash() {
    final int newSize = 2*count;
    mask = newSize-1;

    char[][] newEntries = new char[newSize][];
    for(int i=0;i<entries.length;i++) {
      char[] text = entries[i];
      if (text != null) {
        int code = getHashCode(text, text.length);
        int pos = code & mask;
        if (newEntries[pos] != null) {
          final int inc = ((code>>8)+code)|1;
          do {
            code += inc;
            pos = code & mask;
          } while (newEntries[pos] != null);
        }
        newEntries[pos] = text;
      }
    }

    entries = newEntries;
  }
  
  private int getHashCode(char[] text, int len) {
    int downto = len;
    int code = 0;
    while (downto > 0) {
      final char c;
      if (ignoreCase)
        c = Character.toLowerCase(text[--downto]);
      else
        c = text[--downto];
      code = (code*31) + c;
    }
    return code;
  }
}
