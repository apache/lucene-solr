package org.apache.lucene.analysis;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;

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
 * A simple class that stores Strings as char[]'s in a
 * hash table.  Note that this is not a general purpose
 * class.  For example, it cannot remove items from the
 * set, nor does it resize its hash table to be smaller,
 * etc.  It is designed to be quick to test if a char[]
 * is in the set without the necessity of converting it
 * to a String first.
 */

public class CharArraySet extends AbstractSet {
  private final static int INIT_SIZE = 8;
  private char[][] entries;
  private int count;
  private final boolean ignoreCase;

  /** Create set with enough capacity to hold startSize
   *  terms */
  public CharArraySet(int startSize, boolean ignoreCase) {
    this.ignoreCase = ignoreCase;
    int size = INIT_SIZE;
    while(startSize + (startSize>>2) > size)
      size <<= 1;
    entries = new char[size][];
  }

 /** Create set from a Collection of char[] or String */
  public CharArraySet(Collection c, boolean ignoreCase) {
    this(c.size(), ignoreCase);
    addAll(c);
  }

  /** true if the <code>len</code> chars of <code>text</code> starting at <code>off</code>
   * are in the set */
  public boolean contains(char[] text, int off, int len) {
    return entries[getSlot(text, off, len)] != null;
  }

  /** true if the <code>CharSequence</code> is in the set */
  public boolean contains(CharSequence cs) {
    return entries[getSlot(cs)] != null;
  }

  private int getSlot(char[] text, int off, int len) {
    int code = getHashCode(text, off, len);
    int pos = code & (entries.length-1);
    char[] text2 = entries[pos];
    if (text2 != null && !equals(text, off, len, text2)) {
      final int inc = ((code>>8)+code)|1;
      do {
        code += inc;
        pos = code & (entries.length-1);
        text2 = entries[pos];
      } while (text2 != null && !equals(text, off, len, text2));
    }
    return pos;
  }

  /** Returns true if the String is in the set */  
  private int getSlot(CharSequence text) {
    int code = getHashCode(text);
    int pos = code & (entries.length-1);
    char[] text2 = entries[pos];
    if (text2 != null && !equals(text, text2)) {
      final int inc = ((code>>8)+code)|1;
      do {
        code += inc;
        pos = code & (entries.length-1);
        text2 = entries[pos];
      } while (text2 != null && !equals(text, text2));
    }
    return pos;
  }

  /** Add this CharSequence into the set */
  public boolean add(CharSequence text) {
    return add(text.toString()); // could be more efficient
  }

  /** Add this String into the set */
  public boolean add(String text) {
    return add(text.toCharArray());
  }

  /** Add this char[] directly to the set.
   * If ignoreCase is true for this Set, the text array will be directly modified.
   * The user should never modify this text array after calling this method.
   */
  public boolean add(char[] text) {
    if (ignoreCase)
      for(int i=0;i<text.length;i++)
        text[i] = Character.toLowerCase(text[i]);
    int slot = getSlot(text, 0, text.length);
    if (entries[slot] != null) return false;
    entries[slot] = text;
    count++;

    if (count + (count>>2) > entries.length) {
      rehash();
    }

    return true;
  }

  private boolean equals(char[] text1, int off, int len, char[] text2) {
    if (len != text2.length)
      return false;
    if (ignoreCase) {
      for(int i=0;i<len;i++) {
        if (Character.toLowerCase(text1[off+i]) != text2[i])
          return false;
      }
    } else {
      for(int i=0;i<len;i++) {
        if (text1[off+i] != text2[i])
          return false;
      }
    }
    return true;
  }

  private boolean equals(CharSequence text1, char[] text2) {
    int len = text1.length();
    if (len != text2.length)
      return false;
    if (ignoreCase) {
      for(int i=0;i<len;i++) {
        if (Character.toLowerCase(text1.charAt(i)) != text2[i])
          return false;
      }
    } else {
      for(int i=0;i<len;i++) {
        if (text1.charAt(i) != text2[i])
          return false;
      }
    }
    return true;
  }

  private void rehash() {
    final int newSize = 2*entries.length;
    char[][] oldEntries = entries;
    entries = new char[newSize][];

    for(int i=0;i<oldEntries.length;i++) {
      char[] text = oldEntries[i];
      if (text != null) {
        // todo: could be faster... no need to compare strings on collision
        entries[getSlot(text,0,text.length)] = text;
      }
    }
  }
  
  private int getHashCode(char[] text, int offset, int len) {
    int code = 0;
    final int stop = offset + len;
    if (ignoreCase) {
      for (int i=offset; i<stop; i++) {
        code = code*31 + Character.toLowerCase(text[i]);
      }
    } else {
      for (int i=offset; i<stop; i++) {
        code = code*31 + text[i];
      }
    }
    return code;
  }

  private int getHashCode(CharSequence text) {
    int code;
    if (ignoreCase) {
      code = 0;
      int len = text.length();
      for (int i=0; i<len; i++) {
        code = code*31 + Character.toLowerCase(text.charAt(i));
      }
    } else {
      if (false && text instanceof String) {
        code = text.hashCode();
      } else {
        code = 0;
        int len = text.length();
        for (int i=0; i<len; i++) {
          code = code*31 + text.charAt(i);
        }
      }
    }
    return code;
  }


  public int size() {
    return count;
  }

  public boolean isEmpty() {
    return count==0;
  }

  public boolean contains(Object o) {
    if (o instanceof char[]) {
      char[] text = (char[])o;
      return contains(text, 0, text.length);
    } else if (o instanceof CharSequence) {
      return contains((CharSequence)o);
    }
    return false;
  }

  public boolean add(Object o) {
    if (o instanceof char[]) {
      return add((char[])o);
    } else if (o instanceof String) {
      return add((String)o);
    } else if (o instanceof CharSequence) {
      return add((CharSequence)o);
    } else {
      return add(o.toString());
    }
  }

  /** The Iterator<String> for this set.  Strings are constructed on the fly, so
   * use <code>nextCharArray</code> for more efficient access. */
  public class CharArraySetIterator implements Iterator {
    int pos=-1;
    char[] next;
    CharArraySetIterator() {
      goNext();
    }

    private void goNext() {
      next = null;
      pos++;
      while (pos < entries.length && (next=entries[pos]) == null) pos++;
    }

    public boolean hasNext() {
      return next != null;
    }

    /** do not modify the returned char[] */
    public char[] nextCharArray() {
      char[] ret = next;
      goNext();
      return ret;
    }

    /** Returns the next String, as a Set<String> would...
     * use nextCharArray() for better efficiency. */
    public Object next() {
      return new String(nextCharArray());
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }


  public Iterator iterator() {
    return new CharArraySetIterator();
  }

}
