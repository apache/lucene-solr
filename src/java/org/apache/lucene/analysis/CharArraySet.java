package org.apache.lucene.analysis;

import java.util.Arrays;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import org.apache.lucene.util.CharacterUtils;
import org.apache.lucene.util.Version;

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
 * <p>You must specify the required {@link Version}
 * compatibility when creating {@link CharArraySet}:
 * <ul>
 *   <li> As of 3.1, supplementary characters are
 *       properly lowercased.</li>
 * </ul>
 * Before 3.1 supplementary characters could not be
 * lowercased correctly due to the lack of Unicode 4
 * support in JDK 1.4. To use instances of
 * {@link CharArraySet} with the behavior before Lucene
 * 3.1 pass a {@link Version} < 3.1 to the constructors.
 * <P>
 * <em>Please note:</em> This class implements {@link java.util.Set Set} but
 * does not behave like it should in all cases. The generic type is
 * {@code Set<Object>}, because you can add any object to it,
 * that has a string representation. The add methods will use
 * {@link Object#toString} and store the result using a {@code char[]}
 * buffer. The same behavior have the {@code contains()} methods.
 * The {@link #iterator()} returns an {@code Iterator<String>}.
 * For type safety also {@link #stringIterator()} is provided.
 */
public class CharArraySet extends AbstractSet<Object> {
  private final static int INIT_SIZE = 8;
  private char[][] entries;
  private int count;
  private final boolean ignoreCase;
  public static final CharArraySet EMPTY_SET = CharArraySet.unmodifiableSet(
      new CharArraySet(Version.LUCENE_CURRENT, 0, false));
  
  private final CharacterUtils charUtils;
  private final Version matchVersion;

  /**
   * Create set with enough capacity to hold startSize terms
   * 
   * @param matchVersion
   *          compatibility match version see <a href="#version">Version
   *          note</a> above for details.
   * @param startSize
   *          the initial capacity
   * @param ignoreCase
   *          <code>false</code> if and only if the set should be case sensitive
   *          otherwise <code>true</code>.
   */
  public CharArraySet(Version matchVersion, int startSize, boolean ignoreCase) {
    this.ignoreCase = ignoreCase;
    int size = INIT_SIZE;
    while(startSize + (startSize>>2) > size)
      size <<= 1;
    entries = new char[size][];
    this.charUtils = CharacterUtils.getInstance(matchVersion);
    this.matchVersion = matchVersion;
  }

  /**
   * Creates a set from a Collection of objects. 
   * 
   * @param matchVersion
   *          compatibility match version see <a href="#version">Version
   *          note</a> above for details.
   * @param c
   *          a collection whose elements to be placed into the set
   * @param ignoreCase
   *          <code>false</code> if and only if the set should be case sensitive
   *          otherwise <code>true</code>.
   */
  public CharArraySet(Version matchVersion, Collection<? extends Object> c, boolean ignoreCase) {
    this(matchVersion, c.size(), ignoreCase);
    addAll(c);
  }

  /**
   * Creates a set with enough capacity to hold startSize terms
   * 
   * @param startSize
   *          the initial capacity
   * @param ignoreCase
   *          <code>false</code> if and only if the set should be case sensitive
   *          otherwise <code>true</code>.
   * @deprecated use {@link #CharArraySet(Version, int, boolean)} instead
   */
  @Deprecated
  public CharArraySet(int startSize, boolean ignoreCase) {
    this(Version.LUCENE_30, startSize, ignoreCase);
  }
  
  /**
   * Creates a set from a Collection of objects. 
   * 
   * @param c
   *          a collection whose elements to be placed into the set
   * @param ignoreCase
   *          <code>false</code> if and only if the set should be case sensitive
   *          otherwise <code>true</code>.
   * @deprecated use {@link #CharArraySet(Version, Collection, boolean)} instead         
   */  
  @Deprecated
  public CharArraySet(Collection<? extends Object> c, boolean ignoreCase) {
    this(Version.LUCENE_30, c.size(), ignoreCase);
    addAll(c);
  }
  
  /** Create set from entries */
  private CharArraySet(Version matchVersion, char[][] entries, boolean ignoreCase, int count){
    this.entries = entries;
    this.ignoreCase = ignoreCase;
    this.count = count;
    this.charUtils = CharacterUtils.getInstance(matchVersion);
    this.matchVersion = matchVersion;
  }
  
  /** Clears all entries in this set. This method is supported for reusing, but not {@link Set#remove}. */
  @Override
  public void clear() {
    count = 0;
    Arrays.fill(entries, null);
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
      for(int i=0;i<text.length;){
        i += Character.toChars(
              Character.toLowerCase(
                  charUtils.codePointAt(text, i)), text, i);
      }
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
    final int limit = off+len;
    if (ignoreCase) {
      for(int i=0;i<len;) {
        final int codePointAt = charUtils.codePointAt(text1, off+i, limit);
        if (Character.toLowerCase(codePointAt) != charUtils.codePointAt(text2, i))
          return false;
        i += Character.charCount(codePointAt); 
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
      for(int i=0;i<len;) {
        final int codePointAt = charUtils.codePointAt(text1, i);
        if (Character.toLowerCase(codePointAt) != charUtils.codePointAt(text2, i))
          return false;
        i += Character.charCount(codePointAt);
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
      for (int i=offset; i<stop;) {
        final int codePointAt = charUtils.codePointAt(text, i, stop);
        code = code*31 + Character.toLowerCase(codePointAt);
        i += Character.charCount(codePointAt);
      }
    } else {
      for (int i=offset; i<stop; i++) {
        code = code*31 + text[i];
      }
    }
    return code;
  }

  private int getHashCode(CharSequence text) {
    int code = 0;
    int len = text.length();
    if (ignoreCase) {
      for (int i=0; i<len;) {
        int codePointAt = charUtils.codePointAt(text, i);
        code = code*31 + Character.toLowerCase(codePointAt);
        i += Character.charCount(codePointAt);
      }
    } else {
      for (int i=0; i<len; i++) {
        code = code*31 + text.charAt(i);
      }
    }
    return code;
  }


  @Override
  public int size() {
    return count;
  }

  @Override
  public boolean isEmpty() {
    return count==0;
  }

  @Override
  public boolean contains(Object o) {
    if (o instanceof char[]) {
      final char[] text = (char[])o;
      return contains(text, 0, text.length);
    } 
    return contains(o.toString());
  }

  @Override
  public boolean add(Object o) {
    if (o instanceof char[]) {
      return add((char[])o);
    }
    return add(o.toString());
  }
  
  /**
   * Returns an unmodifiable {@link CharArraySet}. This allows to provide
   * unmodifiable views of internal sets for "read-only" use.
   * 
   * @param set
   *          a set for which the unmodifiable set is returned.
   * @return an new unmodifiable {@link CharArraySet}.
   * @throws NullPointerException
   *           if the given set is <code>null</code>.
   */
  public static CharArraySet unmodifiableSet(CharArraySet set) {
    if (set == null)
      throw new NullPointerException("Given set is null");
    if (set == EMPTY_SET)
      return EMPTY_SET;
    if (set instanceof UnmodifiableCharArraySet)
      return set;

    /*
     * Instead of delegating calls to the given set copy the low-level values to
     * the unmodifiable Subclass
     */
    return new UnmodifiableCharArraySet(set.matchVersion, set.entries, set.ignoreCase, set.count);
  }

  /**
   * Returns a copy of the given set as a {@link CharArraySet}. If the given set
   * is a {@link CharArraySet} the ignoreCase property will be preserved.
   * 
   * @param set
   *          a set to copy
   * @return a copy of the given set as a {@link CharArraySet}. If the given set
   *         is a {@link CharArraySet} the ignoreCase and matchVersion property will be
   *         preserved.
   * @deprecated use {@link #copy(Version, Set)} instead.
   */
  @Deprecated
  public static CharArraySet copy(final Set<?> set) {
    if(set == EMPTY_SET)
      return EMPTY_SET;
    return (set instanceof CharArraySet) ? copy((CharArraySet) set) : copy(Version.LUCENE_30, set);
  }
  
  /**
   * Returns a copy of the given set as a {@link CharArraySet}. If the given set
   * is a {@link CharArraySet} the ignoreCase property will be preserved.
   * <p>
   * <b>Note:</b> If you intend to create a copy of another {@link CharArraySet} where
   * the {@link Version} of the source set differs from its copy
   * {@link #CharArraySet(Version, Collection, boolean)} should be used instead.
   * The {@link #copy(Version, Set)} will preserve the {@link Version} of the
   * source set it is an instance of {@link CharArraySet}.
   * </p>
   * 
   * @param matchVersion
   *          compatibility match version see <a href="#version">Version
   *          note</a> above for details. This argument will be ignored if the
   *          given set is a {@link CharArraySet}.
   * @param set
   *          a set to copy
   * @return a copy of the given set as a {@link CharArraySet}. If the given set
   *         is a {@link CharArraySet} the ignoreCase property as well as the
   *         matchVersion will be of the given set will be preserved.
   */
  public static CharArraySet copy(final Version matchVersion, final Set<?> set) {
    if(set == EMPTY_SET)
      return EMPTY_SET;
    if(set instanceof CharArraySet) {
      final CharArraySet source = (CharArraySet) set;
      // use fast path instead of iterating all values
      // this is even on very small sets ~10 times faster than iterating
      final char[][] entries = new char[source.entries.length][];
      System.arraycopy(source.entries, 0, entries, 0, entries.length);
      return new CharArraySet(source.matchVersion, entries, source.ignoreCase, source.count);
    }
    return new CharArraySet(matchVersion, set, false);
  }
  

  /** The Iterator<String> for this set.  Strings are constructed on the fly, so
   * use <code>nextCharArray</code> for more efficient access. */
  public class CharArraySetIterator implements Iterator<String> {
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
    public String next() {
      return new String(nextCharArray());
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  /** returns an iterator of new allocated Strings */
  public Iterator<String> stringIterator() {
    return new CharArraySetIterator();
  }

  /** returns an iterator of new allocated Strings, this method violates the Set interface */
  @Override
  @SuppressWarnings("unchecked")
  public Iterator<Object> iterator() {
    return (Iterator) stringIterator();
  }
  
  /**
   * Efficient unmodifiable {@link CharArraySet}. This implementation does not
   * delegate calls to a give {@link CharArraySet} like
   * {@link Collections#unmodifiableSet(java.util.Set)} does. Instead is passes
   * the internal representation of a {@link CharArraySet} to a super
   * constructor and overrides all mutators. 
   */
  private static final class UnmodifiableCharArraySet extends CharArraySet {

    private UnmodifiableCharArraySet(Version matchVersion, char[][] entries, boolean ignoreCase,
        int count) {
      super(matchVersion, entries, ignoreCase, count);
    }

    @Override
    public void clear() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean add(Object o){
      throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean addAll(Collection<? extends Object> coll) {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean add(char[] text) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean add(CharSequence text) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean add(String text) {
      throw new UnsupportedOperationException();
    }
  }

}
