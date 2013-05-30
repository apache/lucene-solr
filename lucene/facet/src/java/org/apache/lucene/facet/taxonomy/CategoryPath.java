package org.apache.lucene.facet.taxonomy;

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

import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * Holds a sequence of string components, specifying the hierarchical name of a
 * category.
 * 
 * @lucene.experimental
 */
public class CategoryPath implements Comparable<CategoryPath> {

  /** An empty {@link CategoryPath} */
  public static final CategoryPath EMPTY = new CategoryPath();

  /**
   * The components of this {@link CategoryPath}. Note that this array may be
   * shared with other {@link CategoryPath} instances, e.g. as a result of
   * {@link #subpath(int)}, therefore you should traverse the array up to
   * {@link #length} for this path's components.
   */
  public final String[] components;

  /** The number of components of this {@link CategoryPath}. */
  public final int length;

  // Used by singleton EMPTY
  private CategoryPath() {
    components = null;
    length = 0;
  }

  // Used by subpath
  private CategoryPath(final CategoryPath copyFrom, final int prefixLen) {
    // while the code which calls this method is safe, at some point a test
    // tripped on AIOOBE in toString, but we failed to reproduce. adding the
    // assert as a safety check.
    assert prefixLen > 0 && prefixLen <= copyFrom.components.length : 
      "prefixLen cannot be negative nor larger than the given components' length: prefixLen=" + prefixLen
        + " components.length=" + copyFrom.components.length;
    this.components = copyFrom.components;
    length = prefixLen;
  }
  
  /** Construct from the given path components. */
  public CategoryPath(final String... components) {
    assert components.length > 0 : "use CategoryPath.EMPTY to create an empty path";
    for (String comp : components) {
      if (comp == null || comp.isEmpty()) {
        throw new IllegalArgumentException("empty or null components not allowed: " + Arrays.toString(components));
      }
    }
    this.components = components;
    length = components.length;
  }

  /** Construct from a given path, separating path components with {@code delimiter}. */
  public CategoryPath(final String pathString, final char delimiter) {
    String[] comps = pathString.split(Pattern.quote(Character.toString(delimiter)));
    if (comps.length == 1 && comps[0].isEmpty()) {
      components = null;
      length = 0;
    } else {
      for (String comp : comps) {
        if (comp == null || comp.isEmpty()) {
          throw new IllegalArgumentException("empty or null components not allowed: " + Arrays.toString(comps));
        }
      }
      components = comps;
      length = components.length;
    }
  }

  /**
   * Returns the number of characters needed to represent the path, including
   * delimiter characters, for using with
   * {@link #copyFullPath(char[], int, char)}.
   */
  public int fullPathLength() {
    if (length == 0) return 0;
    
    int charsNeeded = 0;
    for (int i = 0; i < length; i++) {
      charsNeeded += components[i].length();
    }
    charsNeeded += length - 1; // num delimter chars
    return charsNeeded;
  }

  /**
   * Compares this path with another {@link CategoryPath} for lexicographic
   * order.
   */
  @Override
  public int compareTo(CategoryPath other) {
    final int len = length < other.length ? length : other.length;
    for (int i = 0, j = 0; i < len; i++, j++) {
      int cmp = components[i].compareTo(other.components[j]);
      if (cmp < 0) return -1; // this is 'before'
      if (cmp > 0) return 1; // this is 'after'
    }
    
    // one is a prefix of the other
    return length - other.length;
  }

  private void hasDelimiter(String offender, char delimiter) {
    throw new IllegalArgumentException("delimiter character '" + delimiter + "' (U+" + Integer.toHexString(delimiter) + ") appears in path component \"" + offender + "\"");
  }

  private void noDelimiter(char[] buf, int offset, int len, char delimiter) {
    for(int idx=0;idx<len;idx++) {
      if (buf[offset+idx] == delimiter) {
        hasDelimiter(new String(buf, offset, len), delimiter);
      }
    }
  }

  /**
   * Copies the path components to the given {@code char[]}, starting at index
   * {@code start}. {@code delimiter} is copied between the path components.
   * Returns the number of chars copied.
   * 
   * <p>
   * <b>NOTE:</b> this method relies on the array being large enough to hold the
   * components and separators - the amount of needed space can be calculated
   * with {@link #fullPathLength()}.
   */
  public int copyFullPath(char[] buf, int start, char delimiter) {
    if (length == 0) {
      return 0;
    }

    int idx = start;
    int upto = length - 1;
    for (int i = 0; i < upto; i++) {
      int len = components[i].length();
      components[i].getChars(0, len, buf, idx);
      noDelimiter(buf, idx, len, delimiter);
      idx += len;
      buf[idx++] = delimiter;
    }
    components[upto].getChars(0, components[upto].length(), buf, idx);
    noDelimiter(buf, idx, components[upto].length(), delimiter);
    
    return idx + components[upto].length() - start;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof CategoryPath)) {
      return false;
    }
    
    CategoryPath other = (CategoryPath) obj;
    if (length != other.length) {
      return false; // not same length, cannot be equal
    }
    
    // CategoryPaths are more likely to differ at the last components, so start
    // from last-first
    for (int i = length - 1; i >= 0; i--) {
      if (!components[i].equals(other.components[i])) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    if (length == 0) {
      return 0;
    }
    
    int hash = length;
    for (int i = 0; i < length; i++) {
      hash = hash * 31 + components[i].hashCode();
    }
    return hash;
  }

  /** Calculate a 64-bit hash function for this path. */
  public long longHashCode() {
    if (length == 0) {
      return 0;
    }
    
    long hash = length;
    for (int i = 0; i < length; i++) {
      hash = hash * 65599 + components[i].hashCode();
    }
    return hash;
  }

  /** Returns a sub-path of this path up to {@code length} components. */
  public CategoryPath subpath(final int length) {
    if (length >= this.length || length < 0) {
      return this;
    } else if (length == 0) {
      return EMPTY;
    } else {
      return new CategoryPath(this, length);
    }
  }

  /**
   * Returns a string representation of the path, separating components with
   * '/'.
   * 
   * @see #toString(char)
   */
  @Override
  public String toString() {
    return toString('/');
  }

  /**
   * Returns a string representation of the path, separating components with the
   * given delimiter.
   */
  public String toString(char delimiter) {
    if (length == 0) return "";
    
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < length; i++) {
      if (components[i].indexOf(delimiter) != -1) {
        hasDelimiter(components[i], delimiter);
      }
      sb.append(components[i]).append(delimiter);
    }
    sb.setLength(sb.length() - 1); // remove last delimiter
    return sb.toString();
  }

}
