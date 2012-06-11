package org.apache.lucene.facet.taxonomy;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Serializable;

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

/**
 * A CategoryPath holds a sequence of string components, specifying the
 * hierarchical name of a category.
 * <P>
 * CategoryPath is designed to reduce the number of object allocations, in two
 * ways: First, it keeps the components internally in two arrays, rather than
 * keeping individual strings. Second, it allows reusing the same CategoryPath
 * object (which can be clear()ed and new components add()ed again) and of
 * add()'s parameter (which can be a reusable object, not just a string).
 * 
 * @lucene.experimental
 */
public class CategoryPath implements Serializable, Cloneable, Comparable<CategoryPath> {

  // A category path is a sequence of string components. It is kept
  // internally as one large character array "chars" with all the string
  // concatenated (without separators), and an array of integers "ends"
  // pointing to the/ end of each component. Both arrays may be larger
  // than actually in use. An additional integer, "ncomponents" specifies
  // how many components are actually set.
  // We use shorts instead of ints for "ends" to save a bit of space. This
  // means that our path lengths are limited to 32767 characters - which
  // should not be a problem in any realistic scenario.
  protected char[] chars;
  protected short[] ends;
  protected short ncomponents;

  /**
   * Return the number of components in the facet path. Note that this is
   * <I>not</I> the number of characters, but the number of components.
   */
  public short length() {
    return ncomponents;
  }

  /**
   * Trim the last components from the path.
   * 
   * @param nTrim
   *            Number of components to trim. If larger than the number of
   *            components this path has, the entire path will be cleared.
   */
  public void trim(int nTrim) {
    if (nTrim >= this.ncomponents) {
      clear();
    } else if (nTrim > 0) {
      this.ncomponents -= nTrim;
    }
  }

  /**
   * Returns the current character capacity of the CategoryPath. The character
   * capacity is the size of the internal buffer used to hold the characters
   * of all the path's components. When a component is added and the capacity
   * is not big enough, the buffer is automatically grown, and capacityChars()
   * increases.
   */
  public int capacityChars() {
    return chars.length;
  }

  /**
   * Returns the current component capacity of the CategoryPath. The component
   * capacity is the maximum number of components that the internal buffer can
   * currently hold. When a component is added beyond this capacity, the
   * buffer is automatically grown, and capacityComponents() increases.
   */
  public int capacityComponents() {
    return ends.length;
  }

  /**
   * Construct a new empty CategoryPath object. CategoryPath objects are meant
   * to be reused, by add()ing components, and later clear()ing, and add()ing
   * components again. The CategoryPath object is created with a buffer
   * pre-allocated for a given number of characters and components, but the
   * buffer will grow as necessary (see {@link #capacityChars()} and
   * {@link #capacityComponents()}).
   */
  public CategoryPath(int capacityChars, int capacityComponents) {
    ncomponents = 0;
    chars = new char[capacityChars];
    ends = new short[capacityComponents];
  }

  /**
   * Create an empty CategoryPath object. Equivalent to the constructor
   * {@link #CategoryPath(int, int)} with the two initial-capacity arguments
   * set to zero.
   */
  public CategoryPath() {
    this(0, 0);
  }

  /**
   * Add the given component to the end of the path.
   * <P>
   * Note that when a String object is passed to this method, a reference to
   * it is not saved (rather, its content is copied), which will lead to that
   * String object being gc'ed. To reduce the number of garbage objects, you
   * can pass a mutable CharBuffer instead of an immutable String to this
   * method.
   */
  public void add(CharSequence component) {
    // Set the new end, increasing the "ends" array sizes if necessary:
    if (ncomponents >= ends.length) {
      short[] newends = new short[(ends.length + 1) * 2];
      System.arraycopy(ends, 0, newends, 0, ends.length);
      ends = newends;
    }
    short prevend = (ncomponents == 0) ? 0 : ends[ncomponents - 1];
    int cmplen = component.length();
    ends[ncomponents] = (short) (prevend + cmplen);

    // Copy the new component's characters, increasing the "chars" array
    // sizes if necessary:
    if (ends[ncomponents] > chars.length) {
      char[] newchars = new char[ends[ncomponents] * 2];
      System.arraycopy(chars, 0, newchars, 0, chars.length);
      chars = newchars;
    }
    for (int i = 0; i < cmplen; i++) {
      chars[prevend++] = component.charAt(i);
    }

    ncomponents++;
  }

  /**
   * Empty the CategoryPath object, so that it has zero components. The
   * capacity of the object (see {@link #capacityChars()} and
   * {@link #capacityComponents()}) is not reduced, so that the object can be
   * reused without frequent reallocations.
   */
  public void clear() {
    ncomponents = 0;
  }

  /**
   * Build a string representation of the path, with its components separated
   * by the given delimiter character. The resulting string is appended to a
   * given Appendable, e.g., a StringBuilder, CharBuffer or Writer.
   * <P>
   * Note that the two cases of zero components and one component with zero
   * length produce indistinguishable results (both of them append nothing).
   * This is normally not a problem, because components should not normally
   * have zero lengths.
   * <P>
   * An IOException can be thrown if the given Appendable's append() throws
   * this exception.
   */
  public void appendTo(Appendable out, char delimiter) throws IOException {
    if (ncomponents == 0) {
      return; // just append nothing...
    }
    for (int i = 0; i < ends[0]; i++) {
      out.append(chars[i]);
    }
    for (int j = 1; j < ncomponents; j++) {
      out.append(delimiter);
      for (int i = ends[j - 1]; i < ends[j]; i++) {
        out.append(chars[i]);
      }
    }
  }

  /**
   * like {@link #appendTo(Appendable, char)}, but takes only a prefix of the
   * path, rather than the whole path.
   * <P>
   * If the given prefix length is negative or bigger than the path's actual
   * length, the whole path is taken.
   */
  public void appendTo(Appendable out, char delimiter, int prefixLen)
      throws IOException {
    if (prefixLen < 0 || prefixLen > ncomponents) {
      prefixLen = ncomponents;
    }
    if (prefixLen == 0) {
      return; // just append nothing...
    }
    for (int i = 0; i < ends[0]; i++) {
      out.append(chars[i]);
    }
    for (int j = 1; j < prefixLen; j++) {
      out.append(delimiter);
      for (int i = ends[j - 1]; i < ends[j]; i++) {
        out.append(chars[i]);
      }
    }
  }

  /**
   * like {@link #appendTo(Appendable, char)}, but takes only a part of the
   * path, rather than the whole path.
   * <P>
   * <code>start</code> specifies the first component in the subpath, and
   * <code>end</code> is one past the last component. If <code>start</code> is
   * negative, 0 is assumed, and if <code>end</code> is negative or past the
   * end of the path, the path is taken until the end. Otherwise, if
   * <code>end<=start</code>, nothing is appended. Nothing is appended also in
   * the case that the path is empty.
   */
  public void appendTo(Appendable out, char delimiter, int start, int end)
      throws IOException {
    if (start < 0) {
      start = 0;
    }
    if (end < 0 || end > ncomponents) {
      end = ncomponents;
    }
    if (end <= start) {
      return; // just append nothing...
    }
    for (int i = (start == 0 ? 0 : ends[start - 1]); i < ends[start]; i++) {
      out.append(chars[i]);
    }
    for (int j = start + 1; j < end; j++) {
      out.append(delimiter);
      for (int i = ends[j - 1]; i < ends[j]; i++) {
        out.append(chars[i]);
      }
    }
  }

  /**
   * Build a string representation of the path, with its components separated
   * by the given delimiter character. The resulting string is returned as a
   * new String object. To avoid this temporary object creation, consider
   * using {@link #appendTo(Appendable, char)} instead.
   * <P>
   * Note that the two cases of zero components and one component with zero
   * length produce indistinguishable results (both of them return an empty
   * string). This is normally not a problem, because components should not
   * normally have zero lengths.
   */
  public String toString(char delimiter) {
    if (ncomponents == 0) {
      return "";
    }
    StringBuilder sb = new StringBuilder(ends[ncomponents - 1]
        + (ncomponents - 1));
    try {
      this.appendTo(sb, delimiter);
    } catch (IOException e) {
      // can't happen, because StringBuilder.append() never actually
      // throws an exception!
    }
    return sb.toString();
  }

  /**
   * This method, an implementation of the {@link Object#toString()}
   * interface, is to allow simple printing of a CategoryPath, for debugging
   * purposes. When possible, it recommended to avoid using it it, and rather,
   * if you want to output the path with its components separated by a
   * delimiter character, specify the delimiter explicitly, with
   * {@link #toString(char)}.
   */
  @Override
  public String toString() {
    return toString('/');
  }

  /**
   * like {@link #toString(char)}, but takes only a prefix with a given number
   * of components, rather than the whole path.
   * <P>
   * If the given length is negative or bigger than the path's actual length,
   * the whole path is taken.
   */
  public String toString(char delimiter, int prefixLen) {
    if (prefixLen < 0 || prefixLen > ncomponents) {
      prefixLen = ncomponents;
    }
    if (prefixLen == 0) {
      return "";
    }
    StringBuilder sb = new StringBuilder(ends[prefixLen - 1]
        + (prefixLen - 1));
    try {
      this.appendTo(sb, delimiter, prefixLen);
    } catch (IOException e) {
      // can't happen, because sb.append() never actually throws an
      // exception
    }
    return sb.toString();
  }

  /**
   * like {@link #toString(char)}, but takes only a part of the path, rather
   * than the whole path.
   * <P>
   * <code>start</code> specifies the first component in the subpath, and
   * <code>end</code> is one past the last component. If <code>start</code> is
   * negative, 0 is assumed, and if <code>end</code> is negative or past the
   * end of the path, the path is taken until the end. Otherwise, if
   * <code>end<=start</code>, an empty string is returned. An emptry string is
   * returned also in the case that the path is empty.
   */
  public String toString(char delimiter, int start, int end) {
    if (start < 0) {
      start = 0;
    }
    if (end < 0 || end > ncomponents) {
      end = ncomponents;
    }
    if (end <= start) {
      return "";
    }
    int startchar = (start == 0) ? 0 : ends[start - 1];
    StringBuilder sb = new StringBuilder(ends[end - 1] - startchar
        + (end - start) - 1);
    try {
      this.appendTo(sb, delimiter, start, end);
    } catch (IOException e) {
      // can't happen, because sb.append() never actually throws an
      // exception
    }
    return sb.toString();
  }

  /**
   * Return the i'th component of the path, in a new String object. If there
   * is no i'th component, a null is returned.
   */
  public String getComponent(int i) {
    if (i < 0 || i >= ncomponents) {
      return null;
    }
    if (i == 0) {
      return new String(chars, 0, ends[0]);
    }
    return new String(chars, ends[i - 1], ends[i] - ends[i - 1]);
  }

  /**
   * Return the last component of the path, in a new String object. If the
   * path is empty, a null is returned.
   */
  public String lastComponent() {
    if (ncomponents == 0) {
      return null;
    }
    if (ncomponents == 1) {
      return new String(chars, 0, ends[0]);
    }
    return new String(chars, ends[ncomponents - 2], ends[ncomponents - 1]
        - ends[ncomponents - 2]);
  }

  /**
   * Copies the specified number of components from this category path to the
   * specified character array, with the components separated by a given
   * delimiter character. The array must be large enough to hold the
   * components and separators - the amount of needed space can be calculated
   * with {@link #charsNeededForFullPath()}.
   * <P>
   * This method returns the number of characters written to the array.
   * 
   * @param outputBuffer
   *            The destination character array.
   * @param outputBufferStart
   *            The first location to write in the output array.
   * @param numberOfComponentsToCopy
   *            The number of path components to write to the destination
   *            buffer.
   * @param separatorChar
   *            The separator inserted between every pair of path components
   *            in the output buffer.
   * @see #charsNeededForFullPath()
   */
  public int copyToCharArray(char[] outputBuffer, int outputBufferStart,
      int numberOfComponentsToCopy, char separatorChar) {
    if (numberOfComponentsToCopy == 0) {
      return 0;
    }
    if (numberOfComponentsToCopy < 0
        || numberOfComponentsToCopy > ncomponents) {
      numberOfComponentsToCopy = ncomponents;
    }
    int outputBufferInitialStart = outputBufferStart; // for calculating
                              // chars copied.
    int sourceStart = 0;
    int sourceLength = ends[0];
    for (int component = 0; component < numberOfComponentsToCopy; component++) {
      if (component > 0) {
        sourceStart = ends[component - 1];
        sourceLength = ends[component] - sourceStart;
        outputBuffer[outputBufferStart++] = separatorChar;
      }
      System.arraycopy(chars, sourceStart, outputBuffer,
          outputBufferStart, sourceLength);
      outputBufferStart += sourceLength;
    }
    return outputBufferStart - outputBufferInitialStart;
  }

  /**
   * Returns the number of characters required to represent this entire
   * category path, if written using
   * {@link #copyToCharArray(char[], int, int, char)} or
   * {@link #appendTo(Appendable, char)}. This includes the number of
   * characters in all the components, plus the number of separators between
   * them (each one character in the aforementioned methods).
   */
  public int charsNeededForFullPath() {
    if (ncomponents == 0) {
      return 0;
    }
    return ends[ncomponents - 1] + ncomponents - 1;
  }

  /**
   * Construct a new CategoryPath object, given a single string with
   * components separated by a given delimiter character.
   * <P>
   * The initial capacity of the constructed object will be exactly what is
   * needed to hold the given path. This fact is convenient when creating a
   * temporary object that will not be reused later.
   */
  public CategoryPath(String pathString, char delimiter) {
    if (pathString.length() == 0) {
      ncomponents = 0;
      chars = new char[0];
      ends = new short[0];
      return;
    }

    // This constructor is often used for creating a temporary object
    // (one which will not be reused to hold multiple paths), so we want
    // to do our best to allocate exactly the needed size - not less (to
    // avoid reallocation) and not more (so as not to waste space).
    // To do this, we unfortunately need to make an additional pass on the
    // given string:
    int nparts = 1;
    for (int i = pathString.indexOf(delimiter); i >= 0; i = pathString
        .indexOf(delimiter, i + 1)) {
      nparts++;
    }

    ends = new short[nparts];
    chars = new char[pathString.length() - nparts + 1];
    ncomponents = 0;

    add(pathString, delimiter);
  }

  /**
   * Add the given components to the end of the path. The components are given
   * in a single string, separated by a given delimiter character. If the
   * given string is empty, it is assumed to refer to the root (empty)
   * category, and nothing is added to the path (rather than adding a single
   * empty component).
   * <P>
   * Note that when a String object is passed to this method, a reference to
   * it is not saved (rather, its content is copied), which will lead to that
   * String object being gc'ed. To reduce the number of garbage objects, you
   * can pass a mutable CharBuffer instead of an immutable String to this
   * method.
   */
  public void add(CharSequence pathString, char delimiter) {
    int len = pathString.length();
    if (len == 0) {
      return; // assume root category meant, so add nothing.
    }
    short pos = (ncomponents == 0) ? 0 : ends[ncomponents - 1];
    for (int i = 0; i < len; i++) {
      char c = pathString.charAt(i);
      if (c == delimiter) {
        if (ncomponents >= ends.length) {
          short[] newends = new short[(ends.length + 1) * 2];
          System.arraycopy(ends, 0, newends, 0, ends.length);
          ends = newends;
        }
        ends[ncomponents++] = pos;
      } else {
        if (pos >= chars.length) {
          char[] newchars = new char[(chars.length + 1) * 2];
          System.arraycopy(chars, 0, newchars, 0, chars.length);
          chars = newchars;
        }
        chars[pos++] = c;
      }
    }

    // Don't forget to count the last component!
    if (ncomponents >= ends.length) {
      short[] newends = new short[(ends.length + 1) * 2];
      System.arraycopy(ends, 0, newends, 0, ends.length);
      ends = newends;
    }
    ends[ncomponents++] = pos;
  }

  /**
   * Construct a new CategoryPath object, copying an existing path given as an
   * array of strings.
   * <P>
   * The new object occupies exactly the space it needs, without any spare
   * capacity. This is the expected behavior in the typical use case, where
   * this constructor is used to create a temporary object which is never
   * reused.
   */
  public CategoryPath(CharSequence... components) {
    this.ncomponents = (short) components.length;
    this.ends = new short[ncomponents];
    if (ncomponents > 0) {
      this.ends[0] = (short) components[0].length();
      for (int i = 1; i < ncomponents; i++) {
        this.ends[i] = (short) (this.ends[i - 1] + components[i]
            .length());
      }
      this.chars = new char[this.ends[ncomponents - 1]];
      CharSequence cs = components[0];
      if (cs instanceof String) {
        ((String) cs).getChars(0, cs.length(), this.chars, 0);
      } else {
        for (int j = 0, k = cs.length(); j < k; j++) {
          this.chars[j] = cs.charAt(j);
        }
      }
      for (int i = 1; i < ncomponents; i++) {
        cs = components[i];
        int offset = this.ends[i - 1];
        if (cs instanceof String) {
          ((String) cs).getChars(0, cs.length(), this.chars, offset);
        } else {
          for (int j = 0, k = cs.length(); j < k; j++) {
            this.chars[j + offset] = cs.charAt(j);
          }
        }
      }
    } else {
      this.chars = new char[0];
    }
  }

  /**
   * Construct a new CategoryPath object, copying the path given in an
   * existing CategoryPath object.
   * <P>
   * This copy-constructor is handy when you need to save a reference to a
   * CategoryPath (e.g., when it serves as a key to a hash-table), but cannot
   * save a reference to the original object because its contents can be
   * changed later by the user. Copying the contents into a new object is a
   * solution.
   * <P>
   * This constructor </I>does not</I> copy the capacity (spare buffer size)
   * of the existing CategoryPath. Rather, the new object occupies exactly the
   * space it needs, without any spare. This is the expected behavior in the
   * typical use case outlined in the previous paragraph.
   */
  public CategoryPath(CategoryPath existing) {
    ncomponents = existing.ncomponents;
    if (ncomponents == 0) {
      chars = new char[0];
      ends = new short[0];
      return;
    }

    chars = new char[existing.ends[ncomponents - 1]];
    System.arraycopy(existing.chars, 0, chars, 0, chars.length);
    ends = new short[ncomponents];
    System.arraycopy(existing.ends, 0, ends, 0, ends.length);
  }

  /**
   * Construct a new CategoryPath object, copying a prefix with the given
   * number of components of the path given in an existing CategoryPath
   * object.
   * <P>
   * If the given length is negative or bigger than the given path's actual
   * length, the full path is taken.
   * <P>
   * This constructor is often convenient for creating a temporary object with
   * a path's prefix, but this practice is wasteful, and therefore
   * inadvisable. Rather, the application should be written in a way that
   * allows considering only a prefix of a given path, without needing to make
   * a copy of that path.
   */
  public CategoryPath(CategoryPath existing, int prefixLen) {
    if (prefixLen < 0 || prefixLen > existing.ncomponents) {
      ncomponents = existing.ncomponents;
    } else {
      ncomponents = (short) prefixLen;
    }
    if (ncomponents == 0) {
      chars = new char[0];
      ends = new short[0];
      return;
    }

    chars = new char[existing.ends[ncomponents - 1]];
    System.arraycopy(existing.chars, 0, chars, 0, chars.length);
    ends = new short[ncomponents];
    System.arraycopy(existing.ends, 0, ends, 0, ends.length);
  }

  @Override
  public CategoryPath clone() {
    return new CategoryPath(this);
  }

  /**
   * Compare the given CategoryPath to another one. For two category paths to
   * be considered equal, only the path they contain needs to be identical The
   * unused capacity of the objects is not considered in the comparison.
   */
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof CategoryPath) {
      CategoryPath other = (CategoryPath) obj;
      if (other.ncomponents != this.ncomponents) {
        return false;
      }
      // Unfortunately, Arrays.equal() can only compare entire arrays,
      // and in our case we potentially have unused parts of the arrays
      // that must not be compared... I wish that some future version
      // of Java has a offset and length parameter to Arrays.equal
      // (sort of like System.arraycopy()).
      if (ncomponents == 0) {
        return true; // nothing to compare...
      }
      for (int i = 0; i < ncomponents; i++) {
        if (this.ends[i] != other.ends[i]) {
          return false;
        }
      }
      int len = ends[ncomponents - 1]; 
      for (int i = 0; i < len; i++) {
        if (this.chars[i] != other.chars[i]) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  /**
   * Test whether this object is a descendant of another CategoryPath. This is
   * true if the other CategoryPath is the prefix of this.
   */
  public boolean isDescendantOf(CategoryPath other) {
    if (this.ncomponents < other.ncomponents) {
      return false;
    }
    int j = 0;
    for (int i = 0; i < other.ncomponents; i++) {
      if (ends[i] != other.ends[i]) {
        return false;
      }
      for (; j < ends[i]; j++) {
        if (this.chars[j] != other.chars[j]) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Calculate a hashCode for this path, used when a CategoryPath serves as a
   * hash-table key. If two objects are equal(), their hashCodes need to be
   * equal, so like in equal(), hashCode does not consider unused portions of
   * the internal buffers in its calculation.
   * <P>
   * The hash function used is modeled after Java's String.hashCode() - a
   * simple multiplicative hash function with the multiplier 31. The same hash
   * function also appeared in Kernighan & Ritchie's second edition of
   * "The C Programming Language" (1988).
   */
  @Override
  public int hashCode() {
    if (ncomponents == 0) {
      return 0;
    }
    int hash = ncomponents;
    // Unfortunately, Arrays.hashCode() can only calculate a hash code
    // for an entire arrays, and in our case we potentially have unused
    // parts of the arrays that must be ignored, so must use our own loop
    // over the characters. I wish that some future version of Java will
    // add offset and length parameters to Arrays.hashCode (sort of like
    // System.arraycopy()'s parameters).
    for (int i = 0; i < ncomponents; i++) {
      hash = hash * 31 + ends[i];
    }
    int len = ends[ncomponents - 1];
    for (int i = 0; i < len; i++) {
      hash = hash * 31 + chars[i];
    }
    return hash;
  }

  /**
   * Like {@link #hashCode()}, but find the hash function of a prefix with the
   * given number of components, rather than of the entire path.
   */
  public int hashCode(int prefixLen) {
    if (prefixLen < 0 || prefixLen > ncomponents) {
      prefixLen = ncomponents;
    }
    if (prefixLen == 0) {
      return 0;
    }
    int hash = prefixLen;
    for (int i = 0; i < prefixLen; i++) {
      hash = hash * 31 + ends[i];
    }
    int len = ends[prefixLen - 1];
    for (int i = 0; i < len; i++) {
      hash = hash * 31 + chars[i];
    }
    return hash;
  }

  /**
   * Calculate a 64-bit hash function for this path. Unlike
   * {@link #hashCode()}, this method is not part of the Java standard, and is
   * only used if explicitly called by the user.
   * <P>
   * If two objects are equal(), their hash codes need to be equal, so like in
   * {@link #equals(Object)}, longHashCode does not consider unused portions
   * of the internal buffers in its calculation.
   * <P>
   * The hash function used is a simple multiplicative hash function, with the
   * multiplier 65599. While Java's standard multiplier 31 (used in
   * {@link #hashCode()}) gives a good distribution for ASCII strings, it
   * turns out that for foreign-language strings (with 16-bit characters) it
   * gives too many collisions, and a bigger multiplier produces fewer
   * collisions in this case.
   */
  public long longHashCode() {
    if (ncomponents == 0) {
      return 0;
    }
    long hash = ncomponents;
    for (int i = 0; i < ncomponents; i++) {
      hash = hash * 65599 + ends[i];
    }
    int len = ends[ncomponents - 1];
    for (int i = 0; i < len; i++) {
      hash = hash * 65599 + chars[i];
    }
    return hash;
  }

  /**
   * Like {@link #longHashCode()}, but find the hash function of a prefix with
   * the given number of components, rather than of the entire path.
   */
  public long longHashCode(int prefixLen) {
    if (prefixLen < 0 || prefixLen > ncomponents) {
      prefixLen = ncomponents;
    }
    if (prefixLen == 0) {
      return 0;
    }
    long hash = prefixLen;
    for (int i = 0; i < prefixLen; i++) {
      hash = hash * 65599 + ends[i];
    }
    int len = ends[prefixLen - 1];
    for (int i = 0; i < len; i++) {
      hash = hash * 65599 + chars[i];
    }
    return hash;
  }

  /**
   * Write out a serialized (as a character sequence) representation of the
   * path to a given Appendable (e.g., a StringBuilder, CharBuffer, Writer, or
   * something similar.
   * <P>
   * This method may throw a IOException if the given Appendable threw this
   * exception while appending.
   */
  public void serializeAppendTo(Appendable out) throws IOException {
    // Note that we use the fact that ncomponents and ends[] are shorts,
    // so we can write them as chars:
    out.append((char) ncomponents);
    if (ncomponents == 0) {
      return;
    }
    for (int i = 0; i < ncomponents; i++) {
      out.append((char) ends[i]);
    }
    int usedchars = ends[ncomponents - 1];
    for (int i = 0; i < usedchars; i++) {
      out.append(chars[i]);
    }
  }

  /**
   * Just like {@link #serializeAppendTo(Appendable)}, but writes only a
   * prefix of the CategoryPath.
   */
  public void serializeAppendTo(int prefixLen, Appendable out)
      throws IOException {
    if (prefixLen < 0 || prefixLen > ncomponents) {
      prefixLen = ncomponents;
    }
    // Note that we use the fact that ncomponents and ends[] are shorts,
    // so we can write them as chars:
    out.append((char) prefixLen);
    if (prefixLen == 0) {
      return;
    }
    for (int i = 0; i < prefixLen; i++) {
      out.append((char) ends[i]);
    }
    int usedchars = ends[prefixLen - 1];
    for (int i = 0; i < usedchars; i++) {
      out.append(chars[i]);
    }
  }

  /**
   * Set a CategoryPath from a character-sequence representation written by
   * {@link #serializeAppendTo(Appendable)}.
   * <P>
   * Reading starts at the given offset into the given character sequence, and
   * the offset right after the end of this path is returned.
   */
  public int setFromSerialized(CharSequence buffer, int offset) {
    ncomponents = (short) buffer.charAt(offset++);
    if (ncomponents == 0) {
      return offset;
    }

    if (ncomponents >= ends.length) {
      ends = new short[Math.max(ends.length * 2, ncomponents)];
    }
    for (int i = 0; i < ncomponents; i++) {
      ends[i] = (short) buffer.charAt(offset++);
    }

    int usedchars = ends[ncomponents - 1];
    if (usedchars > chars.length) {
      chars = new char[Math.max(chars.length * 2, usedchars)];
    }
    for (int i = 0; i < usedchars; i++) {
      chars[i] = buffer.charAt(offset++);
    }

    return offset;
  }

  /**
   * Check whether the current path is identical to the one serialized (with
   * {@link #serializeAppendTo(Appendable)}) in the given buffer, at the given
   * offset.
   */
  public boolean equalsToSerialized(CharSequence buffer, int offset) {
    int n = (short) buffer.charAt(offset++);
    if (ncomponents != n) {
      return false;
    }
    if (ncomponents == 0) {
      return true;
    }
    for (int i = 0; i < ncomponents; i++) {
      if (ends[i] != (short) buffer.charAt(offset++)) {
        return false;
      }
    }
    int usedchars = ends[ncomponents - 1];
    for (int i = 0; i < usedchars; i++) {
      if (chars[i] != buffer.charAt(offset++)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Just like {@link #equalsToSerialized(CharSequence, int)}, but compare to
   * a prefix of the CategoryPath, instead of the whole CategoryPath.
   */
  public boolean equalsToSerialized(int prefixLen, CharSequence buffer,
      int offset) {
    if (prefixLen < 0 || prefixLen > ncomponents) {
      prefixLen = ncomponents;
    }
    int n = (short) buffer.charAt(offset++);
    if (prefixLen != n) {
      return false;
    }
    if (prefixLen == 0) {
      return true;
    }
    for (int i = 0; i < prefixLen; i++) {
      if (ends[i] != (short) buffer.charAt(offset++)) {
        return false;
      }
    }
    int usedchars = ends[prefixLen - 1];
    for (int i = 0; i < usedchars; i++) {
      if (chars[i] != buffer.charAt(offset++)) {
        return false;
      }
    }
    return true;
  }

  /**
   * This method calculates a hash function of a path that has been written to
   * (using {@link #serializeAppendTo(Appendable)}) a character buffer. It is
   * guaranteed that the value returned is identical to that which
   * {@link #hashCode()} would have produced for the original object before it
   * was serialized.
   */
  public static int hashCodeOfSerialized(CharSequence buffer, int offset) {
    // Note: the algorithm here must be identical to that of hashCode(),
    // in order that they produce identical results!
    int ncomponents = (short) buffer.charAt(offset++);
    if (ncomponents == 0) {
      return 0;
    }
    int hash = ncomponents;
    for (int i = 0; i < ncomponents; i++) {
      hash = hash * 31 + buffer.charAt(offset++);
    }
    int len = buffer.charAt(offset - 1);
    for (int i = 0; i < len; i++) {
      hash = hash * 31 + buffer.charAt(offset++);
    }
    return hash;
  }

  /**
   * Serializes the content of this CategoryPath to a byte stream, using UTF-8
   * encoding to convert characters to bytes, and treating the ends as 16-bit
   * characters. 
   * 
   * @param osw
   *          The output byte stream.
   * @throws IOException
   *           If there are encoding errors.
   */
  // TODO (Facet): consolidate all de/serialize method names to
  // serialize() and unserialize()
  public void serializeToStreamWriter(OutputStreamWriter osw)
      throws IOException {
    osw.write(this.ncomponents);
    if (this.ncomponents <= 0) {
      return;
    }
    for (int j = 0; j < this.ncomponents; j++) {
      osw.write(this.ends[j]);
    }
    osw.write(this.chars, 0, this.ends[this.ncomponents - 1]);
  }

  /**
   * Serializes the content of this CategoryPath to a byte stream, using UTF-8
   * encoding to convert characters to bytes, and treating the ends as 16-bit
   * characters.
   * 
   * @param isr
   *            The input stream.
   * @throws IOException
   *             If there are encoding errors.
   */
  public void deserializeFromStreamReader(InputStreamReader isr)
      throws IOException {
    this.ncomponents = (short) isr.read();
    if (this.ncomponents <= 0) {
      return;
    }
    if (this.ends == null || this.ends.length < this.ncomponents) {
      this.ends = new short[this.ncomponents];
    }
    for (int j = 0; j < this.ncomponents; j++) {
      this.ends[j] = (short) isr.read();
    }
    if (this.chars == null
        || this.ends[this.ncomponents - 1] > chars.length) {
      this.chars = new char[this.ends[this.ncomponents - 1]];
    }
    isr.read(this.chars, 0, this.ends[this.ncomponents - 1]);
  }

  private void writeObject(java.io.ObjectOutputStream out)
  throws IOException {
    OutputStreamWriter osw = new OutputStreamWriter(out, "UTF-8");
    this.serializeToStreamWriter(osw);
    osw.flush();
  }
  
  private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
    InputStreamReader isr = new InputStreamReader(in, "UTF-8");
    this.deserializeFromStreamReader(isr);
  }

  /**
   * Compares this CategoryPath with the other CategoryPath for lexicographic
   * order. 
   * Returns a negative integer, zero, or a positive integer as this
   * CategoryPath lexicographically precedes, equals to, or lexicographically follows 
   * the other CategoryPath.
   */
  public int compareTo(CategoryPath other) {
    int minlength = (this.length() < other.length()) ? this.length() : other.length();
    int ch = 0;
    for (int co = 0 ; co < minlength; co++) {
      if (this.ends[co] <= other.ends[co]) {
        for ( ; ch < this.ends[co] ; ch++) {
          if (this.chars[ch] != other.chars[ch]) {
            return this.chars[ch] - other.chars[ch];
          }
        }
        if (this.ends[co] < other.ends[co]) {
          return -1;
        }
      } else /* this.ends[co] > other.ends[co] */ {
        for ( ; ch < other.ends[co] ; ch++) {
          if (this.chars[ch] != other.chars[ch]) {
            return this.chars[ch] - other.chars[ch];
          }
        }
        return +1;
      }
    }
    // one is a prefix of the other
    return this.length() - other.length();
  }  
}
