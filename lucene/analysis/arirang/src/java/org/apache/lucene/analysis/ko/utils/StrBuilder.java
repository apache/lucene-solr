package org.apache.lucene.analysis.ko.utils;

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

import java.io.Reader;
import java.io.Writer;
import java.util.Collection;
import java.util.Iterator;

/**
 * Builds a string from constituent parts providing a more flexible and powerful API
 * than StringBuffer.
 * <p>
 * The main differences from StringBuffer/StringBuilder are:
 * <ul>
 * <li>Not synchronized</li>
 * <li>Not final</li>
 * <li>Subclasses have direct access to character array</li>
 * <li>Additional methods
 *  <ul>
 *   <li>appendWithSeparators - adds an array of values, with a separator</li>
 *   <li>appendPadding - adds a length padding characters</li>
 *   <li>appendFixedLength - adds a fixed width field to the builder</li>
 *   <li>toCharArray/getChars - simpler ways to get a range of the character array</li>
 *   <li>delete - delete char or string</li>
 *   <li>replace - search and replace for a char or string</li>
 *   <li>leftString/rightString/midString - substring without exceptions</li>
 *   <li>contains - whether the builder contains a char or string</li>
 *   <li>size/clear/isEmpty - collections style API methods</li>
 *  </ul>
 * </li>
 * </ul>
 * <li>Views
 *  <ul>
 *   <li>asTokenizer - uses the internal buffer as the source of a StrTokenizer</li>
 *   <li>asReader - uses the internal buffer as the source of a Reader</li>
 *   <li>asWriter - allows a Writer to write directly to the internal buffer</li>
 *  </ul>
 * </li>
 * </ul>
 * <p>
 * The aim has been to provide an API that mimics very closely what StringBuffer
 * provides, but with additional methods. It should be noted that some edge cases,
 * with invalid indices or null input, have been altered - see individual methods.
 * The biggest of these changes is that by default, null will not output the text
 * 'null'. This can be controlled by a property, {@link #setNullText(String)}.
 * <p>
 * Prior to 3.0, this class implemented Cloneable but did not implement the 
 * clone method so could not be used. From 3.0 onwards it no longer implements 
 * the interface. 
 */
public class StrBuilder implements Cloneable {

  /**
   * The extra capacity for new builders.
   */
  static final int CAPACITY = 32;

  /**
   * Required for serialization support.
   * 
   * @see java.io.Serializable
   */
  private static final long serialVersionUID = 7628716375283629643L;

  /**
   * An empty immutable <code>char</code> array.
   */
  public static final char[] EMPTY_CHAR_ARRAY = new char[0];

  /**
   * <p>
   * The <code>line.separator</code> System Property. Line separator (<code>&quot;\n&quot;</code> on UNIX).
   * </p>
   * 
   * <p>
   * Defaults to <code>null</code> if the runtime does not have
   * security access to read this property or the property does not exist.
   * </p>
   * 
   * <p>
   * This value is initialized when the class is loaded. If {@link System#setProperty(String,String)} or
   * {@link System#setProperties(java.util.Properties)} is called after this class is loaded, the value
   * will be out of sync with that System property.
   * </p>
   * 
   * @since Java 1.1
   */
  public static final String LINE_SEPARATOR = Utilities.getSystemProperty("line.separator");
    
  /** Internal data storage. */
  protected char[] buffer; // TODO make private?
  /** Current size of the buffer. */
  protected int size; // TODO make private?
  /** The new line. */
  private String newLine;
  /** The null text. */
  private String nullText;

  //-----------------------------------------------------------------------
  /**
   * Constructor that creates an empty builder initial capacity 32 characters.
   */
  public StrBuilder() {
    this(CAPACITY);
  }

  /**
   * Constructor that creates an empty builder the specified initial capacity.
   *
   * @param initialCapacity  the initial capacity, zero or less will be converted to 32
   */
  public StrBuilder(int initialCapacity) {
    super();
    if (initialCapacity <= 0) {
      initialCapacity = CAPACITY;
    }
    buffer = new char[initialCapacity];
  }

  /**
   * Constructor that creates a builder from the string, allocating
   * 32 extra characters for growth.
   *
   * @param str  the string to copy, null treated as blank string
   */
  public StrBuilder(String str) {
    super();
    if (str == null) {
      buffer = new char[CAPACITY];
    } else {
      buffer = new char[str.length() + CAPACITY];
      append(str);
    }
  }

  //-----------------------------------------------------------------------
  /**
   * Gets the text to be appended when a new line is added.
   *
   * @return the new line text, null means use system default
   */
  public String getNewLineText() {
    return newLine;
  }

  /**
   * Sets the text to be appended when a new line is added.
   *
   * @param newLine  the new line text, null means use system default
   * @return this, to enable chaining
   */
  public StrBuilder setNewLineText(String newLine) {
    this.newLine = newLine;
    return this;
  }

  //-----------------------------------------------------------------------
  /**
   * Gets the text to be appended when null is added.
   *
   * @return the null text, null means no append
   */
  public String getNullText() {
    return nullText;
  }

  /**
   * Sets the text to be appended when null is added.
   *
   * @param nullText  the null text, null means no append
   * @return this, to enable chaining
   */
  public StrBuilder setNullText(String nullText) {
    if (nullText != null && nullText.length() == 0) {
      nullText = null;
    }
    this.nullText = nullText;
    return this;
  }

  //-----------------------------------------------------------------------
  /**
   * Gets the length of the string builder.
   *
   * @return the length
   */
  public int length() {
    return size;
  }

  /**
   * Updates the length of the builder by either dropping the last characters
   * or adding filler of unicode zero.
   *
   * @param length  the length to set to, must be zero or positive
   * @return this, to enable chaining
   * @throws IndexOutOfBoundsException if the length is negative
   */
  public StrBuilder setLength(int length) {
    if (length < 0) {
      throw new StringIndexOutOfBoundsException(length);
    }
    if (length < size) {
      size = length;
    } else if (length > size) {
      ensureCapacity(length);
      int oldEnd = size;
      int newEnd = length;
      size = length;
      for (int i = oldEnd; i < newEnd; i++) {
        buffer[i] = '\0';
      }
    }
    return this;
  }

  //-----------------------------------------------------------------------
  /**
   * Gets the current size of the internal character array buffer.
   *
   * @return the capacity
   */
  public int capacity() {
    return buffer.length;
  }

  /**
   * Checks the capacity and ensures that it is at least the size specified.
   *
   * @param capacity  the capacity to ensure
   * @return this, to enable chaining
   */
  public StrBuilder ensureCapacity(int capacity) {
    if (capacity > buffer.length) {
      char[] old = buffer;
      buffer = new char[capacity * 2];
      System.arraycopy(old, 0, buffer, 0, size);
    }
    return this;
  }

  /**
   * Minimizes the capacity to the actual length of the string.
   *
   * @return this, to enable chaining
   */
  public StrBuilder minimizeCapacity() {
    if (buffer.length > length()) {
      char[] old = buffer;
      buffer = new char[length()];
      System.arraycopy(old, 0, buffer, 0, size);
    }
    return this;
  }

  //-----------------------------------------------------------------------
  /**
   * Gets the length of the string builder.
   * <p>
   * This method is the same as {@link #length()} and is provided to match the
   * API of Collections.
   *
   * @return the length
   */
  public int size() {
    return size;
  }

  /**
   * Checks is the string builder is empty (convenience Collections API style method).
   * <p>
   * This method is the same as checking {@link #length()} and is provided to match the
   * API of Collections.
   *
   * @return <code>true</code> if the size is <code>0</code>.
   */
  public boolean isEmpty() {
    return size == 0;
  }

  /**
   * Clears the string builder (convenience Collections API style method).
   * <p>
   * This method does not reduce the size of the internal character buffer.
   * To do that, call <code>clear()</code> followed by {@link #minimizeCapacity()}.
   * <p>
   * This method is the same as {@link #setLength(int)} called with zero
   * and is provided to match the API of Collections.
   *
   * @return this, to enable chaining
   */
  public StrBuilder clear() {
    size = 0;
    return this;
  }

  //-----------------------------------------------------------------------
  /**
   * Gets the character at the specified index.
   *
   * @see #setCharAt(int, char)
   * @see #deleteCharAt(int)
   * @param index  the index to retrieve, must be valid
   * @return the character at the index
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  public char charAt(int index) {
    if (index < 0 || index >= length()) {
      throw new StringIndexOutOfBoundsException(index);
    }
    return buffer[index];
  }

  /**
   * Sets the character at the specified index.
   *
   * @see #charAt(int)
   * @see #deleteCharAt(int)
   * @param index  the index to set
   * @param ch  the new character
   * @return this, to enable chaining
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  public StrBuilder setCharAt(int index, char ch) {
    if (index < 0 || index >= length()) {
      throw new StringIndexOutOfBoundsException(index);
    }
    buffer[index] = ch;
    return this;
  }

  /**
   * Deletes the character at the specified index.
   *
   * @see #charAt(int)
   * @see #setCharAt(int, char)
   * @param index  the index to delete
   * @return this, to enable chaining
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  public StrBuilder deleteCharAt(int index) {
    if (index < 0 || index >= size) {
      throw new StringIndexOutOfBoundsException(index);
    }
    deleteImpl(index, index + 1, 1);
    return this;
  }

  //-----------------------------------------------------------------------
  /**
   * Copies the builder's character array into a new character array.
   * 
   * @return a new array that represents the contents of the builder
   */
  public char[] toCharArray() {
    if (size == 0) {
      return EMPTY_CHAR_ARRAY;
    }
    char chars[] = new char[size];
    System.arraycopy(buffer, 0, chars, 0, size);
    return chars;
  }

  /**
   * Copies part of the builder's character array into a new character array.
   * 
   * @param startIndex  the start index, inclusive, must be valid
   * @param endIndex  the end index, exclusive, must be valid except that
   *  if too large it is treated as end of string
   * @return a new array that holds part of the contents of the builder
   * @throws IndexOutOfBoundsException if startIndex is invalid,
   *  or if endIndex is invalid (but endIndex greater than size is valid)
   */
  public char[] toCharArray(int startIndex, int endIndex) {
    endIndex = validateRange(startIndex, endIndex);
    int len = endIndex - startIndex;
    if (len == 0) {
      return EMPTY_CHAR_ARRAY;
    }
    char chars[] = new char[len];
    System.arraycopy(buffer, startIndex, chars, 0, len);
    return chars;
  }

  /**
   * Copies the character array into the specified array.
   * 
   * @param destination  the destination array, null will cause an array to be created
   * @return the input array, unless that was null or too small
   */
  public char[] getChars(char[] destination) {
    int len = length();
    if (destination == null || destination.length < len) {
      destination = new char[len];
    }
    System.arraycopy(buffer, 0, destination, 0, len);
    return destination;
  }

  /**
   * Copies the character array into the specified array.
   *
   * @param startIndex  first index to copy, inclusive, must be valid
   * @param endIndex  last index, exclusive, must be valid
   * @param destination  the destination array, must not be null or too small
   * @param destinationIndex  the index to start copying in destination
   * @throws NullPointerException if the array is null
   * @throws IndexOutOfBoundsException if any index is invalid
   */
  public void getChars(int startIndex, int endIndex, char destination[], int destinationIndex) {
    if (startIndex < 0) {
      throw new StringIndexOutOfBoundsException(startIndex);
    }
    if (endIndex < 0 || endIndex > length()) {
      throw new StringIndexOutOfBoundsException(endIndex);
    }
    if (startIndex > endIndex) {
      throw new StringIndexOutOfBoundsException("end < start");
    }
    System.arraycopy(buffer, startIndex, destination, destinationIndex, endIndex - startIndex);
  }

  //-----------------------------------------------------------------------
  /**
   * Appends the new line string to this string builder.
   * <p>
   * The new line string can be altered using {@link #setNewLineText(String)}.
   * This might be used to force the output to always use Unix line endings
   * even when on Windows.
   *
   * @return this, to enable chaining
   */
  public StrBuilder appendNewLine() {
    if (newLine == null)  {
      append(LINE_SEPARATOR);
      return this;
    }
    return append(newLine);
  }

  /**
   * Appends the text representing <code>null</code> to this string builder.
   *
   * @return this, to enable chaining
   */
  public StrBuilder appendNull() {
    if (nullText == null)  {
      return this;
    }
    return append(nullText);
  }

  /**
   * Appends an object to this string builder.
   * Appending null will call {@link #appendNull()}.
   *
   * @param obj  the object to append
   * @return this, to enable chaining
   */
  public StrBuilder append(Object obj) {
    if (obj == null) {
      return appendNull();
    } 
    return append(obj.toString());        
  }

  /**
   * Appends a string to this string builder.
   * Appending null will call {@link #appendNull()}.
   *
   * @param str  the string to append
   * @return this, to enable chaining
   */
  public StrBuilder append(String str) {
    if (str == null) {
      return appendNull();
    }
    int strLen = str.length();
    if (strLen > 0) {
      int len = length();
      ensureCapacity(len + strLen);
      str.getChars(0, strLen, buffer, len);
      size += strLen;
    }
    return this;
  }

  /**
   * Appends part of a string to this string builder.
   * Appending null will call {@link #appendNull()}.
   *
   * @param str  the string to append
   * @param startIndex  the start index, inclusive, must be valid
   * @param length  the length to append, must be valid
   * @return this, to enable chaining
   */
  public StrBuilder append(String str, int startIndex, int length) {
    if (str == null) {
      return appendNull();
    }
    if (startIndex < 0 || startIndex > str.length()) {
      throw new StringIndexOutOfBoundsException("startIndex must be valid");
    }
    if (length < 0 || (startIndex + length) > str.length()) {
      throw new StringIndexOutOfBoundsException("length must be valid");
    }
    if (length > 0) {
      int len = length();
      ensureCapacity(len + length);
      str.getChars(startIndex, startIndex + length, buffer, len);
      size += length;
    }
    return this;
  }

  /**
   * Appends a string buffer to this string builder.
   * Appending null will call {@link #appendNull()}.
   *
   * @param str  the string buffer to append
   * @return this, to enable chaining
   */
  public StrBuilder append(StringBuffer str) {
    if (str == null) {
      return appendNull();
    }
    int strLen = str.length();
    if (strLen > 0) {
      int len = length();
      ensureCapacity(len + strLen);
      str.getChars(0, strLen, buffer, len);
      size += strLen;
    }
    return this;
  }

  /**
   * Appends part of a string buffer to this string builder.
   * Appending null will call {@link #appendNull()}.
   *
   * @param str  the string to append
   * @param startIndex  the start index, inclusive, must be valid
   * @param length  the length to append, must be valid
   * @return this, to enable chaining
   */
  public StrBuilder append(StringBuffer str, int startIndex, int length) {
    if (str == null) {
      return appendNull();
    }
    if (startIndex < 0 || startIndex > str.length()) {
      throw new StringIndexOutOfBoundsException("startIndex must be valid");
    }
    if (length < 0 || (startIndex + length) > str.length()) {
      throw new StringIndexOutOfBoundsException("length must be valid");
    }
    if (length > 0) {
      int len = length();
      ensureCapacity(len + length);
      str.getChars(startIndex, startIndex + length, buffer, len);
      size += length;
    }
    return this;
  }

  /**
   * Appends another string builder to this string builder.
   * Appending null will call {@link #appendNull()}.
   *
   * @param str  the string builder to append
   * @return this, to enable chaining
   */
  public StrBuilder append(StrBuilder str) {
    if (str == null) {
      return appendNull();
    }
    int strLen = str.length();
    if (strLen > 0) {
      int len = length();
      ensureCapacity(len + strLen);
      System.arraycopy(str.buffer, 0, buffer, len, strLen);
      size += strLen;
    }
    return this;
  }

  /**
   * Appends part of a string builder to this string builder.
   * Appending null will call {@link #appendNull()}.
   *
   * @param str  the string to append
   * @param startIndex  the start index, inclusive, must be valid
   * @param length  the length to append, must be valid
   * @return this, to enable chaining
   */
  public StrBuilder append(StrBuilder str, int startIndex, int length) {
    if (str == null) {
      return appendNull();
    }
    if (startIndex < 0 || startIndex > str.length()) {
      throw new StringIndexOutOfBoundsException("startIndex must be valid");
    }
    if (length < 0 || (startIndex + length) > str.length()) {
      throw new StringIndexOutOfBoundsException("length must be valid");
    }
    if (length > 0) {
      int len = length();
      ensureCapacity(len + length);
      str.getChars(startIndex, startIndex + length, buffer, len);
      size += length;
    }
    return this;
  }

  /**
   * Appends a char array to the string builder.
   * Appending null will call {@link #appendNull()}.
   *
   * @param chars  the char array to append
   * @return this, to enable chaining
   */
  public StrBuilder append(char[] chars) {
    if (chars == null) {
      return appendNull();
    }
    int strLen = chars.length;
    if (strLen > 0) {
      int len = length();
      ensureCapacity(len + strLen);
      System.arraycopy(chars, 0, buffer, len, strLen);
      size += strLen;
    }
    return this;
  }

  /**
   * Appends a char array to the string builder.
   * Appending null will call {@link #appendNull()}.
   *
   * @param chars  the char array to append
   * @param startIndex  the start index, inclusive, must be valid
   * @param length  the length to append, must be valid
   * @return this, to enable chaining
   */
  public StrBuilder append(char[] chars, int startIndex, int length) {
    if (chars == null) {
      return appendNull();
    }
    if (startIndex < 0 || startIndex > chars.length) {
      throw new StringIndexOutOfBoundsException("Invalid startIndex: " + length);
    }
    if (length < 0 || (startIndex + length) > chars.length) {
      throw new StringIndexOutOfBoundsException("Invalid length: " + length);
    }
    if (length > 0) {
      int len = length();
      ensureCapacity(len + length);
      System.arraycopy(chars, startIndex, buffer, len, length);
      size += length;
    }
    return this;
  }

  /**
   * Appends a boolean value to the string builder.
   *
   * @param value  the value to append
   * @return this, to enable chaining
   */
  public StrBuilder append(boolean value) {
    if (value) {
      ensureCapacity(size + 4);
      buffer[size++] = 't';
      buffer[size++] = 'r';
      buffer[size++] = 'u';
      buffer[size++] = 'e';
    } else {
      ensureCapacity(size + 5);
      buffer[size++] = 'f';
      buffer[size++] = 'a';
      buffer[size++] = 'l';
      buffer[size++] = 's';
      buffer[size++] = 'e';
    }
    return this;
  }

  /**
   * Appends a char value to the string builder.
   *
   * @param ch  the value to append
   * @return this, to enable chaining
   */
  public StrBuilder append(char ch) {
    int len = length();
    ensureCapacity(len + 1);
    buffer[size++] = ch;
    return this;
  }

  /**
   * Appends an int value to the string builder using <code>String.valueOf</code>.
   *
   * @param value  the value to append
   * @return this, to enable chaining
   */
  public StrBuilder append(int value) {
    return append(String.valueOf(value));
  }

  /**
   * Appends a long value to the string builder using <code>String.valueOf</code>.
   *
   * @param value  the value to append
   * @return this, to enable chaining
   */
  public StrBuilder append(long value) {
    return append(String.valueOf(value));
  }

  /**
   * Appends a float value to the string builder using <code>String.valueOf</code>.
   *
   * @param value  the value to append
   * @return this, to enable chaining
   */
  public StrBuilder append(float value) {
    return append(String.valueOf(value));
  }

  /**
   * Appends a double value to the string builder using <code>String.valueOf</code>.
   *
   * @param value  the value to append
   * @return this, to enable chaining
   */
  public StrBuilder append(double value) {
    return append(String.valueOf(value));
  }

  //-----------------------------------------------------------------------
  /**
   * Appends an object followed by a new line to this string builder.
   * Appending null will call {@link #appendNull()}.
   *
   * @param obj  the object to append
   * @return this, to enable chaining
   * @since 2.3
   */
  public StrBuilder appendln(Object obj) {
    return append(obj).appendNewLine();
  }

  /**
   * Appends a string followed by a new line to this string builder.
   * Appending null will call {@link #appendNull()}.
   *
   * @param str  the string to append
   * @return this, to enable chaining
   * @since 2.3
   */
  public StrBuilder appendln(String str) {
    return append(str).appendNewLine();
  }

  /**
   * Appends part of a string followed by a new line to this string builder.
   * Appending null will call {@link #appendNull()}.
   *
   * @param str  the string to append
   * @param startIndex  the start index, inclusive, must be valid
   * @param length  the length to append, must be valid
   * @return this, to enable chaining
   * @since 2.3
   */
  public StrBuilder appendln(String str, int startIndex, int length) {
    return append(str, startIndex, length).appendNewLine();
  }

  /**
   * Appends a string buffer followed by a new line to this string builder.
   * Appending null will call {@link #appendNull()}.
   *
   * @param str  the string buffer to append
   * @return this, to enable chaining
   * @since 2.3
   */
  public StrBuilder appendln(StringBuffer str) {
    return append(str).appendNewLine();
  }

  /**
   * Appends part of a string buffer followed by a new line to this string builder.
   * Appending null will call {@link #appendNull()}.
   *
   * @param str  the string to append
   * @param startIndex  the start index, inclusive, must be valid
   * @param length  the length to append, must be valid
   * @return this, to enable chaining
   * @since 2.3
   */
  public StrBuilder appendln(StringBuffer str, int startIndex, int length) {
    return append(str, startIndex, length).appendNewLine();
  }

  /**
   * Appends another string builder followed by a new line to this string builder.
   * Appending null will call {@link #appendNull()}.
   *
   * @param str  the string builder to append
   * @return this, to enable chaining
   * @since 2.3
   */
  public StrBuilder appendln(StrBuilder str) {
    return append(str).appendNewLine();
  }

  /**
   * Appends part of a string builder followed by a new line to this string builder.
   * Appending null will call {@link #appendNull()}.
   *
   * @param str  the string to append
   * @param startIndex  the start index, inclusive, must be valid
   * @param length  the length to append, must be valid
   * @return this, to enable chaining
   * @since 2.3
   */
  public StrBuilder appendln(StrBuilder str, int startIndex, int length) {
    return append(str, startIndex, length).appendNewLine();
  }

  /**
   * Appends a char array followed by a new line to the string builder.
   * Appending null will call {@link #appendNull()}.
   *
   * @param chars  the char array to append
   * @return this, to enable chaining
   * @since 2.3
   */
  public StrBuilder appendln(char[] chars) {
    return append(chars).appendNewLine();
  }

  /**
   * Appends a char array followed by a new line to the string builder.
   * Appending null will call {@link #appendNull()}.
   *
   * @param chars  the char array to append
   * @param startIndex  the start index, inclusive, must be valid
   * @param length  the length to append, must be valid
   * @return this, to enable chaining
   * @since 2.3
   */
  public StrBuilder appendln(char[] chars, int startIndex, int length) {
    return append(chars, startIndex, length).appendNewLine();
  }

  /**
   * Appends a boolean value followed by a new line to the string builder.
   *
   * @param value  the value to append
   * @return this, to enable chaining
   * @since 2.3
   */
  public StrBuilder appendln(boolean value) {
    return append(value).appendNewLine();
  }

  /**
   * Appends a char value followed by a new line to the string builder.
   *
   * @param ch  the value to append
   * @return this, to enable chaining
   * @since 2.3
   */
  public StrBuilder appendln(char ch) {
    return append(ch).appendNewLine();
  }

  /**
   * Appends an int value followed by a new line to the string builder using <code>String.valueOf</code>.
   *
   * @param value  the value to append
   * @return this, to enable chaining
   * @since 2.3
   */
  public StrBuilder appendln(int value) {
    return append(value).appendNewLine();
  }

  /**
   * Appends a long value followed by a new line to the string builder using <code>String.valueOf</code>.
   *
   * @param value  the value to append
   * @return this, to enable chaining
   * @since 2.3
   */
  public StrBuilder appendln(long value) {
    return append(value).appendNewLine();
  }

  /**
   * Appends a float value followed by a new line to the string builder using <code>String.valueOf</code>.
   *
   * @param value  the value to append
   * @return this, to enable chaining
   * @since 2.3
   */
  public StrBuilder appendln(float value) {
    return append(value).appendNewLine();
  }

  /**
   * Appends a double value followed by a new line to the string builder using <code>String.valueOf</code>.
   *
   * @param value  the value to append
   * @return this, to enable chaining
   * @since 2.3
   */
  public StrBuilder appendln(double value) {
    return append(value).appendNewLine();
  }

  //-----------------------------------------------------------------------
  /**
   * Appends each item in an array to the builder without any separators.
   * Appending a null array will have no effect.
   * Each object is appended using {@link #append(Object)}.
   *
   * @param array  the array to append
   * @return this, to enable chaining
   * @since 2.3
   */
  public StrBuilder appendAll(Object[] array) {
    if (array != null && array.length > 0) {
      for (int i = 0; i < array.length; i++) {
        append(array[i]);
      }
    }
    return this;
  }

  /**
   * Appends each item in a collection to the builder without any separators.
   * Appending a null collection will have no effect.
   * Each object is appended using {@link #append(Object)}.
   *
   * @param coll  the collection to append
   * @return this, to enable chaining
   * @since 2.3
   */
//  public StrBuilder appendAll(Collection coll) {
//    if (coll != null && coll.size() > 0) {
//      Iterator it = coll.iterator();
//      while (it.hasNext()) {
//        append(it.next());
//      }
//    }
//    return this;
//  }

  /**
   * Appends each item in an iterator to the builder without any separators.
   * Appending a null iterator will have no effect.
   * Each object is appended using {@link #append(Object)}.
   *
   * @param it  the iterator to append
   * @return this, to enable chaining
   * @since 2.3
   */
//  public StrBuilder appendAll(Iterator it) {
//    if (it != null) {
//      while (it.hasNext()) {
//        append(it.next());
//      }
//    }
//    return this;
//  }

  //-----------------------------------------------------------------------
  /**
   * Appends an array placing separators between each value, but
   * not before the first or after the last.
   * Appending a null array will have no effect.
   * Each object is appended using {@link #append(Object)}.
   *
   * @param array  the array to append
   * @param separator  the separator to use, null means no separator
   * @return this, to enable chaining
   */
  public StrBuilder appendWithSeparators(Object[] array, String separator) {
    if (array != null && array.length > 0) {
      separator = (separator == null ? "" : separator);
      append(array[0]);
      for (int i = 1; i < array.length; i++) {
        append(separator);
        append(array[i]);
      }
    }
    return this;
  }

  /**
   * Appends a collection placing separators between each value, but
   * not before the first or after the last.
   * Appending a null collection will have no effect.
   * Each object is appended using {@link #append(Object)}.
   *
   * @param coll  the collection to append
   * @param separator  the separator to use, null means no separator
   * @return this, to enable chaining
   */
//  public StrBuilder appendWithSeparators(Collection coll, String separator) {
//    if (coll != null && coll.size() > 0) {
//      separator = (separator == null ? "" : separator);
//      Iterator it = coll.iterator();
//      while (it.hasNext()) {
//        append(it.next());
//        if (it.hasNext()) {
//          append(separator);
//        }
//      }
//    }
//    return this;
//  }

  /**
   * Appends an iterator placing separators between each value, but
   * not before the first or after the last.
   * Appending a null iterator will have no effect.
   * Each object is appended using {@link #append(Object)}.
   *
   * @param it  the iterator to append
   * @param separator  the separator to use, null means no separator
   * @return this, to enable chaining
   */
//  public StrBuilder appendWithSeparators(Iterator it, String separator) {
//    if (it != null) {
//      separator = (separator == null ? "" : separator);
//      while (it.hasNext()) {
//        append(it.next());
//        if (it.hasNext()) {
//          append(separator);
//        }
//      }
//    }
//    return this;
//  }

  //-----------------------------------------------------------------------
  /**
   * Appends a separator if the builder is currently non-empty.
   * Appending a null separator will have no effect.
   * The separator is appended using {@link #append(String)}.
   * <p>
   * This method is useful for adding a separator each time around the
   * loop except the first.
   * <pre>
   * for (Iterator it = list.iterator(); it.hasNext(); ) {
   *   appendSeparator(",");
   *   append(it.next());
   * }
   * </pre>
   * Note that for this simple example, you should use
   * {@link #appendWithSeparators(Collection, String)}.
   * 
   * @param separator  the separator to use, null means no separator
   * @return this, to enable chaining
   * @since 2.3
   */
  public StrBuilder appendSeparator(String separator) {
    return appendSeparator(separator, null);
  }

  /**
   * Appends one of both separators to the StrBuilder.
   * If the builder is currently empty it will append the defaultIfEmpty-separator
   * Otherwise it will append the standard-separator
   * 
   * Appending a null separator will have no effect.
   * The separator is appended using {@link #append(String)}.
   * <p>
   * This method is for example useful for constructing queries
   * <pre>
   * StrBuilder whereClause = new StrBuilder();
   * if(searchCommand.getPriority() != null) {
   *  whereClause.appendSeparator(" and", " where");
   *  whereClause.append(" priority = ?")
   * }
   * if(searchCommand.getComponent() != null) {
   *  whereClause.appendSeparator(" and", " where");
   *  whereClause.append(" component = ?")
   * }
   * selectClause.append(whereClause)
   * </pre>
   * 
   * @param standard the separator if builder is not empty, null means no separator
   * @param defaultIfEmpty the separator if builder is empty, null means no separator
   * @return this, to enable chaining
   * @since 2.5
   */
  public StrBuilder appendSeparator(String standard, String defaultIfEmpty) {
    String str = isEmpty() ? defaultIfEmpty : standard;
    if (str != null) {
      append(str);
    }
    return this;
  }

  /**
   * Appends a separator if the builder is currently non-empty.
   * The separator is appended using {@link #append(char)}.
   * <p>
   * This method is useful for adding a separator each time around the
   * loop except the first.
   * <pre>
   * for (Iterator it = list.iterator(); it.hasNext(); ) {
   *   appendSeparator(',');
   *   append(it.next());
   * }
   * </pre>
   * Note that for this simple example, you should use
   * {@link #appendWithSeparators(Collection, String)}.
   * 
   * @param separator  the separator to use
   * @return this, to enable chaining
   * @since 2.3
   */
  public StrBuilder appendSeparator(char separator) {
    if (size() > 0) {
      append(separator);
    }
    return this;
  }

  /**
   * Append one of both separators to the builder
   * If the builder is currently empty it will append the defaultIfEmpty-separator
   * Otherwise it will append the standard-separator
   *
   * The separator is appended using {@link #append(char)}.
   * @param standard the separator if builder is not empty
   * @param defaultIfEmpty the separator if builder is empty
   * @return this, to enable chaining
   * @since 2.5
   */
  public StrBuilder appendSeparator(char standard, char defaultIfEmpty) {
    if (size() > 0) {
      append(standard);
    }
    else {
      append(defaultIfEmpty);
    }
    return this;
  }
  /**
   * Appends a separator to the builder if the loop index is greater than zero.
   * Appending a null separator will have no effect.
   * The separator is appended using {@link #append(String)}.
   * <p>
   * This method is useful for adding a separator each time around the
   * loop except the first.
   * <pre>
   * for (int i = 0; i < list.size(); i++) {
   *   appendSeparator(",", i);
   *   append(list.get(i));
   * }
   * </pre>
   * Note that for this simple example, you should use
   * {@link #appendWithSeparators(Collection, String)}.
   * 
   * @param separator  the separator to use, null means no separator
   * @param loopIndex  the loop index
   * @return this, to enable chaining
   * @since 2.3
   */
  public StrBuilder appendSeparator(String separator, int loopIndex) {
    if (separator != null && loopIndex > 0) {
      append(separator);
    }
    return this;
  }

  /**
   * Appends a separator to the builder if the loop index is greater than zero.
   * The separator is appended using {@link #append(char)}.
   * <p>
   * This method is useful for adding a separator each time around the
   * loop except the first.
   * <pre>
   * for (int i = 0; i < list.size(); i++) {
   *   appendSeparator(",", i);
   *   append(list.get(i));
   * }
   * </pre>
   * Note that for this simple example, you should use
   * {@link #appendWithSeparators(Collection, String)}.
   * 
   * @param separator  the separator to use
   * @param loopIndex  the loop index
   * @return this, to enable chaining
   * @since 2.3
   */
  public StrBuilder appendSeparator(char separator, int loopIndex) {
    if (loopIndex > 0) {
      append(separator);
    }
    return this;
  }

  //-----------------------------------------------------------------------
  /**
   * Appends the pad character to the builder the specified number of times.
   * 
   * @param length  the length to append, negative means no append
   * @param padChar  the character to append
   * @return this, to enable chaining
   */
  public StrBuilder appendPadding(int length, char padChar) {
    if (length >= 0) {
      ensureCapacity(size + length);
      for (int i = 0; i < length; i++) {
        buffer[size++] = padChar;
      }
    }
    return this;
  }

  //-----------------------------------------------------------------------
  /**
   * Appends an object to the builder padding on the left to a fixed width.
   * The <code>toString</code> of the object is used.
   * If the object is larger than the length, the left hand side is lost.
   * If the object is null, the null text value is used.
   * 
   * @param obj  the object to append, null uses null text
   * @param width  the fixed field width, zero or negative has no effect
   * @param padChar  the pad character to use
   * @return this, to enable chaining
   */
  public StrBuilder appendFixedWidthPadLeft(Object obj, int width, char padChar) {
    if (width > 0) {
      ensureCapacity(size + width);
      String str = (obj == null ? getNullText() : obj.toString());
      if (str == null) {
        str = "";
      }
      int strLen = str.length();
      if (strLen >= width) {
        str.getChars(strLen - width, strLen, buffer, size);
      } else {
        int padLen = width - strLen;
        for (int i = 0; i < padLen; i++) {
          buffer[size + i] = padChar;
        }
        str.getChars(0, strLen, buffer, size + padLen);
      }
      size += width;
    }
    return this;
  }

  /**
   * Appends an object to the builder padding on the left to a fixed width.
   * The <code>String.valueOf</code> of the <code>int</code> value is used.
   * If the formatted value is larger than the length, the left hand side is lost.
   * 
   * @param value  the value to append
   * @param width  the fixed field width, zero or negative has no effect
   * @param padChar  the pad character to use
   * @return this, to enable chaining
   */
  public StrBuilder appendFixedWidthPadLeft(int value, int width, char padChar) {
    return appendFixedWidthPadLeft(String.valueOf(value), width, padChar);
  }

  /**
   * Appends an object to the builder padding on the right to a fixed length.
   * The <code>toString</code> of the object is used.
   * If the object is larger than the length, the right hand side is lost.
   * If the object is null, null text value is used.
   * 
   * @param obj  the object to append, null uses null text
   * @param width  the fixed field width, zero or negative has no effect
   * @param padChar  the pad character to use
   * @return this, to enable chaining
   */
  public StrBuilder appendFixedWidthPadRight(Object obj, int width, char padChar) {
    if (width > 0) {
      ensureCapacity(size + width);
      String str = (obj == null ? getNullText() : obj.toString());
      if (str == null) {
        str = "";
      }
      int strLen = str.length();
      if (strLen >= width) {
        str.getChars(0, width, buffer, size);
      } else {
        int padLen = width - strLen;
        str.getChars(0, strLen, buffer, size);
        for (int i = 0; i < padLen; i++) {
          buffer[size + strLen + i] = padChar;
        }
      }
      size += width;
    }
    return this;
  }

  /**
   * Appends an object to the builder padding on the right to a fixed length.
   * The <code>String.valueOf</code> of the <code>int</code> value is used.
   * If the object is larger than the length, the right hand side is lost.
   * 
   * @param value  the value to append
   * @param width  the fixed field width, zero or negative has no effect
   * @param padChar  the pad character to use
   * @return this, to enable chaining
   */
  public StrBuilder appendFixedWidthPadRight(int value, int width, char padChar) {
    return appendFixedWidthPadRight(String.valueOf(value), width, padChar);
  }

  //-----------------------------------------------------------------------
  /**
   * Inserts the string representation of an object into this builder.
   * Inserting null will use the stored null text value.
   *
   * @param index  the index to add at, must be valid
   * @param obj  the object to insert
   * @return this, to enable chaining
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  public StrBuilder insert(int index, Object obj) {
    if (obj == null) {
      return insert(index, nullText);
    }
    return insert(index, obj.toString());
  }

  /**
   * Inserts the string into this builder.
   * Inserting null will use the stored null text value.
   *
   * @param index  the index to add at, must be valid
   * @param str  the string to insert
   * @return this, to enable chaining
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  public StrBuilder insert(int index, String str) {
    validateIndex(index);
    if (str == null) {
      str = nullText;
    }
    int strLen = (str == null ? 0 : str.length());
    if (strLen > 0) {
      int newSize = size + strLen;
      ensureCapacity(newSize);
      System.arraycopy(buffer, index, buffer, index + strLen, size - index);
      size = newSize;
      str.getChars(0, strLen, buffer, index); // str cannot be null here
    }
    return this;
  }

  /**
   * Inserts the character array into this builder.
   * Inserting null will use the stored null text value.
   *
   * @param index  the index to add at, must be valid
   * @param chars  the char array to insert
   * @return this, to enable chaining
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  public StrBuilder insert(int index, char chars[]) {
    validateIndex(index);
    if (chars == null) {
      return insert(index, nullText);
    }
    int len = chars.length;
    if (len > 0) {
      ensureCapacity(size + len);
      System.arraycopy(buffer, index, buffer, index + len, size - index);
      System.arraycopy(chars, 0, buffer, index, len);
      size += len;
    }
    return this;
  }

  /**
   * Inserts part of the character array into this builder.
   * Inserting null will use the stored null text value.
   *
   * @param index  the index to add at, must be valid
   * @param chars  the char array to insert
   * @param offset  the offset into the character array to start at, must be valid
   * @param length  the length of the character array part to copy, must be positive
   * @return this, to enable chaining
   * @throws IndexOutOfBoundsException if any index is invalid
   */
  public StrBuilder insert(int index, char chars[], int offset, int length) {
    validateIndex(index);
    if (chars == null) {
      return insert(index, nullText);
    }
    if (offset < 0 || offset > chars.length) {
      throw new StringIndexOutOfBoundsException("Invalid offset: " + offset);
    }
    if (length < 0 || offset + length > chars.length) {
      throw new StringIndexOutOfBoundsException("Invalid length: " + length);
    }
    if (length > 0) {
      ensureCapacity(size + length);
      System.arraycopy(buffer, index, buffer, index + length, size - index);
      System.arraycopy(chars, offset, buffer, index, length);
      size += length;
    }
    return this;
  }

  /**
   * Inserts the value into this builder.
   *
   * @param index  the index to add at, must be valid
   * @param value  the value to insert
   * @return this, to enable chaining
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  public StrBuilder insert(int index, boolean value) {
    validateIndex(index);
    if (value) {
      ensureCapacity(size + 4);
      System.arraycopy(buffer, index, buffer, index + 4, size - index);
      buffer[index++] = 't';
      buffer[index++] = 'r';
      buffer[index++] = 'u';
      buffer[index] = 'e';
      size += 4;
    } else {
      ensureCapacity(size + 5);
      System.arraycopy(buffer, index, buffer, index + 5, size - index);
      buffer[index++] = 'f';
      buffer[index++] = 'a';
      buffer[index++] = 'l';
      buffer[index++] = 's';
      buffer[index] = 'e';
      size += 5;
    }
    return this;
  }

  /**
   * Inserts the value into this builder.
   *
   * @param index  the index to add at, must be valid
   * @param value  the value to insert
   * @return this, to enable chaining
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  public StrBuilder insert(int index, char value) {
    validateIndex(index);
    ensureCapacity(size + 1);
    System.arraycopy(buffer, index, buffer, index + 1, size - index);
    buffer[index] = value;
    size++;
    return this;
  }

  /**
   * Inserts the value into this builder.
   *
   * @param index  the index to add at, must be valid
   * @param value  the value to insert
   * @return this, to enable chaining
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  public StrBuilder insert(int index, int value) {
    return insert(index, String.valueOf(value));
  }

  /**
   * Inserts the value into this builder.
   *
   * @param index  the index to add at, must be valid
   * @param value  the value to insert
   * @return this, to enable chaining
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  public StrBuilder insert(int index, long value) {
    return insert(index, String.valueOf(value));
  }

  /**
   * Inserts the value into this builder.
   *
   * @param index  the index to add at, must be valid
   * @param value  the value to insert
   * @return this, to enable chaining
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  public StrBuilder insert(int index, float value) {
    return insert(index, String.valueOf(value));
  }

  /**
   * Inserts the value into this builder.
   *
   * @param index  the index to add at, must be valid
   * @param value  the value to insert
   * @return this, to enable chaining
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  public StrBuilder insert(int index, double value) {
    return insert(index, String.valueOf(value));
  }

  //-----------------------------------------------------------------------
  /**
   * Internal method to delete a range without validation.
   *
   * @param startIndex  the start index, must be valid
   * @param endIndex  the end index (exclusive), must be valid
   * @param len  the length, must be valid
   * @throws IndexOutOfBoundsException if any index is invalid
   */
  private void deleteImpl(int startIndex, int endIndex, int len) {
    System.arraycopy(buffer, endIndex, buffer, startIndex, size - endIndex);
    size -= len;
  }

  /**
   * Deletes the characters between the two specified indices.
   *
   * @param startIndex  the start index, inclusive, must be valid
   * @param endIndex  the end index, exclusive, must be valid except
   *  that if too large it is treated as end of string
   * @return this, to enable chaining
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  public StrBuilder delete(int startIndex, int endIndex) {
    endIndex = validateRange(startIndex, endIndex);
    int len = endIndex - startIndex;
    if (len > 0) {
      deleteImpl(startIndex, endIndex, len);
    }
    return this;
  }

  //-----------------------------------------------------------------------
  /**
   * Deletes the character wherever it occurs in the builder.
   *
   * @param ch  the character to delete
   * @return this, to enable chaining
   */
  public StrBuilder deleteAll(char ch) {
    for (int i = 0; i < size; i++) {
      if (buffer[i] == ch) {
        int start = i;
        while (++i < size) {
          if (buffer[i] != ch) {
            break;
          }
        }
        int len = i - start;
        deleteImpl(start, i, len);
        i -= len;
      }
    }
    return this;
  }

  /**
   * Deletes the character wherever it occurs in the builder.
   *
   * @param ch  the character to delete
   * @return this, to enable chaining
   */
  public StrBuilder deleteFirst(char ch) {
    for (int i = 0; i < size; i++) {
      if (buffer[i] == ch) {
        deleteImpl(i, i + 1, 1);
        break;
      }
    }
    return this;
  }

  //-----------------------------------------------------------------------
  /**
   * Deletes the string wherever it occurs in the builder.
   *
   * @param str  the string to delete, null causes no action
   * @return this, to enable chaining
   */
  public StrBuilder deleteAll(String str) {
    int len = (str == null ? 0 : str.length());
    if (len > 0) {
      int index = indexOf(str, 0);
      while (index >= 0) {
        deleteImpl(index, index + len, len);
        index = indexOf(str, index);
      }
    }
    return this;
  }

  /**
   * Deletes the string wherever it occurs in the builder.
   *
   * @param str  the string to delete, null causes no action
   * @return this, to enable chaining
   */
  public StrBuilder deleteFirst(String str) {
    int len = (str == null ? 0 : str.length());
    if (len > 0) {
      int index = indexOf(str, 0);
      if (index >= 0) {
        deleteImpl(index, index + len, len);
      }
    }
    return this;
  }



  //-----------------------------------------------------------------------
  /**
   * Internal method to delete a range without validation.
   *
   * @param startIndex  the start index, must be valid
   * @param endIndex  the end index (exclusive), must be valid
   * @param removeLen  the length to remove (endIndex - startIndex), must be valid
   * @param insertStr  the string to replace with, null means delete range
   * @param insertLen  the length of the insert string, must be valid
   * @throws IndexOutOfBoundsException if any index is invalid
   */
  private void replaceImpl(int startIndex, int endIndex, int removeLen, String insertStr, int insertLen) {
    int newSize = size - removeLen + insertLen;
    if (insertLen != removeLen) {
      ensureCapacity(newSize);
      System.arraycopy(buffer, endIndex, buffer, startIndex + insertLen, size - endIndex);
      size = newSize;
    }
    if (insertLen > 0) {
      insertStr.getChars(0, insertLen, buffer, startIndex);
    }
  }

  /**
   * Replaces a portion of the string builder with another string.
   * The length of the inserted string does not have to match the removed length.
   *
   * @param startIndex  the start index, inclusive, must be valid
   * @param endIndex  the end index, exclusive, must be valid except
   *  that if too large it is treated as end of string
   * @param replaceStr  the string to replace with, null means delete range
   * @return this, to enable chaining
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  public StrBuilder replace(int startIndex, int endIndex, String replaceStr) {
    endIndex = validateRange(startIndex, endIndex);
    int insertLen = (replaceStr == null ? 0 : replaceStr.length());
    replaceImpl(startIndex, endIndex, endIndex - startIndex, replaceStr, insertLen);
    return this;
  }

  //-----------------------------------------------------------------------
  /**
   * Replaces the search character with the replace character
   * throughout the builder.
   *
   * @param search  the search character
   * @param replace  the replace character
   * @return this, to enable chaining
   */
  public StrBuilder replaceAll(char search, char replace) {
    if (search != replace) {
      for (int i = 0; i < size; i++) {
        if (buffer[i] == search) {
          buffer[i] = replace;
        }
      }
    }
    return this;
  }

  /**
   * Replaces the first instance of the search character with the
   * replace character in the builder.
   *
   * @param search  the search character
   * @param replace  the replace character
   * @return this, to enable chaining
   */
  public StrBuilder replaceFirst(char search, char replace) {
    if (search != replace) {
      for (int i = 0; i < size; i++) {
        if (buffer[i] == search) {
          buffer[i] = replace;
          break;
        }
      }
    }
    return this;
  }

  //-----------------------------------------------------------------------
  /**
   * Replaces the search string with the replace string throughout the builder.
   *
   * @param searchStr  the search string, null causes no action to occur
   * @param replaceStr  the replace string, null is equivalent to an empty string
   * @return this, to enable chaining
   */
  public StrBuilder replaceAll(String searchStr, String replaceStr) {
    int searchLen = (searchStr == null ? 0 : searchStr.length());
    if (searchLen > 0) {
      int replaceLen = (replaceStr == null ? 0 : replaceStr.length());
      int index = indexOf(searchStr, 0);
      while (index >= 0) {
        replaceImpl(index, index + searchLen, searchLen, replaceStr, replaceLen);
        index = indexOf(searchStr, index + replaceLen);
      }
    }
    return this;
  }

  /**
   * Replaces the first instance of the search string with the replace string.
   *
   * @param searchStr  the search string, null causes no action to occur
   * @param replaceStr  the replace string, null is equivalent to an empty string
   * @return this, to enable chaining
   */
  public StrBuilder replaceFirst(String searchStr, String replaceStr) {
    int searchLen = (searchStr == null ? 0 : searchStr.length());
    if (searchLen > 0) {
      int index = indexOf(searchStr, 0);
      if (index >= 0) {
        int replaceLen = (replaceStr == null ? 0 : replaceStr.length());
        replaceImpl(index, index + searchLen, searchLen, replaceStr, replaceLen);
      }
    }
    return this;
  }

  
  //-----------------------------------------------------------------------
  /**
   * Reverses the string builder placing each character in the opposite index.
   * 
   * @return this, to enable chaining
   */
  public StrBuilder reverse() {
    if (size == 0) {
      return this;
    }
        
    int half = size / 2;
    char[] buf = buffer;
    for (int leftIdx = 0, rightIdx = size - 1; leftIdx < half; leftIdx++,rightIdx--) {
      char swap = buf[leftIdx];
      buf[leftIdx] = buf[rightIdx];
      buf[rightIdx] = swap;
    }
    return this;
  }

  //-----------------------------------------------------------------------
  /**
   * Trims the builder by removing characters less than or equal to a space
   * from the beginning and end.
   *
   * @return this, to enable chaining
   */
  public StrBuilder trim() {
    if (size == 0) {
      return this;
    }
    int len = size;
    char[] buf = buffer;
    int pos = 0;
    while (pos < len && buf[pos] <= ' ') {
      pos++;
    }
    while (pos < len && buf[len - 1] <= ' ') {
      len--;
    }
    if (len < size) {
      delete(len, size);
    }
    if (pos > 0) {
      delete(0, pos);
    }
    return this;
  }

  //-----------------------------------------------------------------------
  /**
   * Checks whether this builder starts with the specified string.
   * <p>
   * Note that this method handles null input quietly, unlike String.
   * 
   * @param str  the string to search for, null returns false
   * @return true if the builder starts with the string
   */
  public boolean startsWith(String str) {
    if (str == null) {
      return false;
    }
    int len = str.length();
    if (len == 0) {
      return true;
    }
    if (len > size) {
      return false;
    }
    for (int i = 0; i < len; i++) {
      if (buffer[i] != str.charAt(i)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Checks whether this builder ends with the specified string.
   * <p>
   * Note that this method handles null input quietly, unlike String.
   * 
   * @param str  the string to search for, null returns false
   * @return true if the builder ends with the string
   */
  public boolean endsWith(String str) {
    if (str == null) {
      return false;
    }
    int len = str.length();
    if (len == 0) {
      return true;
    }
    if (len > size) {
      return false;
    }
    int pos = size - len;
    for (int i = 0; i < len; i++,pos++) {
      if (buffer[pos] != str.charAt(i)) {
        return false;
      }
    }
    return true;
  }

  //-----------------------------------------------------------------------
  /**
   * Extracts a portion of this string builder as a string.
   * 
   * @param start  the start index, inclusive, must be valid
   * @return the new string
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  public String substring(int start) {
    return substring(start, size);
  }

  /**
   * Extracts a portion of this string builder as a string.
   * <p>
   * Note: This method treats an endIndex greater than the length of the
   * builder as equal to the length of the builder, and continues
   * without error, unlike StringBuffer or String.
   * 
   * @param startIndex  the start index, inclusive, must be valid
   * @param endIndex  the end index, exclusive, must be valid except
   *  that if too large it is treated as end of string
   * @return the new string
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  public String substring(int startIndex, int endIndex) {
    endIndex = validateRange(startIndex, endIndex);
    return new String(buffer, startIndex, endIndex - startIndex);
  }

  /**
   * Extracts the leftmost characters from the string builder without
   * throwing an exception.
   * <p>
   * This method extracts the left <code>length</code> characters from
   * the builder. If this many characters are not available, the whole
   * builder is returned. Thus the returned string may be shorter than the
   * length requested.
   * 
   * @param length  the number of characters to extract, negative returns empty string
   * @return the new string
   */
  public String leftString(int length) {
    if (length <= 0) {
      return "";
    } else if (length >= size) {
      return new String(buffer, 0, size);
    } else {
      return new String(buffer, 0, length);
    }
  }

  /**
   * Extracts the rightmost characters from the string builder without
   * throwing an exception.
   * <p>
   * This method extracts the right <code>length</code> characters from
   * the builder. If this many characters are not available, the whole
   * builder is returned. Thus the returned string may be shorter than the
   * length requested.
   * 
   * @param length  the number of characters to extract, negative returns empty string
   * @return the new string
   */
  public String rightString(int length) {
    if (length <= 0) {
      return "";
    } else if (length >= size) {
      return new String(buffer, 0, size);
    } else {
      return new String(buffer, size - length, length);
    }
  }

  /**
   * Extracts some characters from the middle of the string builder without
   * throwing an exception.
   * <p>
   * This method extracts <code>length</code> characters from the builder
   * at the specified index.
   * If the index is negative it is treated as zero.
   * If the index is greater than the builder size, it is treated as the builder size.
   * If the length is negative, the empty string is returned.
   * If insufficient characters are available in the builder, as much as possible is returned.
   * Thus the returned string may be shorter than the length requested.
   * 
   * @param index  the index to start at, negative means zero
   * @param length  the number of characters to extract, negative returns empty string
   * @return the new string
   */
  public String midString(int index, int length) {
    if (index < 0) {
      index = 0;
    }
    if (length <= 0 || index >= size) {
      return "";
    }
    if (size <= index + length) {
      return new String(buffer, index, size - index);
    } else {
      return new String(buffer, index, length);
    }
  }

  //-----------------------------------------------------------------------
  /**
   * Checks if the string builder contains the specified char.
   *
   * @param ch  the character to find
   * @return true if the builder contains the character
   */
  public boolean contains(char ch) {
    char[] thisBuf = buffer;
    for (int i = 0; i < this.size; i++) {
      if (thisBuf[i] == ch) {
        return true;
      }
    }
    return false;
  }

  /**
   * Checks if the string builder contains the specified string.
   *
   * @param str  the string to find
   * @return true if the builder contains the string
   */
  public boolean contains(String str) {
    return indexOf(str, 0) >= 0;
  }

  //-----------------------------------------------------------------------
  /**
   * Searches the string builder to find the first reference to the specified char.
   * 
   * @param ch  the character to find
   * @return the first index of the character, or -1 if not found
   */
  public int indexOf(char ch) {
    return indexOf(ch, 0);
  }

  /**
   * Searches the string builder to find the first reference to the specified char.
   * 
   * @param ch  the character to find
   * @param startIndex  the index to start at, invalid index rounded to edge
   * @return the first index of the character, or -1 if not found
   */
  public int indexOf(char ch, int startIndex) {
    startIndex = (startIndex < 0 ? 0 : startIndex);
    if (startIndex >= size) {
      return -1;
    }
    char[] thisBuf = buffer;
    for (int i = startIndex; i < size; i++) {
      if (thisBuf[i] == ch) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Searches the string builder to find the first reference to the specified string.
   * <p>
   * Note that a null input string will return -1, whereas the JDK throws an exception.
   * 
   * @param str  the string to find, null returns -1
   * @return the first index of the string, or -1 if not found
   */
  public int indexOf(String str) {
    return indexOf(str, 0);
  }

  /**
   * Searches the string builder to find the first reference to the specified
   * string starting searching from the given index.
   * <p>
   * Note that a null input string will return -1, whereas the JDK throws an exception.
   * 
   * @param str  the string to find, null returns -1
   * @param startIndex  the index to start at, invalid index rounded to edge
   * @return the first index of the string, or -1 if not found
   */
  public int indexOf(String str, int startIndex) {
    startIndex = (startIndex < 0 ? 0 : startIndex);
    if (str == null || startIndex >= size) {
      return -1;
    }
    int strLen = str.length();
    if (strLen == 1) {
      return indexOf(str.charAt(0), startIndex);
    }
    if (strLen == 0) {
      return startIndex;
    }
    if (strLen > size) {
      return -1;
    }
    char[] thisBuf = buffer;
    int len = size - strLen + 1;
    outer:
    for (int i = startIndex; i < len; i++) {
      for (int j = 0; j < strLen; j++) {
        if (str.charAt(j) != thisBuf[i + j]) {
          continue outer;
        }
      }
      return i;
    }
    return -1;
  }

  //-----------------------------------------------------------------------
  /**
   * Searches the string builder to find the last reference to the specified char.
   * 
   * @param ch  the character to find
   * @return the last index of the character, or -1 if not found
   */
  public int lastIndexOf(char ch) {
    return lastIndexOf(ch, size - 1);
  }

  /**
   * Searches the string builder to find the last reference to the specified char.
   * 
   * @param ch  the character to find
   * @param startIndex  the index to start at, invalid index rounded to edge
   * @return the last index of the character, or -1 if not found
   */
  public int lastIndexOf(char ch, int startIndex) {
    startIndex = (startIndex >= size ? size - 1 : startIndex);
    if (startIndex < 0) {
      return -1;
    }
    for (int i = startIndex; i >= 0; i--) {
      if (buffer[i] == ch) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Searches the string builder to find the last reference to the specified string.
   * <p>
   * Note that a null input string will return -1, whereas the JDK throws an exception.
   * 
   * @param str  the string to find, null returns -1
   * @return the last index of the string, or -1 if not found
   */
  public int lastIndexOf(String str) {
    return lastIndexOf(str, size - 1);
  }

  /**
   * Searches the string builder to find the last reference to the specified
   * string starting searching from the given index.
   * <p>
   * Note that a null input string will return -1, whereas the JDK throws an exception.
   * 
   * @param str  the string to find, null returns -1
   * @param startIndex  the index to start at, invalid index rounded to edge
   * @return the last index of the string, or -1 if not found
   */
  public int lastIndexOf(String str, int startIndex) {
    startIndex = (startIndex >= size ? size - 1 : startIndex);
    if (str == null || startIndex < 0) {
      return -1;
    }
    int strLen = str.length();
    if (strLen > 0 && strLen <= size) {
      if (strLen == 1) {
        return lastIndexOf(str.charAt(0), startIndex);
      }

      outer:
      for (int i = startIndex - strLen + 1; i >= 0; i--) {
        for (int j = 0; j < strLen; j++) {
          if (str.charAt(j) != buffer[i + j]) {
            continue outer;
          }
        }
        return i;
      }
            
    } else if (strLen == 0) {
      return startIndex;
    }
    return -1;
  }


  //-----------------------------------------------------------------------
  /**
   * Gets the contents of this builder as a Reader.
   * <p>
   * This method allows the contents of the builder to be read
   * using any standard method that expects a Reader.
   * <p>
   * To use, simply create a <code>StrBuilder</code>, populate it with
   * data, call <code>asReader</code>, and then read away.
   * <p>
   * The internal character array is shared between the builder and the reader.
   * This allows you to append to the builder after creating the reader,
   * and the changes will be picked up.
   * Note however, that no synchronization occurs, so you must perform
   * all operations with the builder and the reader in one thread.
   * <p>
   * The returned reader supports marking, and ignores the flush method.
   *
   * @return a reader that reads from this builder
   */
  public Reader asReader() {
    return new StrBuilderReader();
  }

  //-----------------------------------------------------------------------
  /**
   * Gets this builder as a Writer that can be written to.
   * <p>
   * This method allows you to populate the contents of the builder
   * using any standard method that takes a Writer.
   * <p>
   * To use, simply create a <code>StrBuilder</code>,
   * call <code>asWriter</code>, and populate away. The data is available
   * at any time using the methods of the <code>StrBuilder</code>.
   * <p>
   * The internal character array is shared between the builder and the writer.
   * This allows you to intermix calls that append to the builder and
   * write using the writer and the changes will be occur correctly.
   * Note however, that no synchronization occurs, so you must perform
   * all operations with the builder and the writer in one thread.
   * <p>
   * The returned writer ignores the close and flush methods.
   *
   * @return a writer that populates this builder
   */
  public Writer asWriter() {
    return new StrBuilderWriter();
  }

  //-----------------------------------------------------------------------
//    /**
//     * Gets a String version of the string builder by calling the internal
//     * constructor of String by reflection.
//     * <p>
//     * WARNING: You must not use the StrBuilder after calling this method
//     * as the buffer is now shared with the String object. To ensure this,
//     * the internal character array is set to null, so you will get
//     * NullPointerExceptions on all method calls.
//     *
//     * @return the builder as a String
//     */
//    public String toSharedString() {
//        try {
//            Constructor con = String.class.getDeclaredConstructor(
//                new Class[] {int.class, int.class, char[].class});
//            con.setAccessible(true);
//            char[] buffer = buf;
//            buf = null;
//            size = -1;
//            nullText = null;
//            return (String) con.newInstance(
//                new Object[] {new Integer(0), new Integer(size), buffer});
//            
//        } catch (Exception ex) {
//            ex.printStackTrace();
//            throw new UnsupportedOperationException("StrBuilder.toSharedString is unsupported: " + ex.getMessage());
//        }
//    }

  //-----------------------------------------------------------------------
  /**
   * Checks the contents of this builder against another to see if they
   * contain the same character content ignoring case.
   *
   * @param other  the object to check, null returns false
   * @return true if the builders contain the same characters in the same order
   */
  public boolean equalsIgnoreCase(StrBuilder other) {
    if (this == other) {
      return true;
    }
    if (this.size != other.size) {
      return false;
    }
    char thisBuf[] = this.buffer;
    char otherBuf[] = other.buffer;
    for (int i = size - 1; i >= 0; i--) {
      char c1 = thisBuf[i];
      char c2 = otherBuf[i];
      if (c1 != c2 && Character.toUpperCase(c1) != Character.toUpperCase(c2)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Checks the contents of this builder against another to see if they
   * contain the same character content.
   *
   * @param other  the object to check, null returns false
   * @return true if the builders contain the same characters in the same order
   */
  public boolean equals(StrBuilder other) {
    if (this == other) {
      return true;
    }
    if (this.size != other.size) {
      return false;
    }
    char thisBuf[] = this.buffer;
    char otherBuf[] = other.buffer;
    for (int i = size - 1; i >= 0; i--) {
      if (thisBuf[i] != otherBuf[i]) {
        return false;
      }
    }
    return true;
  }

  /**
   * Checks the contents of this builder against another to see if they
   * contain the same character content.
   *
   * @param obj  the object to check, null returns false
   * @return true if the builders contain the same characters in the same order
   */
  public boolean equals(Object obj) {
    if (obj instanceof StrBuilder) {
      return equals((StrBuilder) obj);
    }
    return false;
  }

  /**
   * Gets a suitable hash code for this builder.
   *
   * @return a hash code
   */
  public int hashCode() {
    char buf[] = buffer;
    int hash = 0;
    for (int i = size - 1; i >= 0; i--) {
      hash = 31 * hash + buf[i];
    }
    return hash;
  }

  //-----------------------------------------------------------------------
  /**
   * Gets a String version of the string builder, creating a new instance
   * each time the method is called.
   * <p>
   * Note that unlike StringBuffer, the string version returned is
   * independent of the string builder.
   *
   * @return the builder as a String
   */
  public String toString() {
    return new String(buffer, 0, size);
  }

  /**
   * Gets a StringBuffer version of the string builder, creating a
   * new instance each time the method is called.
   *
   * @return the builder as a StringBuffer
   */
  public StringBuffer toStringBuffer() {
    return new StringBuffer(size).append(buffer, 0, size);
  }

  /**
   * Clone this object.
   *
   * @return a clone of this object
   * @throws CloneNotSupportedException if clone is not supported
   * @since 2.6
   */
  public Object clone() throws CloneNotSupportedException {
    StrBuilder clone = (StrBuilder)super.clone();
    clone.buffer = new char[buffer.length];
    System.arraycopy(buffer, 0, clone.buffer, 0, buffer.length);
    return clone;
  }

  //-----------------------------------------------------------------------
  /**
   * Validates parameters defining a range of the builder.
   * 
   * @param startIndex  the start index, inclusive, must be valid
   * @param endIndex  the end index, exclusive, must be valid except
   *  that if too large it is treated as end of string
   * @return the new string
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  protected int validateRange(int startIndex, int endIndex) {
    if (startIndex < 0) {
      throw new StringIndexOutOfBoundsException(startIndex);
    }
    if (endIndex > size) {
      endIndex = size;
    }
    if (startIndex > endIndex) {
      throw new StringIndexOutOfBoundsException("end < start");
    }
    return endIndex;
  }

  /**
   * Validates parameters defining a single index in the builder.
   * 
   * @param index  the index, must be valid
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  protected void validateIndex(int index) {
    if (index < 0 || index > size) {
      throw new StringIndexOutOfBoundsException(index);
    }
  }


  //-----------------------------------------------------------------------
  /**
   * Inner class to allow StrBuilder to operate as a writer.
   */
  class StrBuilderReader extends Reader {
    /** The current stream position. */
    private int pos;
    /** The last mark position. */
    private int mark;

    /**
     * Default constructor.
     */
    StrBuilderReader() {
      super();
    }

    /** {@inheritDoc} */
    public void close() {
      // do nothing
    }

    /** {@inheritDoc} */
    public int read() {
      if (ready() == false) {
        return -1;
      }
      return StrBuilder.this.charAt(pos++);
    }

    /** {@inheritDoc} */
    public int read(char b[], int off, int len) {
      if (off < 0 || len < 0 || off > b.length ||
          (off + len) > b.length || (off + len) < 0) {
        throw new IndexOutOfBoundsException();
      }
      if (len == 0) {
        return 0;
      }
      if (pos >= StrBuilder.this.size()) {
        return -1;
      }
      if (pos + len > size()) {
        len = StrBuilder.this.size() - pos;
      }
      StrBuilder.this.getChars(pos, pos + len, b, off);
      pos += len;
      return len;
    }

    /** {@inheritDoc} */
    public long skip(long n) {
      if (pos + n > StrBuilder.this.size()) {
        n = StrBuilder.this.size() - pos;
      }
      if (n < 0) {
        return 0;
      }
      pos += n;
      return n;
    }

    /** {@inheritDoc} */
    public boolean ready() {
      return pos < StrBuilder.this.size();
    }

    /** {@inheritDoc} */
    public boolean markSupported() {
      return true;
    }

    /** {@inheritDoc} */
    public void mark(int readAheadLimit) {
      mark = pos;
    }

    /** {@inheritDoc} */
    public void reset() {
      pos = mark;
    }
  }

  //-----------------------------------------------------------------------
  /**
   * Inner class to allow StrBuilder to operate as a writer.
   */
  class StrBuilderWriter extends Writer {

    /**
     * Default constructor.
     */
    StrBuilderWriter() {
      super();
    }

    /** {@inheritDoc} */
    public void close() {
      // do nothing
    }

    /** {@inheritDoc} */
    public void flush() {
      // do nothing
    }

    /** {@inheritDoc} */
    public void write(int c) {
      StrBuilder.this.append((char) c);
    }

    /** {@inheritDoc} */
    public void write(char[] cbuf) {
      StrBuilder.this.append(cbuf);
    }

    /** {@inheritDoc} */
    public void write(char[] cbuf, int off, int len) {
      StrBuilder.this.append(cbuf, off, len);
    }

    /** {@inheritDoc} */
    public void write(String str) {
      StrBuilder.this.append(str);
    }

    /** {@inheritDoc} */
    public void write(String str, int off, int len) {
      StrBuilder.this.append(str, off, len);
    }
  }
}
