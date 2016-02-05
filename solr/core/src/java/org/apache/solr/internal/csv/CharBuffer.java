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
package org.apache.solr.internal.csv;

/**
 * A simple StringBuffer replacement that aims to 
 * reduce copying as much as possible. The buffer
 * grows as necessary.
 * This class is not thread safe.
 */
public class CharBuffer {

    private char[] c;

    /**
     * Actually used number of characters in the array. 
     * It is also the index at which
     * a new character will be inserted into <code>c</code>. 
     */ 
    private int length;
    
    /**
     * Creates a new CharBuffer with an initial capacity of 32 characters.
     */
    public CharBuffer() {
        this(32);
    }
    
    /**
     * Creates a new CharBuffer with an initial capacity 
     * of <code>length</code> characters.
     */
    public CharBuffer(final int length) {
        if (length == 0) {
            throw new IllegalArgumentException("Can't create an empty CharBuffer");
        }
        this.c = new char[length];
    }
    
    /**
     * Empties the buffer. The capacity still remains the same, so no memory is freed.
     */
    public void clear() {
        length = 0;
    }
    
    /**
     * Returns the number of characters in the buffer.
     * @return the number of characters
     */
    public int length() {
        return length;
    }

    /**
     * Returns the current capacity of the buffer.
     * @return the maximum number of characters that can be stored in this buffer without
     * resizing it.
     */
    public int capacity() {
        return c.length;
    }

    
    /**
     * Appends the contents of <code>cb</code> to the end of this CharBuffer.
     * @param cb the CharBuffer to append or null
     */
    public void append(final CharBuffer cb) {
        if (cb == null) {
            return;
        }
        provideCapacity(length + cb.length);
        System.arraycopy(cb.c, 0, c, length, cb.length);
        length += cb.length;
    }
    
    /**
     * Appends <code>s</code> to the end of this CharBuffer.
     * This method involves copying the new data once!
     * @param s the String to append or null
     */
    public void append(final String s) {
        if (s == null) {
            return;
        }
        append(s.toCharArray());
    }
    
    /**
     * Appends <code>sb</code> to the end of this CharBuffer.
     * This method involves copying the new data once!
     * @param sb the StringBuffer to append or null
     */
    public void append(final StringBuffer sb) {
        if (sb == null) {
            return;
        }
        provideCapacity(length + sb.length());
        sb.getChars(0, sb.length(), c, length);
        length += sb.length();
    }
    
    /**
     * Appends <code>data</code> to the end of this CharBuffer.
     * This method involves copying the new data once!
     * @param data the char[] to append or null
     */
    public void append(final char[] data) {
        if (data == null) {
            return;
        }
        provideCapacity(length + data.length);
        System.arraycopy(data, 0, c, length, data.length);
        length += data.length;
    }
    
    /**
     * Appends a single character to the end of this CharBuffer.
     * This method involves copying the new data once!
     * @param data the char to append
     */
    public void append(final char data) {
        provideCapacity(length + 1);
        c[length] = data;
        length++;
    }
    
    /**
     * Shrinks the capacity of the buffer to the current length if necessary.
     * This method involves copying the data once!
     */
    public void shrink() {
        if (c.length == length) {
            return;
        }
        char[] newc = new char[length];
        System.arraycopy(c, 0, newc, 0, length);
        c = newc;
    }

   /**
    * Removes trailing whitespace.
    */
    public void trimTrailingWhitespace() {
      while (length>0 && Character.isWhitespace(c[length-1])) {
        length--;
      }
    }

    /**
     * Returns the contents of the buffer as a char[]. The returned array may
     * be the internal array of the buffer, so the caller must take care when
     * modifying it.
     * This method allows to avoid copying if the caller knows the exact capacity
     * before.
     */
    public char[] getCharacters() {
        if (c.length == length) {
            return c;
        }
        char[] chars = new char[length];
        System.arraycopy(c, 0, chars, 0, length);
        return chars;
    }

   /**
    * Returns the character at the specified position.
    */
    public char charAt(int pos) {
      return c[pos];
   }

    /**
     * Converts the contents of the buffer into a StringBuffer.
     * This method involves copying the new data once!
     */
    @Override
    public String toString() {
        return new String(c, 0, length);
    }
    
    /**
     * Copies the data into a new array of at least <code>capacity</code> size.
     */
    public void provideCapacity(final int capacity) {
        if (c.length >= capacity) {
            return;
        }
        int newcapacity = ((capacity*3)>>1) + 1;
        char[] newc = new char[newcapacity];
        System.arraycopy(c, 0, newc, 0, length);
        c = newc;
    }
}
