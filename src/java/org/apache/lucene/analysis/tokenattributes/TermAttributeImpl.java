package org.apache.lucene.analysis.tokenattributes;

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

import java.io.Serializable;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.AttributeImpl;

/**
 * The term text of a Token.
 */
public class TermAttributeImpl extends AttributeImpl implements TermAttribute, Cloneable, Serializable {
  private static int MIN_BUFFER_SIZE = 10;
  
  private char[] termBuffer;
  private int termLength;
  
  /** Returns the Token's term text.
   * 
   * This method has a performance penalty
   * because the text is stored internally in a char[].  If
   * possible, use {@link #termBuffer()} and {@link
   * #termLength()} directly instead.  If you really need a
   * String, use this method, which is nothing more than
   * a convenience call to <b>new String(token.termBuffer(), 0, token.termLength())</b>
   */
  public String term() {
    initTermBuffer();
    return new String(termBuffer, 0, termLength);
  }

  /** Copies the contents of buffer, starting at offset for
   *  length characters, into the termBuffer array.
   *  @param buffer the buffer to copy
   *  @param offset the index in the buffer of the first character to copy
   *  @param length the number of characters to copy
   */
  public void setTermBuffer(char[] buffer, int offset, int length) {
    growTermBuffer(length);
    System.arraycopy(buffer, offset, termBuffer, 0, length);
    termLength = length;
  }

  /** Copies the contents of buffer into the termBuffer array.
   *  @param buffer the buffer to copy
   */
  public void setTermBuffer(String buffer) {
    int length = buffer.length();
    growTermBuffer(length);
    buffer.getChars(0, length, termBuffer, 0);
    termLength = length;
  }

  /** Copies the contents of buffer, starting at offset and continuing
   *  for length characters, into the termBuffer array.
   *  @param buffer the buffer to copy
   *  @param offset the index in the buffer of the first character to copy
   *  @param length the number of characters to copy
   */
  public void setTermBuffer(String buffer, int offset, int length) {
    assert offset <= buffer.length();
    assert offset + length <= buffer.length();
    growTermBuffer(length);
    buffer.getChars(offset, offset + length, termBuffer, 0);
    termLength = length;
  }

  /** Returns the internal termBuffer character array which
   *  you can then directly alter.  If the array is too
   *  small for your token, use {@link
   *  #resizeTermBuffer(int)} to increase it.  After
   *  altering the buffer be sure to call {@link
   *  #setTermLength} to record the number of valid
   *  characters that were placed into the termBuffer. */
  public char[] termBuffer() {
    initTermBuffer();
    return termBuffer;
  }

  /** Grows the termBuffer to at least size newSize, preserving the
   *  existing content. Note: If the next operation is to change
   *  the contents of the term buffer use
   *  {@link #setTermBuffer(char[], int, int)},
   *  {@link #setTermBuffer(String)}, or
   *  {@link #setTermBuffer(String, int, int)}
   *  to optimally combine the resize with the setting of the termBuffer.
   *  @param newSize minimum size of the new termBuffer
   *  @return newly created termBuffer with length >= newSize
   */
  public char[] resizeTermBuffer(int newSize) {
    if (termBuffer == null) {
      // The buffer is always at least MIN_BUFFER_SIZE
      termBuffer = new char[ArrayUtil.getNextSize(newSize < MIN_BUFFER_SIZE ? MIN_BUFFER_SIZE : newSize)]; 
    } else {
      if(termBuffer.length < newSize){
        // Not big enough; create a new array with slight
        // over allocation and preserve content
        final char[] newCharBuffer = new char[ArrayUtil.getNextSize(newSize)];
        System.arraycopy(termBuffer, 0, newCharBuffer, 0, termBuffer.length);
        termBuffer = newCharBuffer;
      }
    } 
    return termBuffer;   
  }


  /** Allocates a buffer char[] of at least newSize, without preserving the existing content.
   * its always used in places that set the content 
   *  @param newSize minimum size of the buffer
   */
  private void growTermBuffer(int newSize) {
    if (termBuffer == null) {
      // The buffer is always at least MIN_BUFFER_SIZE
      termBuffer = new char[ArrayUtil.getNextSize(newSize < MIN_BUFFER_SIZE ? MIN_BUFFER_SIZE : newSize)];   
    } else {
      if(termBuffer.length < newSize){
        // Not big enough; create a new array with slight
        // over allocation:
        termBuffer = new char[ArrayUtil.getNextSize(newSize)];
      }
    } 
  }
  
  private void initTermBuffer() {
    if (termBuffer == null) {
      termBuffer = new char[ArrayUtil.getNextSize(MIN_BUFFER_SIZE)];
      termLength = 0;
    }
  }

  /** Return number of valid characters (length of the term)
   *  in the termBuffer array. */
  public int termLength() {
    return termLength;
  }

  /** Set number of valid characters (length of the term) in
   *  the termBuffer array. Use this to truncate the termBuffer
   *  or to synchronize with external manipulation of the termBuffer.
   *  Note: to grow the size of the array,
   *  use {@link #resizeTermBuffer(int)} first.
   *  @param length the truncated length
   */
  public void setTermLength(int length) {
    initTermBuffer();
    if (length > termBuffer.length)
      throw new IllegalArgumentException("length " + length + " exceeds the size of the termBuffer (" + termBuffer.length + ")");
    termLength = length;
  }

  public int hashCode() {
    initTermBuffer();
    int code = termLength;
    code = code * 31 + ArrayUtil.hashCode(termBuffer, 0, termLength);
    return code;
  }

  public void clear() {
    termLength = 0;    
  }

  public Object clone() {
    TermAttributeImpl t = (TermAttributeImpl)super.clone();
    // Do a deep clone
    if (termBuffer != null) {
      t.termBuffer = (char[]) termBuffer.clone();
    }
    return t;
  }
  
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    
    if (other instanceof TermAttribute) {
      initTermBuffer();
      TermAttributeImpl o = ((TermAttributeImpl) other);
      o.initTermBuffer();
      
      if (termLength != o.termLength)
        return false;
      for(int i=0;i<termLength;i++) {
        if (termBuffer[i] != o.termBuffer[i]) {
          return false;
        }
      }
      return true;
    }
    
    return false;
  }

  public String toString() {
    initTermBuffer();
    return "term=" + new String(termBuffer, 0, termLength);
  }
  
  public void copyTo(AttributeImpl target) {
    initTermBuffer();
    TermAttribute t = (TermAttribute) target;
    t.setTermBuffer(termBuffer, 0, termLength);
  }
}
