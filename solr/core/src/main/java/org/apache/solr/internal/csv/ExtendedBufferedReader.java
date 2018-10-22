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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;

/**
 * ExtendedBufferedReader
 *
 * A special reader decorater which supports more
 * sophisticated access to the underlying reader object.
 * 
 * In particular the reader supports a look-ahead option,
 * which allows you to see the next char returned by
 * next().
 * Furthermore the skip-method supports skipping until
 * (but excluding) a given char. Similar functionality
 * is supported by the reader as well.
 * 
 */
class ExtendedBufferedReader extends BufferedReader  {

  
  /** the end of stream symbol */
  public static final int END_OF_STREAM = -1;
  /** undefined state for the lookahead char */
  public static final int UNDEFINED = -2;
  
  /** the lookahead chars */
  private int lookaheadChar = UNDEFINED;
  /** the last char returned */
  private int lastChar = UNDEFINED;
  /** the line counter */
  private int lineCounter = 0;
  private CharBuffer line = new CharBuffer();
  
  /**
   * Created extended buffered reader using default buffer-size
   *
   */
  public ExtendedBufferedReader(Reader r) {
    super(r);
    /* note uh: do not fetch the first char here,
     *          because this might block the method!
     */
  }
    
  /**
   * Create extended buffered reader using the given buffer-size
   */
  public ExtendedBufferedReader(Reader r, int bufSize) {
    super(r, bufSize);
    /* note uh: do not fetch the first char here,
     *          because this might block the method!
     */
  }
  
  /**
   * Reads the next char from the input stream.
   * @return the next char or END_OF_STREAM if end of stream has been reached.
   */
  @Override
  public int read() throws IOException {
    // initialize the lookahead
    if (lookaheadChar == UNDEFINED) {
      lookaheadChar = super.read();
    }
    lastChar = lookaheadChar;
    if (super.ready()) {
      lookaheadChar = super.read();
    } else {
      lookaheadChar = UNDEFINED;
    }
    if (lastChar == '\n') {
      lineCounter++;
    } 
    return lastChar;
  }
  
  /**
   * Returns the last read character again.
   * 
   * @return the last read char or UNDEFINED
   */
  public int readAgain() {
    return lastChar;  
  }
  
  /**
   * Non-blocking reading of len chars into buffer buf starting
   * at bufferposition off.
   * 
   * performs an iterative read on the underlying stream
   * as long as the following conditions hold:
   *   - less than len chars have been read
   *   - end of stream has not been reached
   *   - next read is not blocking
   * 
   * @return nof chars actually read or END_OF_STREAM
   */
  @Override
  public int read(char[] buf, int off, int len) throws IOException {
    // do not claim if len == 0
    if (len == 0) {
      return 0;
    } 
    
    // init lookahead, but do not block !!
    if (lookaheadChar == UNDEFINED) {
        if (ready()) {
         lookaheadChar = super.read();
        } else {
          return -1;
        }
    }
    // 'first read of underlying stream'
    if (lookaheadChar == -1) {
      return -1;
    }
    // continue until the lookaheadChar would block
    int cOff = off;
    while (len > 0 && ready()) {
      if (lookaheadChar == -1) {
        // eof stream reached, do not continue
        return cOff - off;
      } else {
        buf[cOff++] = (char) lookaheadChar;
        if (lookaheadChar == '\n') {
          lineCounter++;
        } 
        lastChar = lookaheadChar;
        lookaheadChar = super.read();
        len--;
      }
    }
    return cOff - off;
  }
 
 /**
  * Reads all characters up to (but not including) the given character.
  * 
  * @param c the character to read up to
  * @return the string up to the character <code>c</code>
  * @throws IOException If there is a low-level I/O error.
  */
 public String readUntil(char c) throws IOException {
   if (lookaheadChar == UNDEFINED) {
     lookaheadChar = super.read();
   }
   line.clear(); // reuse
   while (lookaheadChar != c && lookaheadChar != END_OF_STREAM) {
     line.append((char) lookaheadChar);
     if (lookaheadChar == '\n') {
       lineCounter++;
     } 
     lastChar = lookaheadChar;
     lookaheadChar = super.read();
   }
   return line.toString();    
 }
 
 /**
  * @return A String containing the contents of the line, not 
  *         including any line-termination characters, or null 
  *         if the end of the stream has been reached
  */
  @Override
  public String readLine() throws IOException {
    
    if (lookaheadChar == UNDEFINED) {
      lookaheadChar = super.read(); 
    }
    
    line.clear(); //reuse
    
    // return null if end of stream has been reached
    if (lookaheadChar == END_OF_STREAM) {
      return null;
    }
    // do we have a line termination already
    char laChar = (char) lookaheadChar;
    if (laChar == '\n' || laChar == '\r') {
      lastChar = lookaheadChar;
      lookaheadChar = super.read();
      // ignore '\r\n' as well
      if ((char) lookaheadChar == '\n') {
        lastChar = lookaheadChar;
        lookaheadChar = super.read();
      }
      lineCounter++;
      return line.toString();
    }
    
    // create the rest-of-line return and update the lookahead
    line.append(laChar);
    String restOfLine = super.readLine(); // TODO involves copying
    lastChar = lookaheadChar;
    lookaheadChar = super.read();
    if (restOfLine != null) {
      line.append(restOfLine);
    }
    lineCounter++;
    return line.toString();
  }
  
  /**
   * Skips char in the stream
   * 
   * ATTENTION: invalidates the line-counter !!!!!
   * 
   * @return nof skiped chars
   */
  @Override
  public long skip(long n) throws IllegalArgumentException, IOException  {
    
    if (lookaheadChar == UNDEFINED) {
      lookaheadChar = super.read();   
    }
    
    // illegal argument
    if (n < 0) {
      throw new IllegalArgumentException("negative argument not supported");  
    }
    
    // no skipping
    if (n == 0 || lookaheadChar == END_OF_STREAM) {
      return 0;
    } 
    
    // skip and reread the lookahead-char
    long skiped = 0;
    if (n > 1) {
      skiped = super.skip(n - 1);
    }
    lookaheadChar = super.read();
    // fixme uh: we should check the skiped sequence for line-terminations...
    lineCounter = Integer.MIN_VALUE;
    return skiped + 1;
  }
  
  /**
   * Skips all chars in the input until (but excluding) the given char
   * 
   * @return counter
   * @throws IOException If there is a low-level I/O error.
   */
  public long skipUntil(char c) throws IllegalArgumentException, IOException {
    if (lookaheadChar == UNDEFINED) {
      lookaheadChar = super.read();   
    }
    long counter = 0;
    while (lookaheadChar != c && lookaheadChar != END_OF_STREAM) {
      if (lookaheadChar == '\n') {
        lineCounter++;
      } 
      lookaheadChar = super.read();
      counter++;
    }
    return counter;
  }
  
  /**
   * Returns the next char in the stream without consuming it.
   * 
   * Remember the next char read by read(..) will always be
   * identical to lookAhead().
   * 
   * @return the next char (without consuming it) or END_OF_STREAM
   */
  public int lookAhead() throws IOException {
    if (lookaheadChar == UNDEFINED) {
      lookaheadChar = super.read();
    }
    return lookaheadChar;
  }
  
  
  /**
   * Returns the nof line read
   * ATTENTION: the skip-method does invalidate the line-number counter
   * 
   * @return the current-line-number (or -1)
   */ 
  public int getLineNumber() {
    if (lineCounter > -1) {
      return lineCounter;
    } else {
      return -1;
    }
  }
  @Override
  public boolean markSupported() {
    /* note uh: marking is not supported, cause we cannot
     *          see into the future...
     */
    return false;
  }
  
}
