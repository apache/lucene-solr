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
package org.apache.solr.parser;

import java.io.*;

/** An efficient implementation of JavaCC's CharStream interface.  <p>Note that
 * this does not do line-number counting, but instead keeps track of the
 * character position of the token in the input, as required by Lucene's {@link
 * org.apache.lucene.analysis.tokenattributes.OffsetAttribute} API.
 * */
public final class FastCharStream implements CharStream {
  char[] buffer = null;

  int bufferLength = 0;          // end of valid chars
  int bufferPosition = 0;        // next char to read

  int tokenStart = 0;          // offset in buffer
  int bufferStart = 0;          // position in file of buffer

  Reader input;            // source of chars

  /** Constructs from a Reader. */
  public FastCharStream(Reader r) {
    input = r;
  }

  @Override
  public final char readChar() throws IOException {
    if (bufferPosition >= bufferLength)
      refill();
    return buffer[bufferPosition++];
  }

  private final void refill() throws IOException {
    int newPosition = bufferLength - tokenStart;

    if (tokenStart == 0) {        // token won't fit in buffer
      if (buffer == null) {        // first time: alloc buffer
  buffer = new char[2048];
      } else if (bufferLength == buffer.length) { // grow buffer
  char[] newBuffer = new char[buffer.length*2];
  System.arraycopy(buffer, 0, newBuffer, 0, bufferLength);
  buffer = newBuffer;
      }
    } else {            // shift token to front
      System.arraycopy(buffer, tokenStart, buffer, 0, newPosition);
    }

    bufferLength = newPosition;        // update state
    bufferPosition = newPosition;
    bufferStart += tokenStart;
    tokenStart = 0;

    int charsRead =          // fill space in buffer
      input.read(buffer, newPosition, buffer.length-newPosition);
    if (charsRead == -1)
      throw READ_PAST_EOF;
    else
      bufferLength += charsRead;
  }

  @Override
  public final char BeginToken() throws IOException {
    tokenStart = bufferPosition;
    return readChar();
  }

  /**
   * This Exception is used as a signal rather than an exceptional state.
   */
  private static final IOException READ_PAST_EOF = new IOException("read past eof");

  @Override
  public final void backup(int amount) {
    bufferPosition -= amount;
  }

  @Override
  public final String GetImage() {
    return new String(buffer, tokenStart, bufferPosition - tokenStart);
  }

  @Override
  public final char[] GetSuffix(int len) {
    char[] value = new char[len];
    System.arraycopy(buffer, bufferPosition - len, value, 0, len);
    return value;
  }

  @Override
  public final void Done() {
    try {
      input.close();
    } catch (IOException e) {
    }
  }

  @Override
  public final int getEndColumn() {
    return bufferStart + bufferPosition;
  }
  @Override
  public final int getEndLine() {
    return 1;
  }
  @Override
  public final int getBeginColumn() {
    return bufferStart + tokenStart;
  }
  @Override
  public final int getBeginLine() {
    return 1;
  }

  @Override
  @Deprecated
  /**
   * Returns the column position of the character last read.
   * @deprecated
   * @see #getEndColumn
   */
  public int getColumn() {
    return 1;
  }

  @Override
  @Deprecated
  /**
   * Returns the line number of the character last read.
   * @deprecated
   * @see #getEndLine
   */
  public int getLine() {
    return 1;
  }
}
