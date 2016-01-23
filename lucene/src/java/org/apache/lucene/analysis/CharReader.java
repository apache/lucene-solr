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

package org.apache.lucene.analysis;

import java.io.IOException;
import java.io.Reader;

/**
 * CharReader is a Reader wrapper. It reads chars from
 * Reader and outputs {@link CharStream}, defining an
 * identify function {@link #correctOffset} method that
 * simply returns the provided offset.
 */
public final class CharReader extends CharStream {

  private final Reader input;
  
  public static CharStream get(Reader input) {
    return input instanceof CharStream ?
      (CharStream)input : new CharReader(input);
  }

  private CharReader(Reader in) {
    input = in;
  }

  @Override
  public int correctOffset(int currentOff) {
    return currentOff;
  }

  @Override
  public void close() throws IOException {
    input.close();
  }

  @Override
  public int read(char[] cbuf, int off, int len) throws IOException {
    return input.read(cbuf, off, len);
  }

  @Override
  public boolean markSupported(){
    return input.markSupported();
  }

  @Override
  public void mark( int readAheadLimit ) throws IOException {
    input.mark(readAheadLimit);
  }

  @Override
  public void reset() throws IOException {
    input.reset();
  }
}
