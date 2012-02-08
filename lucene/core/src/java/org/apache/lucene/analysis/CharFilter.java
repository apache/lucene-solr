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

/**
 * Subclasses of CharFilter can be chained to filter CharStream.
 * They can be used as {@link java.io.Reader} with additional offset
 * correction. {@link Tokenizer}s will automatically use {@link #correctOffset}
 * if a CharFilter/CharStream subclass is used.
 */
public abstract class CharFilter extends CharStream {

  protected CharStream input;

  protected CharFilter(CharStream in) {
    input = in;
  }

  /**
   * Subclass may want to override to correct the current offset.
   *
   * @param currentOff current offset
   * @return corrected offset
   */
  protected int correct(int currentOff) {
    return currentOff;
  }

  /**
   * Chains the corrected offset through the input
   * CharFilter.
   */
  @Override
  public final int correctOffset(int currentOff) {
    return input.correctOffset(correct(currentOff));
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
