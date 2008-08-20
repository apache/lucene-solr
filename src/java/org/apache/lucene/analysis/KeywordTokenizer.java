package org.apache.lucene.analysis;

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

import java.io.IOException;
import java.io.Reader;

/**
 * Emits the entire input as a single token.
 */
public class KeywordTokenizer extends Tokenizer {
  
  private static final int DEFAULT_BUFFER_SIZE = 256;

  private boolean done;

  public KeywordTokenizer(Reader input) {
    this(input, DEFAULT_BUFFER_SIZE);
  }

  public KeywordTokenizer(Reader input, int bufferSize) {
    super(input);
    this.done = false;
  }

  public Token next(final Token reusableToken) throws IOException {
    assert reusableToken != null;
    if (!done) {
      done = true;
      int upto = 0;
      reusableToken.clear();
      char[] buffer = reusableToken.termBuffer();
      while (true) {
        final int length = input.read(buffer, upto, buffer.length-upto);
        if (length == -1) break;
        upto += length;
        if (upto == buffer.length)
          buffer = reusableToken.resizeTermBuffer(1+buffer.length);
      }
      reusableToken.setTermLength(upto);
      return reusableToken;
    }
    return null;
  }

  public void reset(Reader input) throws IOException {
    super.reset(input);
    this.done = false;
  }
}
