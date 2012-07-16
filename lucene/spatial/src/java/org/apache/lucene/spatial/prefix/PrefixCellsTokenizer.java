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

package org.apache.lucene.spatial.prefix;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.IOException;
import java.io.Reader;


/**
 * @lucene.internal
 */
class PrefixCellsTokenizer extends Tokenizer {
  public PrefixCellsTokenizer(Reader input) {
    super(input);
  }

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

  @Override
  public final boolean incrementToken() throws IOException {
    clearAttributes();
    int length = 0;
    char[] buffer = termAtt.buffer();
    while (true) {
      char c = (char) input.read();
      if (c < 0) break;
      if (c == 'a' || c == 'A') {
        buffer[length++] = 'A';
        continue;
      }
      if (c == 'b' || c == 'B') {
        buffer[length++] = 'B';
        continue;
      }
      if (c == 'c' || c == 'C') {
        buffer[length++] = 'C';
        continue;
      }
      if (c == 'd' || c == 'D') {
        buffer[length++] = 'D';
        continue;
      }
      if (c == '*') {
        buffer[length++] = '*';
        continue;
      }
      if (c == '+') {
        buffer[length++] = '+';
        continue;
      }

      if (length > 0) {
        // Skip any other character
        break;
      }
    }

    termAtt.setLength(length);
    return length > 0; // should only happen at the end
  }

  @Override
  public final void end() {

  }

  @Override
  public void setReader(Reader input) throws IOException {
    super.setReader(input);
  }
}