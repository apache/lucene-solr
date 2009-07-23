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

package org.apache.lucene.analysis.cn.smart;

import java.io.IOException;
import java.io.Reader;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.Tokenizer;

/**
 * Tokenizes input text into sentences.
 * <p>
 * The output tokens can then be broken into words with {@link WordTokenFilter}
 * </p>
 */
public class SentenceTokenizer extends Tokenizer {

  /**
   * End of sentence punctuation: 。，！？；,!?;
   */
  private final static String PUNCTION = "。，！？；,!?;";

  private final StringBuffer buffer = new StringBuffer();

  private int tokenStart = 0, tokenEnd = 0;

  public SentenceTokenizer(Reader reader) {
    super(reader);
  }

  public Token next(final Token reusableToken) throws IOException {
    buffer.setLength(0);
    int ci;
    char ch, pch;
    boolean atBegin = true;
    tokenStart = tokenEnd;
    ci = input.read();
    ch = (char) ci;

    while (true) {
      if (ci == -1) {
        break;
      } else if (PUNCTION.indexOf(ch) != -1) {
        // End of a sentence
        buffer.append(ch);
        tokenEnd++;
        break;
      } else if (atBegin && Utility.SPACES.indexOf(ch) != -1) {
        tokenStart++;
        tokenEnd++;
        ci = input.read();
        ch = (char) ci;
      } else {
        buffer.append(ch);
        atBegin = false;
        tokenEnd++;
        pch = ch;
        ci = input.read();
        ch = (char) ci;
        // Two spaces, such as CR, LF
        if (Utility.SPACES.indexOf(ch) != -1
            && Utility.SPACES.indexOf(pch) != -1) {
          // buffer.append(ch);
          tokenEnd++;
          break;
        }
      }
    }
    if (buffer.length() == 0)
      return null;
    else {
      reusableToken.clear();
      reusableToken.reinit(buffer.toString(), input.correctOffset(tokenStart), input.correctOffset(tokenEnd), "sentence");
      return reusableToken;
    }
  }

}
