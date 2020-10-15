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

package org.apache.lucene.search.uhighlight;

import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.automaton.ByteRunAutomaton;

/**
 * Associates a label with a CharArrayMatcher to distinguish different sources for terms in highlighting
 *
 * @lucene.internal
 */
public interface LabelledCharArrayMatcher extends CharArrayMatcher {

  /**
   * @return the label for this matcher
   */
  String getLabel();

  /**
   * Associates a label with a CharArrayMatcher
   */
  static LabelledCharArrayMatcher wrap(String label, CharArrayMatcher in) {
    return new LabelledCharArrayMatcher() {
      @Override
      public String getLabel() {
        return label;
      }

      @Override
      public boolean match(char[] s, int offset, int length) {
        return in.match(s, offset, length);
      }
    };
  }

  /**
   * Returns a representation of the automaton that matches char[] instead of byte[]
   */
  static LabelledCharArrayMatcher wrap(String label, ByteRunAutomaton runAutomaton) {
      return wrap(label, (chars, offset, length) -> {
        int state = 0;
        final int maxIdx = offset + length;
        for (int i = offset; i < maxIdx; i++) {
          final int code = chars[i];
          int b;
          // UTF16 to UTF8   (inlined logic from UnicodeUtil.UTF16toUTF8 )
          if (code < 0x80) {
            state = runAutomaton.step(state, code);
            if (state == -1) return false;
          } else if (code < 0x800) {
            b = (0xC0 | (code >> 6));
            state = runAutomaton.step(state, b);
            if (state == -1) return false;
            b = (0x80 | (code & 0x3F));
            state = runAutomaton.step(state, b);
            if (state == -1) return false;
          } else {
            // more complex
            byte[] utf8Bytes = new byte[4 * (maxIdx - i)];
            int utf8Len = UnicodeUtil.UTF16toUTF8(chars, i, maxIdx - i, utf8Bytes);
            for (int utfIdx = 0; utfIdx < utf8Len; utfIdx++) {
              state = runAutomaton.step(state, utf8Bytes[utfIdx] & 0xFF);
              if (state == -1) return false;
            }
            break;
          }
        }
        return runAutomaton.isAccept(state);
      });
  }

}
