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
package org.apache.lucene.analysis.fa;

import java.io.IOException;
import java.io.Reader;
import org.apache.lucene.analysis.CharFilter;

/** CharFilter that replaces instances of Zero-width non-joiner with an ordinary space. */
public class PersianCharFilter extends CharFilter {

  public PersianCharFilter(Reader in) {
    super(in);
  }

  @Override
  public int read(char[] cbuf, int off, int len) throws IOException {
    final int charsRead = input.read(cbuf, off, len);
    if (charsRead > 0) {
      final int end = off + charsRead;
      while (off < end) {
        if (cbuf[off] == '\u200C') cbuf[off] = ' ';
        off++;
      }
    }
    return charsRead;
  }

  // optimized impl: some other charfilters consume with read()
  @Override
  public int read() throws IOException {
    int ch = input.read();
    if (ch == '\u200C') {
      return ' ';
    } else {
      return ch;
    }
  }

  @Override
  protected int correct(int currentOff) {
    return currentOff; // we don't change the length of the string
  }
}
