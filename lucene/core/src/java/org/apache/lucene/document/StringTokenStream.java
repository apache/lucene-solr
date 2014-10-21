package org.apache.lucene.document;

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

import java.io.IOException;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;

final class StringTokenStream extends TokenStream {
  private final CharTermAttribute termAttribute = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAttribute = addAttribute(OffsetAttribute.class);
  private boolean used = false;
  private String value = null;
    
  /** Creates a new TokenStream that returns a String as single token.
   * <p>Warning: Does not initialize the value, you must call
   * {@link #setValue(String)} afterwards!
   */
  StringTokenStream() {
  }
    
  /** Sets the string value. */
  void setValue(String value) {
    this.value = value;
  }

  @Override
  public boolean incrementToken() {
    if (used) {
      return false;
    }
    clearAttributes();
    termAttribute.append(value);
    offsetAttribute.setOffset(0, value.length());
    used = true;
    return true;
  }

  @Override
  public void end() throws IOException {
    super.end();
    final int finalOffset = value.length();
    offsetAttribute.setOffset(finalOffset, finalOffset);
  }
    
  @Override
  public void reset() {
    used = false;
  }

  @Override
  public void close() {
    value = null;
  }
}

