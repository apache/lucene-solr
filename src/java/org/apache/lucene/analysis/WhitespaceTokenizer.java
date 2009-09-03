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

import java.io.Reader;

import org.apache.lucene.util.AttributeSource;

/** A WhitespaceTokenizer is a tokenizer that divides text at whitespace.
 * Adjacent sequences of non-Whitespace characters form tokens. */

public class WhitespaceTokenizer extends CharTokenizer {
  /** Construct a new WhitespaceTokenizer. */
  public WhitespaceTokenizer(Reader in) {
    super(in);
  }

  /** Construct a new WhitespaceTokenizer using a given {@link AttributeSource}. */
  public WhitespaceTokenizer(AttributeSource source, Reader in) {
    super(source, in);
  }

  /** Construct a new WhitespaceTokenizer using a given {@link org.apache.lucene.util.AttributeSource.AttributeFactory}. */
  public WhitespaceTokenizer(AttributeFactory factory, Reader in) {
    super(factory, in);
  }
  
  /** Collects only characters which do not satisfy
   * {@link Character#isWhitespace(char)}.*/
  protected boolean isTokenChar(char c) {
    return !Character.isWhitespace(c);
  }
}
