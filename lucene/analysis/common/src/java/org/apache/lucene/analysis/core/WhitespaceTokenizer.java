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
package org.apache.lucene.analysis.core;


import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.util.CharTokenizer;
import org.apache.lucene.util.AttributeFactory;

/**
 * A tokenizer that divides text at whitespace characters as defined by
 * {@link Character#isWhitespace(int)}.  Note: That definition explicitly excludes the non-breaking space.
 * Adjacent sequences of non-Whitespace characters form tokens.
 *
 * @see UnicodeWhitespaceTokenizer
 */
public final class WhitespaceTokenizer extends CharTokenizer {
  
  /**
   * Construct a new WhitespaceTokenizer.
   */
  public WhitespaceTokenizer() {
  }

  /**
   * Construct a new WhitespaceTokenizer using a given
   * {@link org.apache.lucene.util.AttributeFactory}.
   *
   * @param factory
   *          the attribute factory to use for this {@link Tokenizer}
   */
  public WhitespaceTokenizer(AttributeFactory factory) {
    super(factory);
  }
  
  /** Collects only characters which do not satisfy
   * {@link Character#isWhitespace(int)}.*/
  @Override
  protected boolean isTokenChar(int c) {
    return !Character.isWhitespace(c);
  }
}
