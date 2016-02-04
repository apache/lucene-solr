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
import org.apache.lucene.analysis.util.UnicodeProps;
import org.apache.lucene.util.AttributeFactory;

/**
 * A UnicodeWhitespaceTokenizer is a tokenizer that divides text at whitespace.
 * Adjacent sequences of non-Whitespace characters form tokens (according to
 * Unicode's WHITESPACE property).
 * <p>
 * <em>For Unicode version see: {@link UnicodeProps}</em>
 */
public final class UnicodeWhitespaceTokenizer extends CharTokenizer {
  
  /**
   * Construct a new UnicodeWhitespaceTokenizer.
   */
  public UnicodeWhitespaceTokenizer() {
  }

  /**
   * Construct a new UnicodeWhitespaceTokenizer using a given
   * {@link org.apache.lucene.util.AttributeFactory}.
   *
   * @param factory
   *          the attribute factory to use for this {@link Tokenizer}
   */
  public UnicodeWhitespaceTokenizer(AttributeFactory factory) {
    super(factory);
  }
  
  /** Collects only characters which do not satisfy Unicode's WHITESPACE property. */
  @Override
  protected boolean isTokenChar(int c) {
    return !UnicodeProps.WHITESPACE.get(c);
  }
  
}
