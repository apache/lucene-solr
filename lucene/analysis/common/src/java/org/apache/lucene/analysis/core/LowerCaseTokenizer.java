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
import org.apache.lucene.util.AttributeFactory;

/**
 * LowerCaseTokenizer performs the function of LetterTokenizer
 * and LowerCaseFilter together.  It divides text at non-letters and converts
 * them to lower case.  While it is functionally equivalent to the combination
 * of LetterTokenizer and LowerCaseFilter, there is a performance advantage
 * to doing the two tasks at once, hence this (redundant) implementation.
 * <P>
 * Note: this does a decent job for most European languages, but does a terrible
 * job for some Asian languages, where words are not separated by spaces.
 * </p>
 */
public final class LowerCaseTokenizer extends LetterTokenizer {
  
  /**
   * Construct a new LowerCaseTokenizer.
   */
  public LowerCaseTokenizer() {
  }

  /**
   * Construct a new LowerCaseTokenizer using a given
   * {@link org.apache.lucene.util.AttributeFactory}.
   *
   * @param factory
   *          the attribute factory to use for this {@link Tokenizer}
   */
  public LowerCaseTokenizer(AttributeFactory factory) {
    super(factory);
  }
  
  /**
   * Construct a new LowerCaseTokenizer using a given
   * {@link org.apache.lucene.util.AttributeFactory}.
   *
   * @param factory the attribute factory to use for this {@link Tokenizer}
   * @param maxTokenLen maximum token length the tokenizer will emit. 
   *        Must be greater than 0 and less than MAX_TOKEN_LENGTH_LIMIT (1024*1024)
   * @throws IllegalArgumentException if maxTokenLen is invalid.
   */
  public LowerCaseTokenizer(AttributeFactory factory, int maxTokenLen) {
    super(factory, maxTokenLen);
  }
  
  /** Converts char to lower case
   * {@link Character#toLowerCase(int)}.*/
  @Override
  protected int normalize(int c) {
    return Character.toLowerCase(c);
  }
}
