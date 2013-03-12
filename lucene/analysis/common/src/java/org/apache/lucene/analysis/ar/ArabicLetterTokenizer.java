package org.apache.lucene.analysis.ar;
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

import java.io.Reader;

import org.apache.lucene.analysis.core.LetterTokenizer;
import org.apache.lucene.analysis.util.CharTokenizer;
import org.apache.lucene.analysis.standard.StandardTokenizer; // javadoc @link
import org.apache.lucene.util.Version;

/**
 * Tokenizer that breaks text into runs of letters and diacritics.
 * <p>
 * The problem with the standard Letter tokenizer is that it fails on diacritics.
 * Handling similar to this is necessary for Indic Scripts, Hebrew, Thaana, etc.
 * </p>
 * <p>
 * <a name="version"/>
 * You must specify the required {@link Version} compatibility when creating
 * {@link ArabicLetterTokenizer}:
 * <ul>
 * <li>As of 3.1, {@link CharTokenizer} uses an int based API to normalize and
 * detect token characters. See {@link #isTokenChar(int)} and
 * {@link #normalize(int)} for details.</li>
 * </ul>
 * @deprecated (3.1) Use {@link StandardTokenizer} instead.
 */
@Deprecated
public class ArabicLetterTokenizer extends LetterTokenizer {
  /**
   * Construct a new ArabicLetterTokenizer.
   * @param matchVersion Lucene version
   * to match See {@link <a href="#version">above</a>}
   * 
   * @param in
   *          the input to split up into tokens
   */
  public ArabicLetterTokenizer(Version matchVersion, Reader in) {
    super(matchVersion, in);
  }

  /**
   * Construct a new ArabicLetterTokenizer using a given
   * {@link org.apache.lucene.util.AttributeSource.AttributeFactory}. * @param
   * matchVersion Lucene version to match See
   * {@link <a href="#version">above</a>}
   * 
   * @param factory
   *          the attribute factory to use for this Tokenizer
   * @param in
   *          the input to split up into tokens
   */
  public ArabicLetterTokenizer(Version matchVersion, AttributeFactory factory, Reader in) {
    super(matchVersion, factory, in);
  }
  
  /**
   * Allows for Letter category or NonspacingMark category
   * @see org.apache.lucene.analysis.core.LetterTokenizer#isTokenChar(int)
   */
  @Override
  protected boolean isTokenChar(int c) {
    return super.isTokenChar(c) || Character.getType(c) == Character.NON_SPACING_MARK;
  }

}
