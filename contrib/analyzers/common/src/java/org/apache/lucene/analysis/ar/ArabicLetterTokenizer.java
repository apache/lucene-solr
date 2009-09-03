package org.apache.lucene.analysis.ar;
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

import org.apache.lucene.analysis.LetterTokenizer;
import org.apache.lucene.util.AttributeSource;

/**
 * Tokenizer that breaks text into runs of letters and diacritics.
 * <p>
 * The problem with the standard Letter tokenizer is that it fails on diacritics.
 * Handling similar to this is necessary for Indic Scripts, Hebrew, Thaana, etc.
 * </p>
 *
 */
public class ArabicLetterTokenizer extends LetterTokenizer {

  public ArabicLetterTokenizer(Reader in) {
    super(in);
  }

  public ArabicLetterTokenizer(AttributeSource source, Reader in) {
    super(source, in);
  }

  public ArabicLetterTokenizer(AttributeFactory factory, Reader in) {
    super(factory, in);
  }
  
  /** 
   * Allows for Letter category or NonspacingMark category
   * @see org.apache.lucene.analysis.LetterTokenizer#isTokenChar(char)
   */
  protected boolean isTokenChar(char c) {
    return super.isTokenChar(c) || Character.getType(c) == Character.NON_SPACING_MARK;
  }

}
