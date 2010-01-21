package org.apache.lucene.analysis.snowball;

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

import java.io.IOException;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.analysis.tr.TurkishLowerCaseFilter; // javadoc @link
import org.apache.lucene.analysis.LowerCaseFilter; // javadoc @link
import org.tartarus.snowball.SnowballProgram;

/**
 * A filter that stems words using a Snowball-generated stemmer.
 *
 * Available stemmers are listed in {@link org.tartarus.snowball.ext}.
 * <p><b>NOTE</b>: SnowballFilter expects lowercased text.
 * <ul>
 *  <li>For the Turkish language, see {@link TurkishLowerCaseFilter}.
 *  <li>For other languages, see {@link LowerCaseFilter}.
 * </ul>
 * </p>
 */
public final class SnowballFilter extends TokenFilter {

  private SnowballProgram stemmer;

  private TermAttribute termAtt;
  
  public SnowballFilter(TokenStream input, SnowballProgram stemmer) {
    super(input);
    this.stemmer = stemmer;
    termAtt = addAttribute(TermAttribute.class);
  }

  /**
   * Construct the named stemming filter.
   *
   * Available stemmers are listed in {@link org.tartarus.snowball.ext}.
   * The name of a stemmer is the part of the class name before "Stemmer",
   * e.g., the stemmer in {@link org.tartarus.snowball.ext.EnglishStemmer} is named "English".
   *
   * @param in the input tokens to stem
   * @param name the name of a stemmer
   */
  public SnowballFilter(TokenStream in, String name) {
    super(in);
    try {      
      Class<?> stemClass = Class.forName("org.tartarus.snowball.ext." + name + "Stemmer");
      stemmer = (SnowballProgram) stemClass.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e.toString());
    }
    termAtt = addAttribute(TermAttribute.class);
  }

  /** Returns the next input Token, after being stemmed */
  @Override
  public final boolean incrementToken() throws IOException {
    if (input.incrementToken()) {
      char termBuffer[] = termAtt.termBuffer();
      final int length = termAtt.termLength();
      stemmer.setCurrent(termBuffer, length);
      stemmer.stem();
      final char finalTerm[] = stemmer.getCurrentBuffer();
      final int newLength = stemmer.getCurrentBufferLength();
      if (finalTerm != termBuffer)
        termAtt.setTermBuffer(finalTerm, 0, newLength);
      else
        termAtt.setTermLength(newLength); 
      return true;
    } else {
      return false;
    }
  }
}
