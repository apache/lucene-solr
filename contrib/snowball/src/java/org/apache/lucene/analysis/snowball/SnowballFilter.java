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

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.tartarus.snowball.SnowballProgram;

/**
 * A filter that stems words using a Snowball-generated stemmer.
 *
 * Available stemmers are listed in {@link org.tartarus.snowball.ext}.
 */
public class SnowballFilter extends TokenFilter {

  private SnowballProgram stemmer;

  public SnowballFilter(TokenStream input, SnowballProgram stemmer) {
    super(input);
    this.stemmer = stemmer;
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
      Class stemClass = Class.forName("org.tartarus.snowball.ext." + name + "Stemmer");
      stemmer = (SnowballProgram) stemClass.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e.toString());
    }
  }

  /** Returns the next input Token, after being stemmed */
  public final Token next(final Token reusableToken) throws IOException {
    assert reusableToken != null;
    Token nextToken = input.next(reusableToken);
    if (nextToken == null)
      return null;
    String originalTerm = nextToken.term();
    stemmer.setCurrent(originalTerm);
    stemmer.stem();
    String finalTerm = stemmer.getCurrent();
    // Don't bother updating, if it is unchanged.
    if (!originalTerm.equals(finalTerm))
      nextToken.setTermBuffer(finalTerm);
    return nextToken;
  }
}
