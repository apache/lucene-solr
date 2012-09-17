package org.apache.lucene.analysis.fr;

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

import org.apache.lucene.analysis.miscellaneous.KeywordMarkerFilter; // for javadoc
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.apache.lucene.analysis.tokenattributes.KeywordAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.IOException;

/**
 * A {@link TokenFilter} that stems french words. 
 * <p>
 * The used stemmer can be changed at runtime after the
 * filter object is created (as long as it is a {@link FrenchStemmer}).
 * </p>
 * <p>
 * To prevent terms from being stemmed use an instance of
 * {@link KeywordMarkerFilter} or a custom {@link TokenFilter} that sets
 * the {@link KeywordAttribute} before this {@link TokenStream}.
 * </p>
 * @see KeywordMarkerFilter
 * @deprecated (3.1) Use {@link SnowballFilter} with 
 * {@link org.tartarus.snowball.ext.FrenchStemmer} instead, which has the
 * same functionality. This filter will be removed in Lucene 5.0
 */
@Deprecated
public final class FrenchStemFilter extends TokenFilter {

  /**
   * The actual token in the input stream.
   */
  private FrenchStemmer stemmer = new FrenchStemmer();
  
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final KeywordAttribute keywordAttr = addAttribute(KeywordAttribute.class);

  public FrenchStemFilter( TokenStream in ) {
    super(in);
  }

  /**
   * @return  Returns true for the next token in the stream, or false at EOS
   */
  @Override
  public boolean incrementToken() throws IOException {
    if (input.incrementToken()) {
      String term = termAtt.toString();

      // Check the exclusion table
      if (!keywordAttr.isKeyword()) {
        String s = stemmer.stem( term );
        // If not stemmed, don't waste the time  adjusting the token.
        if ((s != null) && !s.equals( term ) )
          termAtt.setEmpty().append(s);
      }
      return true;
    } else {
      return false;
    }
  }
  /**
   * Set a alternative/custom {@link FrenchStemmer} for this filter.
   */
  public void setStemmer( FrenchStemmer stemmer ) {
    if ( stemmer != null ) {
      this.stemmer = stemmer;
    }
  }
}


