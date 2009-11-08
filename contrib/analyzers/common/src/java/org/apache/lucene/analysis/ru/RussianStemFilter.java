package org.apache.lucene.analysis.ru;

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

import org.apache.lucene.analysis.LowerCaseFilter; // for javadoc
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.analysis.ru.RussianStemmer;//javadoc @link

import java.io.IOException;

/**
 * A {@link TokenFilter} that stems Russian words. 
 * <p>
 * The implementation was inspired by GermanStemFilter.
 * The input should be filtered by {@link LowerCaseFilter} before passing it to RussianStemFilter ,
 * because RussianStemFilter only works with lowercase characters.
 * </p>
 */
public final class RussianStemFilter extends TokenFilter
{
    /**
     * The actual token in the input stream.
     */
    private RussianStemmer stemmer = null;

    private TermAttribute termAtt;

    public RussianStemFilter(TokenStream in)
    {
        super(in);
        stemmer = new RussianStemmer();
        termAtt = addAttribute(TermAttribute.class);
    }
    /**
     * Returns the next token in the stream, or null at EOS
     */
    @Override
    public final boolean incrementToken() throws IOException
    {
      if (input.incrementToken()) {
        String term = termAtt.term();
        String s = stemmer.stem(term);
        if (s != null && !s.equals(term))
          termAtt.setTermBuffer(s);
        return true;
      } else {
        return false;
      }
    }


    /**
     * Set a alternative/custom {@link RussianStemmer} for this filter.
     */
    public void setStemmer(RussianStemmer stemmer)
    {
        if (stemmer != null)
        {
            this.stemmer = stemmer;
        }
    }
}
