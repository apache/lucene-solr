package org.apache.lucene.analysis.el;

/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

/**
 * Normalizes token text to lower case, analyzing given ("greek") charset.
 *
 */
public final class GreekLowerCaseFilter extends TokenFilter
{
    /**
     * @deprecated Support for non-Unicode encodings will be removed in Lucene 3.0
     */
    char[] charset;

    private TermAttribute termAtt;
    
    /**
     * @deprecated Use {@link #GreekLowerCaseFilter(TokenStream)} instead.
     */
    public GreekLowerCaseFilter(TokenStream in, char[] charset)
    {
        super(in);
        this.charset = charset;
        termAtt = (TermAttribute) addAttribute(TermAttribute.class);
    }
    
    public GreekLowerCaseFilter(TokenStream in)
    {
    	this(in, GreekCharsets.UnicodeGreek);
    }

    public boolean incrementToken() throws IOException {
      if (input.incrementToken()) {
        char[] chArray = termAtt.termBuffer();
        int chLen = termAtt.termLength();
        for (int i = 0; i < chLen; i++)
        {
          chArray[i] = GreekCharsets.toLowerCase(chArray[i], charset);
        }
        return true;
      } else {
        return false;
      }
    }
}
