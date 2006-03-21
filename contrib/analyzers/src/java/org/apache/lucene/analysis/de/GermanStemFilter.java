package org.apache.lucene.analysis.de;

/**
 * Copyright 2004 The Apache Software Foundation
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

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;

import java.io.IOException;
import java.util.Set;

/**
 * A filter that stems German words. It supports a table of words that should
 * not be stemmed at all. The stemmer used can be changed at runtime after the
 * filter object is created (as long as it is a GermanStemmer).
 *
 * @author    Gerhard Schwarz
 * @version   $Id$
 */
public final class GermanStemFilter extends TokenFilter
{
    /**
     * The actual token in the input stream.
     */
    private Token token = null;
    private GermanStemmer stemmer = null;
    private Set exclusionSet = null;

    public GermanStemFilter( TokenStream in )
    {
      super(in);
      stemmer = new GermanStemmer();
    }

    /**
     * Builds a GermanStemFilter that uses an exclusiontable.
     */
    public GermanStemFilter( TokenStream in, Set exclusionSet )
    {
      this( in );
      this.exclusionSet = exclusionSet;
    }

    /**
     * @return  Returns the next token in the stream, or null at EOS
     */
    public final Token next()
      throws IOException
    {
      if ( ( token = input.next() ) == null ) {
        return null;
      }
      // Check the exclusiontable
      else if ( exclusionSet != null && exclusionSet.contains( token.termText() ) ) {
        return token;
      }
      else {
        String s = stemmer.stem( token.termText() );
        // If not stemmed, dont waste the time creating a new token
        if ( !s.equals( token.termText() ) ) {
          return new Token( s, token.startOffset(),
            token.endOffset(), token.type() );
        }
        return token;
      }
    }

    /**
     * Set a alternative/custom GermanStemmer for this filter.
     */
    public void setStemmer( GermanStemmer stemmer )
    {
      if ( stemmer != null ) {
        this.stemmer = stemmer;
      }
    }


    /**
     * Set an alternative exclusion list for this filter.
     */
    public void setExclusionSet( Set exclusionSet )
    {
      this.exclusionSet = exclusionSet;
    }
}
