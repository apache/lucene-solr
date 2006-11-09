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

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import java.io.IOException;

/**
 * A filter that stems Russian words. The implementation was inspired by GermanStemFilter.
 * The input should be filtered by RussianLowerCaseFilter before passing it to RussianStemFilter ,
 * because RussianStemFilter only works  with lowercase part of any "russian" charset.
 *
 * @author    Boris Okner, b.okner@rogers.com
 * @version   $Id$
 */
public final class RussianStemFilter extends TokenFilter
{
    /**
     * The actual token in the input stream.
     */
    private Token token = null;
    private RussianStemmer stemmer = null;

    public RussianStemFilter(TokenStream in, char[] charset)
    {
        super(in);
        stemmer = new RussianStemmer(charset);
    }

    /**
     * @return  Returns the next token in the stream, or null at EOS
     */
    public final Token next() throws IOException
    {
        if ((token = input.next()) == null)
        {
            return null;
        }
        else
        {
            String s = stemmer.stem(token.termText());
            if (!s.equals(token.termText()))
            {
                return new Token(s, token.startOffset(), token.endOffset(),
                    token.type());
            }
            return token;
        }
    }

    /**
     * Set a alternative/custom RussianStemmer for this filter.
     */
    public void setStemmer(RussianStemmer stemmer)
    {
        if (stemmer != null)
        {
            this.stemmer = stemmer;
        }
    }
}
