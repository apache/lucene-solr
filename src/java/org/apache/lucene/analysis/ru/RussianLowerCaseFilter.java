package org.apache.lucene.analysis.ru;

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

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;

/**
 * Normalizes token text to lower case, analyzing given ("russian") charset.
 *
 * @author  Boris Okner, b.okner@rogers.com
 * @version $Id$
 */
public final class RussianLowerCaseFilter extends TokenFilter
{
    char[] charset;

    public RussianLowerCaseFilter(TokenStream in, char[] charset)
    {
        super(in);
        this.charset = charset;
    }

    public final Token next() throws java.io.IOException
    {
        Token t = input.next();

        if (t == null)
            return null;

        String txt = t.termText();

        char[] chArray = txt.toCharArray();
        for (int i = 0; i < chArray.length; i++)
        {
            chArray[i] = RussianCharsets.toLowerCase(chArray[i], charset);
        }

        String newTxt = new String(chArray);
        // create new token
        Token newToken = new Token(newTxt, t.startOffset(), t.endOffset());

        return newToken;
    }
}
