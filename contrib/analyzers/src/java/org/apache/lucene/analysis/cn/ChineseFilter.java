package org.apache.lucene.analysis.cn;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2004 The Apache Software Foundation.  All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution,
 *    if any, must include the following acknowledgment:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowledgment may appear in the software itself,
 *    if and wherever such third-party acknowledgments normally appear.
 *
 * 4. The names "Apache" and "Apache Software Foundation" and
 *    "Apache Lucene" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Lucene", nor may "Apache" appear in their name, without
 *    prior written permission of the Apache Software Foundation.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 */

import java.util.Hashtable;
import org.apache.lucene.analysis.*;

/**
 * Title: ChineseFilter
 * Description: Filter with a stop word table
 *              Rule: No digital is allowed.
 *                    English word/token should larger than 1 character.
 *                    One Chinese character as one Chinese word.
 * TO DO:
 *   1. Add Chinese stop words, such as \ue400
 *   2. Dictionary based Chinese word extraction
 *   3. Intelligent Chinese word extraction
 *
 * Copyright:    Copyright (c) 2001
 * Company:
 * @author Yiyi Sun
 * @version 1.0
 *
 */

public final class ChineseFilter extends TokenFilter {


    // Only English now, Chinese to be added later.
    public static final String[] STOP_WORDS = {
    "and", "are", "as", "at", "be", "but", "by",
    "for", "if", "in", "into", "is", "it",
    "no", "not", "of", "on", "or", "such",
    "that", "the", "their", "then", "there", "these",
    "they", "this", "to", "was", "will", "with"
    };


    private Hashtable stopTable;

    public ChineseFilter(TokenStream in) {
        super(in);

        stopTable = new Hashtable(STOP_WORDS.length);
        for (int i = 0; i < STOP_WORDS.length; i++)
            stopTable.put(STOP_WORDS[i], STOP_WORDS[i]);
    }

    public final Token next() throws java.io.IOException {

        for (Token token = input.next(); token != null; token = input.next()) {
            String text = token.termText();

          // why not key off token type here assuming ChineseTokenizer comes first?
            if (stopTable.get(text) == null) {
                switch (Character.getType(text.charAt(0))) {

                case Character.LOWERCASE_LETTER:
                case Character.UPPERCASE_LETTER:

                    // English word/token should larger than 1 character.
                    if (text.length()>1) {
                        return token;
                    }
                    break;
                case Character.OTHER_LETTER:

                    // One Chinese character as one Chinese word.
                    // Chinese word extraction to be added later here.

                    return token;
                }

            }

        }
        return null;
    }

}