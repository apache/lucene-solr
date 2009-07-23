package org.apache.lucene.analysis.cn;

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

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;

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


    private Map stopTable;

    public ChineseFilter(TokenStream in) {
        super(in);

        stopTable = new HashMap(STOP_WORDS.length);
        for (int i = 0; i < STOP_WORDS.length; i++)
            stopTable.put(STOP_WORDS[i], STOP_WORDS[i]);
    }

    public final Token next(final Token reusableToken) throws java.io.IOException {
        assert reusableToken != null;

        for (Token nextToken = input.next(reusableToken); nextToken != null; nextToken = input.next(reusableToken)) {
            String text = nextToken.term();

          // why not key off token type here assuming ChineseTokenizer comes first?
            if (stopTable.get(text) == null) {
                switch (Character.getType(text.charAt(0))) {

                case Character.LOWERCASE_LETTER:
                case Character.UPPERCASE_LETTER:

                    // English word/token should larger than 1 character.
                    if (text.length()>1) {
                        return nextToken;
                    }
                    break;
                case Character.OTHER_LETTER:

                    // One Chinese character as one Chinese word.
                    // Chinese word extraction to be added later here.

                    return nextToken;
                }

            }

        }
        return null;
    }

}