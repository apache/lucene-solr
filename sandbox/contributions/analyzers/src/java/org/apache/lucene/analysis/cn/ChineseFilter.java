package org.apache.lucene.analysis.cn;

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