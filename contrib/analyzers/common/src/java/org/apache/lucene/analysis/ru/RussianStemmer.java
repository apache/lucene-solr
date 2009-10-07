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

/**
 * Russian stemming algorithm implementation (see http://snowball.sourceforge.net for detailed description).
 */
class RussianStemmer
{
    // positions of RV, R1 and R2 respectively
    private int RV, R1, R2;

    // letters (currently unused letters are commented out)
    private final static char A = '\u0430';
    //private final static char B = '\u0431';
    private final static char V = '\u0432';
    private final static char G = '\u0433';
    //private final static char D = '\u0434';
    private final static char E = '\u0435';
    //private final static char ZH = '\u0436';
    //private final static char Z = '\u0437';
    private final static char I = '\u0438';
    private final static char I_ = '\u0439';
    //private final static char K = '\u043A';
    private final static char L = '\u043B';
    private final static char M = '\u043C';
    private final static char N = '\u043D';
    private final static char O = '\u043E';
    //private final static char P = '\u043F';
    //private final static char R = '\u0440';
    private final static char S = '\u0441';
    private final static char T = '\u0442';
    private final static char U = '\u0443';
    //private final static char F = '\u0444';
    private final static char X = '\u0445';
    //private final static char TS = '\u0446';
    //private final static char CH = '\u0447';
    private final static char SH = '\u0448';
    private final static char SHCH = '\u0449';
    //private final static char HARD = '\u044A';
    private final static char Y = '\u044B';
    private final static char SOFT = '\u044C';
    private final static char AE = '\u044D';
    private final static char IU = '\u044E';
    private final static char IA = '\u044F';

    // stem definitions
    private static char[] vowels = { A, E, I, O, U, Y, AE, IU, IA };

    private static char[][] perfectiveGerundEndings1 = {
        { V },
        { V, SH, I },
        { V, SH, I, S, SOFT }
    };

    private static char[][] perfectiveGerund1Predessors = {
        { A },
        { IA }
    };

    private static char[][] perfectiveGerundEndings2 = { { I, V }, {
        Y, V }, {
            I, V, SH, I }, {
                Y, V, SH, I }, {
                    I, V, SH, I, S, SOFT }, {
                        Y, V, SH, I, S, SOFT }
    };

    private static char[][] adjectiveEndings = {
        { E, E },
        { I, E },
        { Y, E },
        { O, E },
        { E, I_ },
        { I, I_ },
        { Y, I_ },
        { O, I_ },
        { E, M },
        { I, M },
        { Y, M },
        { O, M },
        { I, X },
        { Y, X },
        { U, IU },
        { IU, IU },
        { A, IA },
        { IA, IA },
        { O, IU },
        { E, IU },
        { I, M, I },
        { Y, M, I },
        { E, G, O },
        { O, G, O },
        { E, M, U },
        {O, M, U }
    };

    private static char[][] participleEndings1 = {
        { SHCH },
        { E, M },
        { N, N },
        { V, SH },
        { IU, SHCH }
    };

    private static char[][] participleEndings2 = {
        { I, V, SH },
        { Y, V, SH },
        { U, IU, SHCH }
    };

    private static char[][] participle1Predessors = {
        { A },
        { IA }
    };

    private static char[][] reflexiveEndings = {
        { S, IA },
        { S, SOFT }
    };

    private static char[][] verbEndings1 = {
        { I_ },
        { L },
        { N },
        { L, O },
        { N, O },
        { E, T },
        { IU, T },
        { L, A },
        { N, A },
        { L, I },
        { E, M },
        { N, Y },
        { E, T, E },
        { I_, T, E },
        { T, SOFT },
        { E, SH, SOFT },
        { N, N, O }
    };

    private static char[][] verbEndings2 = {
        { IU },
        { U, IU },
        { E, N },
        { E, I_ },
        { IA, T },
        { U, I_ },
        { I, L },
        { Y, L },
        { I, M },
        { Y, M },
        { I, T },
        { Y, T },
        { I, L, A },
        { Y, L, A },
        { E, N, A },
        { I, T, E },
        { I, L, I },
        { Y, L, I },
        { I, L, O },
        { Y, L, O },
        { E, N, O },
        { U, E, T },
        { U, IU, T },
        { E, N, Y },
        { I, T, SOFT },
        { Y, T, SOFT },
        { I, SH, SOFT },
        { E, I_, T, E },
        { U, I_, T, E }
    };

    private static char[][] verb1Predessors = {
        { A },
        { IA }
    };

    private static char[][] nounEndings = {
        { A },
        { U },
        { I_ },
        { O },
        { U },
        { E },
        { Y },
        { I },
        { SOFT },
        { IA },
        { E, V },
        { O, V },
        { I, E },
        { SOFT, E },
        { IA, X },
        { I, IU },
        { E, I },
        { I, I },
        { E, I_ },
        { O, I_ },
        { E, M },
        { A, M },
        { O, M },
        { A, X },
        { SOFT, IU },
        { I, IA },
        { SOFT, IA },
        { I, I_ },
        { IA, M },
        { IA, M, I },
        { A, M, I },
        { I, E, I_ },
        { I, IA, M },
        { I, E, M },
        { I, IA, X },
        { I, IA, M, I }
    };

    private static char[][] superlativeEndings = {
        { E, I_, SH },
        { E, I_, SH, E }
    };

    private static char[][] derivationalEndings = {
        { O, S, T },
        { O, S, T, SOFT }
    };

    /**
     * RussianStemmer constructor comment.
     */
    public RussianStemmer()
    {
        super();
    }

    /**
     * Adjectival ending is an adjective ending,
     * optionally preceded by participle ending.
     * Creation date: (17/03/2002 12:14:58 AM)
     * @param stemmingZone java.lang.StringBuilder
     */
    private boolean adjectival(StringBuilder stemmingZone)
    {
        // look for adjective ending in a stemming zone
        if (!findAndRemoveEnding(stemmingZone, adjectiveEndings))
            return false;
        // if adjective ending was found, try for participle ending.
        // variable r is unused, we are just interested in the side effect of
        // findAndRemoveEnding():
        boolean r =
            findAndRemoveEnding(stemmingZone, participleEndings1, participle1Predessors)
            ||
            findAndRemoveEnding(stemmingZone, participleEndings2);
        return true;
    }

    /**
     * Derivational endings
     * Creation date: (17/03/2002 12:14:58 AM)
     * @param stemmingZone java.lang.StringBuilder
     */
    private boolean derivational(StringBuilder stemmingZone)
    {
        int endingLength = findEnding(stemmingZone, derivationalEndings);
        if (endingLength == 0)
             // no derivational ending found
            return false;
        else
        {
            // Ensure that the ending locates in R2
            if (R2 - RV <= stemmingZone.length() - endingLength)
            {
                stemmingZone.setLength(stemmingZone.length() - endingLength);
                return true;
            }
            else
            {
                return false;
            }
        }
    }

    /**
     * Finds ending among given ending class and returns the length of ending found(0, if not found).
     * Creation date: (17/03/2002 8:18:34 PM)
     */
    private int findEnding(StringBuilder stemmingZone, int startIndex, char[][] theEndingClass)
    {
        boolean match = false;
        for (int i = theEndingClass.length - 1; i >= 0; i--)
        {
            char[] theEnding = theEndingClass[i];
            // check if the ending is bigger than stemming zone
            if (startIndex < theEnding.length - 1)
            {
                match = false;
                continue;
            }
            match = true;
            int stemmingIndex = startIndex;
            for (int j = theEnding.length - 1; j >= 0; j--)
            {
                if (stemmingZone.charAt(stemmingIndex--) != theEnding[j])
                {
                    match = false;
                    break;
                }
            }
            // check if ending was found
            if (match)
            {
                return theEndingClass[i].length; // cut ending
            }
        }
        return 0;
    }

    private int findEnding(StringBuilder stemmingZone, char[][] theEndingClass)
    {
        return findEnding(stemmingZone, stemmingZone.length() - 1, theEndingClass);
    }

    /**
     * Finds the ending among the given class of endings and removes it from stemming zone.
     * Creation date: (17/03/2002 8:18:34 PM)
     */
    private boolean findAndRemoveEnding(StringBuilder stemmingZone, char[][] theEndingClass)
    {
        int endingLength = findEnding(stemmingZone, theEndingClass);
        if (endingLength == 0)
            // not found
            return false;
        else {
            stemmingZone.setLength(stemmingZone.length() - endingLength);
            // cut the ending found
            return true;
        }
    }

    /**
     * Finds the ending among the given class of endings, then checks if this ending was
     * preceded by any of given predecessors, and if so, removes it from stemming zone.
     * Creation date: (17/03/2002 8:18:34 PM)
     */
    private boolean findAndRemoveEnding(StringBuilder stemmingZone,
        char[][] theEndingClass, char[][] thePredessors)
    {
        int endingLength = findEnding(stemmingZone, theEndingClass);
        if (endingLength == 0)
            // not found
            return false;
        else
        {
            int predessorLength =
                findEnding(stemmingZone,
                    stemmingZone.length() - endingLength - 1,
                    thePredessors);
            if (predessorLength == 0)
                return false;
            else {
                stemmingZone.setLength(stemmingZone.length() - endingLength);
                // cut the ending found
                return true;
            }
        }

    }

    /**
     * Marks positions of RV, R1 and R2 in a given word.
     * Creation date: (16/03/2002 3:40:11 PM)
     */
    private void markPositions(String word)
    {
        RV = 0;
        R1 = 0;
        R2 = 0;
        int i = 0;
        // find RV
        while (word.length() > i && !isVowel(word.charAt(i)))
        {
            i++;
        }
        if (word.length() - 1 < ++i)
            return; // RV zone is empty
        RV = i;
        // find R1
        while (word.length() > i && isVowel(word.charAt(i)))
        {
            i++;
        }
        if (word.length() - 1 < ++i)
            return; // R1 zone is empty
        R1 = i;
        // find R2
        while (word.length() > i && !isVowel(word.charAt(i)))
        {
            i++;
        }
        if (word.length() - 1 < ++i)
            return; // R2 zone is empty
        while (word.length() > i && isVowel(word.charAt(i)))
        {
            i++;
        }
        if (word.length() - 1 < ++i)
            return; // R2 zone is empty
        R2 = i;
    }

    /**
     * Checks if character is a vowel..
     * Creation date: (16/03/2002 10:47:03 PM)
     * @return boolean
     * @param letter char
     */
    private boolean isVowel(char letter)
    {
        for (int i = 0; i < vowels.length; i++)
        {
            if (letter == vowels[i])
                return true;
        }
        return false;
    }

    /**
     * Noun endings.
     * Creation date: (17/03/2002 12:14:58 AM)
     * @param stemmingZone java.lang.StringBuilder
     */
    private boolean noun(StringBuilder stemmingZone)
    {
        return findAndRemoveEnding(stemmingZone, nounEndings);
    }

    /**
     * Perfective gerund endings.
     * Creation date: (17/03/2002 12:14:58 AM)
     * @param stemmingZone java.lang.StringBuilder
     */
    private boolean perfectiveGerund(StringBuilder stemmingZone)
    {
        return findAndRemoveEnding(
            stemmingZone,
            perfectiveGerundEndings1,
            perfectiveGerund1Predessors)
            || findAndRemoveEnding(stemmingZone, perfectiveGerundEndings2);
    }

    /**
     * Reflexive endings.
     * Creation date: (17/03/2002 12:14:58 AM)
     * @param stemmingZone java.lang.StringBuilder
     */
    private boolean reflexive(StringBuilder stemmingZone)
    {
        return findAndRemoveEnding(stemmingZone, reflexiveEndings);
    }

    /**
     * Insert the method's description here.
     * Creation date: (17/03/2002 12:14:58 AM)
     * @param stemmingZone java.lang.StringBuilder
     */
    private boolean removeI(StringBuilder stemmingZone)
    {
        if (stemmingZone.length() > 0
            && stemmingZone.charAt(stemmingZone.length() - 1) == I)
        {
            stemmingZone.setLength(stemmingZone.length() - 1);
            return true;
        }
        else
        {
            return false;
        }
    }

    /**
     * Insert the method's description here.
     * Creation date: (17/03/2002 12:14:58 AM)
     * @param stemmingZone java.lang.StringBuilder
     */
    private boolean removeSoft(StringBuilder stemmingZone)
    {
        if (stemmingZone.length() > 0
            && stemmingZone.charAt(stemmingZone.length() - 1) == SOFT)
        {
            stemmingZone.setLength(stemmingZone.length() - 1);
            return true;
        }
        else
        {
            return false;
        }
    }

    /**
     * Finds the stem for given Russian word.
     * Creation date: (16/03/2002 3:36:48 PM)
     * @return java.lang.String
     * @param input java.lang.String
     */
    public String stem(String input)
    {
        markPositions(input);
        if (RV == 0)
            return input; //RV wasn't detected, nothing to stem
        StringBuilder stemmingZone = new StringBuilder(input.substring(RV));
        // stemming goes on in RV
        // Step 1

        if (!perfectiveGerund(stemmingZone))
        {
            reflexive(stemmingZone);
            // variable r is unused, we are just interested in the flow that gets
            // created by logical expression: apply adjectival(); if that fails,
            // apply verb() etc
            boolean r =
                adjectival(stemmingZone)
                || verb(stemmingZone)
                || noun(stemmingZone);
        }
        // Step 2
        removeI(stemmingZone);
        // Step 3
        derivational(stemmingZone);
        // Step 4
        superlative(stemmingZone);
        undoubleN(stemmingZone);
        removeSoft(stemmingZone);
        // return result
        return input.substring(0, RV) + stemmingZone.toString();
    }

    /**
     * Superlative endings.
     * Creation date: (17/03/2002 12:14:58 AM)
     * @param stemmingZone java.lang.StringBuilder
     */
    private boolean superlative(StringBuilder stemmingZone)
    {
        return findAndRemoveEnding(stemmingZone, superlativeEndings);
    }

    /**
     * Undoubles N.
     * Creation date: (17/03/2002 12:14:58 AM)
     * @param stemmingZone java.lang.StringBuilder
     */
    private boolean undoubleN(StringBuilder stemmingZone)
    {
        char[][] doubleN = {
            { N, N }
        };
        if (findEnding(stemmingZone, doubleN) != 0)
        {
            stemmingZone.setLength(stemmingZone.length() - 1);
            return true;
        }
        else
        {
            return false;
        }
    }

    /**
     * Verb endings.
     * Creation date: (17/03/2002 12:14:58 AM)
     * @param stemmingZone java.lang.StringBuilder
     */
    private boolean verb(StringBuilder stemmingZone)
    {
        return findAndRemoveEnding(
            stemmingZone,
            verbEndings1,
            verb1Predessors)
            || findAndRemoveEnding(stemmingZone, verbEndings2);
    }
   
    /**
     * Static method for stemming.
     */
    public static String stemWord(String theWord)
    {
        RussianStemmer stemmer = new RussianStemmer();
        return stemmer.stem(theWord);
    }
}
