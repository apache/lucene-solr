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
 * RussianCharsets class contains encodings schemes (charsets) and toLowerCase() method implementation
 * for russian characters in Unicode, KOI8 and CP1252.
 * Each encoding scheme contains lowercase (positions 0-31) and uppercase (position 32-63) characters.
 * One should be able to add other encoding schemes (like ISO-8859-5 or customized) by adding a new charset
 * and adding logic to toLowerCase() method for that charset.
 *
 *
 * @version $Id$
 */
public class RussianCharsets
{
    // Unicode Russian charset (lowercase letters only)
    public static char[] UnicodeRussian = {
        '\u0430',
        '\u0431',
        '\u0432',
        '\u0433',
        '\u0434',
        '\u0435',
        '\u0436',
        '\u0437',
        '\u0438',
        '\u0439',
        '\u043A',
        '\u043B',
        '\u043C',
        '\u043D',
        '\u043E',
        '\u043F',
        '\u0440',
        '\u0441',
        '\u0442',
        '\u0443',
        '\u0444',
        '\u0445',
        '\u0446',
        '\u0447',
        '\u0448',
        '\u0449',
        '\u044A',
        '\u044B',
        '\u044C',
        '\u044D',
        '\u044E',
        '\u044F',
        // upper case
        '\u0410',
        '\u0411',
        '\u0412',
        '\u0413',
        '\u0414',
        '\u0415',
        '\u0416',
        '\u0417',
        '\u0418',
        '\u0419',
        '\u041A',
        '\u041B',
        '\u041C',
        '\u041D',
        '\u041E',
        '\u041F',
        '\u0420',
        '\u0421',
        '\u0422',
        '\u0423',
        '\u0424',
        '\u0425',
        '\u0426',
        '\u0427',
        '\u0428',
        '\u0429',
        '\u042A',
        '\u042B',
        '\u042C',
        '\u042D',
        '\u042E',
        '\u042F'
    };

    // KOI8 charset
    public static char[] KOI8 = {
        0xc1,
        0xc2,
        0xd7,
        0xc7,
        0xc4,
        0xc5,
        0xd6,
        0xda,
        0xc9,
        0xca,
        0xcb,
        0xcc,
        0xcd,
        0xce,
        0xcf,
        0xd0,
        0xd2,
        0xd3,
        0xd4,
        0xd5,
        0xc6,
        0xc8,
        0xc3,
        0xde,
        0xdb,
        0xdd,
        0xdf,
        0xd9,
        0xd8,
        0xdc,
        0xc0,
        0xd1,
        // upper case
        0xe1,
        0xe2,
        0xf7,
        0xe7,
        0xe4,
        0xe5,
        0xf6,
        0xfa,
        0xe9,
        0xea,
        0xeb,
        0xec,
        0xed,
        0xee,
        0xef,
        0xf0,
        0xf2,
        0xf3,
        0xf4,
        0xf5,
        0xe6,
        0xe8,
        0xe3,
        0xfe,
        0xfb,
        0xfd,
        0xff,
        0xf9,
        0xf8,
        0xfc,
        0xe0,
        0xf1
    };

    // CP1251 eharset
    public static char[] CP1251 = {
        0xE0,
        0xE1,
        0xE2,
        0xE3,
        0xE4,
        0xE5,
        0xE6,
        0xE7,
        0xE8,
        0xE9,
        0xEA,
        0xEB,
        0xEC,
        0xED,
        0xEE,
        0xEF,
        0xF0,
        0xF1,
        0xF2,
        0xF3,
        0xF4,
        0xF5,
        0xF6,
        0xF7,
        0xF8,
        0xF9,
        0xFA,
        0xFB,
        0xFC,
        0xFD,
        0xFE,
        0xFF,
        // upper case
        0xC0,
        0xC1,
        0xC2,
        0xC3,
        0xC4,
        0xC5,
        0xC6,
        0xC7,
        0xC8,
        0xC9,
        0xCA,
        0xCB,
        0xCC,
        0xCD,
        0xCE,
        0xCF,
        0xD0,
        0xD1,
        0xD2,
        0xD3,
        0xD4,
        0xD5,
        0xD6,
        0xD7,
        0xD8,
        0xD9,
        0xDA,
        0xDB,
        0xDC,
        0xDD,
        0xDE,
        0xDF
    };

    public static char toLowerCase(char letter, char[] charset)
    {
        if (charset == UnicodeRussian)
        {
            if (letter >= '\u0430' && letter <= '\u044F')
            {
                return letter;
            }
            if (letter >= '\u0410' && letter <= '\u042F')
            {
                return (char) (letter + 32);
            }
        }

        if (charset == KOI8)
        {
            if (letter >= 0xe0 && letter <= 0xff)
            {
                return (char) (letter - 32);
            }
            if (letter >= 0xc0 && letter <= 0xdf)
            {
                return letter;
            }

        }

        if (charset == CP1251)
        {
            if (letter >= 0xC0 && letter <= 0xDF)
            {
                return (char) (letter + 32);
            }
            if (letter >= 0xE0 && letter <= 0xFF)
            {
                return letter;
            }

        }

        return Character.toLowerCase(letter);
    }
}
