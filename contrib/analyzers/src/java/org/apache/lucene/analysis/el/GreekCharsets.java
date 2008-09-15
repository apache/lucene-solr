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

/**
 * GreekCharsets class contains encodings schemes (charsets) and toLowerCase() method implementation
 * for greek characters in Unicode, ISO-8859-7 and Microsoft Windows CP1253.
 * Each encoding scheme contains lowercase (positions 0-35) and uppercase (position 36-68) characters,
 * including accented ones. One should be able to add other encoding schemes (see RFC 1947) by adding
 * the definition of a new charset as well as the required logic in the toLowerCase() method.
 *
 */
public class GreekCharsets
{
    // Unicode Greek charset
    public static char[] UnicodeGreek = {
    	// lower case
        '\u0390',
        '\u03AC',
        '\u03AD',
        '\u03AE',
        '\u03AF',
        '\u03B0',
        '\u03B1',
        '\u03B2',
        '\u03B3',
        '\u03B4',
        '\u03B5',
        '\u03B6',
        '\u03B7',
        '\u03B8',
        '\u03B9',
        '\u03BA',
        '\u03BB',
        '\u03BC',
        '\u03BD',
        '\u03BE',
        '\u03BF',
        '\u03C0',
        '\u03C1',
        '\u03C2',
        '\u03C3',
        '\u03C4',
        '\u03C5',
        '\u03C6',
        '\u03C7',
        '\u03C8',
        '\u03C9',
        '\u03CA',
        '\u03CB',
        '\u03CC',
        '\u03CD',
        '\u03CE',
        // upper case
        '\u0386',
        '\u0388',
        '\u0389',
        '\u038A',
        '\u038C',
        '\u038E',
        '\u038F',
        '\u0391',
        '\u0392',
        '\u0393',
        '\u0394',
        '\u0395',
        '\u0396',
        '\u0397',
        '\u0398',
        '\u0399',
        '\u039A',
        '\u039B',
        '\u039C',
        '\u039D',
        '\u039E',
        '\u039F',
        '\u03A0',
        '\u03A1',
        '\u03A3',
        '\u03A4',
        '\u03A5',
        '\u03A6',
        '\u03A7',
        '\u03A8',
        '\u03A9',
        '\u03AA',
        '\u03AB'
    };

    // ISO-8859-7 charset (ELOT-928)
    public static char[] ISO = {
       	// lower case
        0xc0,
        0xdc,
        0xdd,
        0xde,
        0xdf,
        0xe0,
        0xe1,
        0xe2,
        0xe3,
        0xe4,
        0xe5,
        0xe6,
        0xe7,
        0xe8,
        0xe9,
        0xea,
        0xeb,
        0xec,
        0xed,
        0xee,
        0xef,
        0xf0,
        0xf1,
        0xf2,
        0xf3,
        0xf4,
        0xf5,
        0xf6,
        0xf7,
        0xf8,
        0xf9,
        0xfa,
		0xfb,
		0xfc,
		0xfd,
		0xfe,
        // upper case
        0xb6,
        0xb8,
        0xb9,
        0xba,
        0xbc,
        0xbe,
        0xbf,
        0xc1,
        0xc2,
        0xc3,
        0xc4,
        0xc5,
        0xc6,
        0xc7,
        0xc8,
        0xc9,
        0xca,
        0xcb,
        0xcc,
        0xcd,
        0xce,
        0xcf,
        0xd0,
        0xd1,
        0xd3,
        0xd4,
        0xd5,
        0xd6,
        0xd7,
        0xd8,
        0xd9,
        0xda,
		0xdb
    };

    // CP1253 charset
    public static char[] CP1253 = {
       	// lower case
        0xc0,
        0xdc,
        0xdd,
        0xde,
        0xdf,
        0xe0,
        0xe1,
        0xe2,
        0xe3,
        0xe4,
        0xe5,
        0xe6,
        0xe7,
        0xe8,
        0xe9,
        0xea,
        0xeb,
        0xec,
        0xed,
        0xee,
        0xef,
        0xf0,
        0xf1,
        0xf2,
        0xf3,
        0xf4,
        0xf5,
        0xf6,
        0xf7,
        0xf8,
        0xf9,
        0xfa,
		0xfb,
		0xfc,
		0xfd,
		0xfe,
        // upper case
        0xa2,
        0xb8,
        0xb9,
        0xba,
        0xbc,
        0xbe,
        0xbf,
        0xc1,
        0xc2,
        0xc3,
        0xc4,
        0xc5,
        0xc6,
        0xc7,
        0xc8,
        0xc9,
        0xca,
        0xcb,
        0xcc,
        0xcd,
        0xce,
        0xcf,
        0xd0,
        0xd1,
        0xd3,
        0xd4,
        0xd5,
        0xd6,
        0xd7,
        0xd8,
        0xd9,
        0xda,
		0xdb
    };

    public static char toLowerCase(char letter, char[] charset)
    {
        if (charset == UnicodeGreek) {
        	// First deal with lower case, not accented letters
            if (letter >= '\u03B1' && letter <= '\u03C9')
            {
            	// Special case 'small final sigma', where we return 'small sigma'
                if (letter == '\u03C2') {
                	return '\u03C3';
                } else {
                	return letter;
                }
            }
            // Then deal with lower case, accented letters
            // alpha with acute
            if (letter == '\u03AC') {
            	return '\u03B1';
            }
            // epsilon with acute
            if (letter == '\u03AD') {
            	return '\u03B5';
            }
            // eta with acute
            if (letter == '\u03AE') {
            	return '\u03B7';
            }
            // iota with acute, iota with diaeresis, iota with acute and diaeresis
            if (letter == '\u03AF' || letter == '\u03CA' || letter == '\u0390') {
            	return '\u03B9';
            }
            // upsilon with acute, upsilon with diaeresis, upsilon with acute and diaeresis
            if (letter == '\u03CD' || letter == '\u03CB' || letter == '\u03B0') {
            	return '\u03C5';
            }
            // omicron with acute
            if (letter == '\u03CC') {
            	return '\u03BF';
            }
            // omega with acute
            if (letter == '\u03CE') {
            	return '\u03C9';
            }
            // After that, deal with upper case, not accented letters
            if (letter >= '\u0391' && letter <= '\u03A9')
            {
                return (char) (letter + 32);
            }
            // Finally deal with upper case, accented letters
            // alpha with acute
            if (letter == '\u0386') {
            	return '\u03B1';
            }
            // epsilon with acute
            if (letter == '\u0388') {
            	return '\u03B5';
            }
            // eta with acute
            if (letter == '\u0389') {
            	return '\u03B7';
            }
            // iota with acute, iota with diaeresis
            if (letter == '\u038A' || letter == '\u03AA') {
            	return '\u03B9';
            }
            // upsilon with acute, upsilon with diaeresis
            if (letter == '\u038E' || letter == '\u03AB') {
            	return '\u03C5';
            }
            // omicron with acute
            if (letter == '\u038C') {
            	return '\u03BF';
            }
            // omega with acute
            if (letter == '\u038F') {
            	return '\u03C9';
            }
        } else if (charset == ISO) {
        	// First deal with lower case, not accented letters
            if (letter >= 0xe1 && letter <= 0xf9)
            {
            	// Special case 'small final sigma', where we return 'small sigma'
                if (letter == 0xf2) {
                	return 0xf3;
                } else {
                	return letter;
                }
            }
            // Then deal with lower case, accented letters
            // alpha with acute
            if (letter == 0xdc) {
            	return 0xe1;
            }
            // epsilon with acute
            if (letter == 0xdd) {
            	return 0xe5;
            }
            // eta with acute
            if (letter == 0xde) {
            	return 0xe7;
            }
            // iota with acute, iota with diaeresis, iota with acute and diaeresis
            if (letter == 0xdf || letter == 0xfa || letter == 0xc0) {
            	return '\u03B9';
            }
            // upsilon with acute, upsilon with diaeresis, upsilon with acute and diaeresis
            if (letter == 0xfd || letter == 0xfb || letter == 0xe0) {
            	return 0xf5;
            }
            // omicron with acute
            if (letter == 0xfc) {
            	return 0xef;
            }
            // omega with acute
            if (letter == 0xfe) {
            	return 0xf9;
            }
            // After that, deal with upper case, not accented letters
            if (letter >= 0xc1 && letter <= 0xd9) {
                return (char) (letter + 32);
            }
            // Finally deal with upper case, accented letters
            // alpha with acute
            if (letter == 0xb6) {
            	return 0xe1;
            }
            // epsilon with acute
            if (letter == 0xb8) {
            	return 0xe5;
            }
            // eta with acute
            if (letter == 0xb9) {
            	return 0xe7;
            }
            // iota with acute, iota with diaeresis
            if (letter == 0xba || letter == 0xda) {
            	return 0xe9;
            }
            // upsilon with acute, upsilon with diaeresis
            if (letter == 0xbe || letter == 0xdb) {
            	return 0xf5;
            }
            // omicron with acute
            if (letter == 0xbc) {
            	return 0xef;
            }
            // omega with acute
            if (letter == 0xbf) {
            	return 0xf9;
            }
        } else if (charset == CP1253) {
        	// First deal with lower case, not accented letters
            if (letter >= 0xe1 && letter <= 0xf9)
            {
            	// Special case 'small final sigma', where we return 'small sigma'
                if (letter == 0xf2) {
                	return 0xf3;
                } else {
                	return letter;
                }
            }
            // Then deal with lower case, accented letters
            // alpha with acute
            if (letter == 0xdc) {
            	return 0xe1;
            }
            // epsilon with acute
            if (letter == 0xdd) {
            	return 0xe5;
            }
            // eta with acute
            if (letter == 0xde) {
            	return 0xe7;
            }
            // iota with acute, iota with diaeresis, iota with acute and diaeresis
            if (letter == 0xdf || letter == 0xfa || letter == 0xc0) {
            	return '\u03B9';
            }
            // upsilon with acute, upsilon with diaeresis, upsilon with acute and diaeresis
            if (letter == 0xfd || letter == 0xfb || letter == 0xe0) {
            	return 0xf5;
            }
            // omicron with acute
            if (letter == 0xfc) {
            	return 0xef;
            }
            // omega with acute
            if (letter == 0xfe) {
            	return 0xf9;
            }
            // After that, deal with upper case, not accented letters
            if (letter >= 0xc1 && letter <= 0xd9) {
                return (char) (letter + 32);
            }
            // Finally deal with upper case, accented letters
            // alpha with acute
            if (letter == 0xa2) {
            	return 0xe1;
            }
            // epsilon with acute
            if (letter == 0xb8) {
            	return 0xe5;
            }
            // eta with acute
            if (letter == 0xb9) {
            	return 0xe7;
            }
            // iota with acute, iota with diaeresis
            if (letter == 0xba || letter == 0xda) {
            	return 0xe9;
            }
            // upsilon with acute, upsilon with diaeresis
            if (letter == 0xbe || letter == 0xdb) {
            	return 0xf5;
            }
            // omicron with acute
            if (letter == 0xbc) {
            	return 0xef;
            }
            // omega with acute
            if (letter == 0xbf) {
            	return 0xf9;
            }
        }

        return Character.toLowerCase(letter);
    }
}
