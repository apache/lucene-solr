package org.apache.lucene.analysis.cjk;

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

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.Tokenizer;

import java.io.Reader;


/**
 * <p>
 * CJKTokenizer was modified from StopTokenizer which does a decent job for
 * most European languages. and it perferm other token method for double-byte
 * Characters: the token will return at each two charactors with overlap match.<br>
 * Example: "java C1C2C3C4" will be segment to: "java" "C1C2" "C2C3" "C3C4" it
 * also need filter filter zero length token ""<br>
 * for Digit: digit, '+', '#' will token as letter<br>
 * for more info on Asia language(Chinese Japanese Korean) text segmentation:
 * please search  <a
 * href="http://www.google.com/search?q=word+chinese+segment">google</a>
 * </p>
 *
 * @author Che, Dong
 */
public final class CJKTokenizer extends Tokenizer {
    //~ Static fields/initializers ---------------------------------------------

    /** Max word length */
    private static final int MAX_WORD_LEN = 255;

    /** buffer size: */
    private static final int IO_BUFFER_SIZE = 256;

    //~ Instance fields --------------------------------------------------------

    /** word offset, used to imply which character(in ) is parsed */
    private int offset = 0;

    /** the index used only for ioBuffer */
    private int bufferIndex = 0;

    /** data length */
    private int dataLen = 0;

    /**
     * character buffer, store the characters which are used to compose <br>
     * the returned Token
     */
    private final char[] buffer = new char[MAX_WORD_LEN];

    /**
     * I/O buffer, used to store the content of the input(one of the <br>
     * members of Tokenizer)
     */
    private final char[] ioBuffer = new char[IO_BUFFER_SIZE];

    /** word type: single=>ASCII  double=>non-ASCII word=>default */
    private String tokenType = "word";

    /**
     * tag: previous character is a cached double-byte character  "C1C2C3C4"
     * ----(set the C1 isTokened) C1C2 "C2C3C4" ----(set the C2 isTokened)
     * C1C2 C2C3 "C3C4" ----(set the C3 isTokened) "C1C2 C2C3 C3C4"
     */
    private boolean preIsTokened = false;

    //~ Constructors -----------------------------------------------------------

    /**
     * Construct a token stream processing the given input.
     *
     * @param in I/O reader
     */
    public CJKTokenizer(Reader in) {
        input = in;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Returns the next token in the stream, or null at EOS.
     *
     * @return Token
     *
     * @throws java.io.IOException - throw IOException when read error <br>
     *         hanppened in the InputStream
     *
     * @see "http://java.sun.com/j2se/1.3/docs/api/java/lang/Character.UnicodeBlock.html"
     *      for detail
     */
    public final Token next() throws java.io.IOException {
        /** how many character(s) has been stored in buffer */
        int length = 0;

        /** the position used to create Token */
        int start = offset;

        while (true) {
            /** current charactor */
            char c;

            /** unicode block of current charactor for detail */
            Character.UnicodeBlock ub;

            offset++;

            if (bufferIndex >= dataLen) {
                dataLen = input.read(ioBuffer);
                bufferIndex = 0;
            }

            if (dataLen == -1) {
                if (length > 0) {
                    if (preIsTokened == true) {
                        length = 0;
                        preIsTokened = false;
                    }

                    break;
                } else {
                    return null;
                }
            } else {
                //get current character
                c = ioBuffer[bufferIndex++];

                //get the UnicodeBlock of the current character
                ub = Character.UnicodeBlock.of(c);
            }

            //if the current character is ASCII or Extend ASCII
            if ((ub == Character.UnicodeBlock.BASIC_LATIN)
                    || (ub == Character.UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS)
               ) {
                if (ub == Character.UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS) {
                    /** convert  HALFWIDTH_AND_FULLWIDTH_FORMS to BASIC_LATIN */
                    int i = (int) c;
                    i = i - 65248;
                    c = (char) i;
                }

                // if the current character is a letter or "_" "+" "#"
                if (Character.isLetterOrDigit(c)
                        || ((c == '_') || (c == '+') || (c == '#'))
                   ) {
                    if (length == 0) {
                        // "javaC1C2C3C4linux" <br>
                        //      ^--: the current character begin to token the ASCII
                        // letter
                        start = offset - 1;
                    } else if (tokenType == "double") {
                        // "javaC1C2C3C4linux" <br>
                        //              ^--: the previous non-ASCII
                        // : the current character
                        offset--;
                        bufferIndex--;
                        tokenType = "single";

                        if (preIsTokened == true) {
                            // there is only one non-ASCII has been stored
                            length = 0;
                            preIsTokened = false;

                            break;
                        } else {
                            break;
                        }
                    }

                    // store the LowerCase(c) in the buffer
                    buffer[length++] = Character.toLowerCase(c);
                    tokenType = "single";

                    // break the procedure if buffer overflowed!
                    if (length == MAX_WORD_LEN) {
                        break;
                    }
                } else if (length > 0) {
                    if (preIsTokened == true) {
                        length = 0;
                        preIsTokened = false;
                    } else {
                        break;
                    }
                }
            } else {
                // non-ASCII letter, eg."C1C2C3C4"
                if (Character.isLetter(c)) {
                    if (length == 0) {
                        start = offset - 1;
                        buffer[length++] = c;
                        tokenType = "double";
                    } else {
                        if (tokenType == "single") {
                            offset--;
                            bufferIndex--;

                            //return the previous ASCII characters
                            break;
                        } else {
                            buffer[length++] = c;
                            tokenType = "double";

                            if (length == 2) {
                                offset--;
                                bufferIndex--;
                                preIsTokened = true;

                                break;
                            }
                        }
                    }
                } else if (length > 0) {
                    if (preIsTokened == true) {
                        // empty the buffer
                        length = 0;
                        preIsTokened = false;
                    } else {
                        break;
                    }
                }
            }
        }

        return new Token(new String(buffer, 0, length), start, start + length,
                         tokenType
                        );
    }
}
