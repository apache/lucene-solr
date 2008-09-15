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


import java.io.Reader;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.Tokenizer;


/**
 * Title: ChineseTokenizer
 * Description: Extract tokens from the Stream using Character.getType()
 *              Rule: A Chinese character as a single token
 * Copyright:   Copyright (c) 2001
 * Company:
 *
 * The difference between thr ChineseTokenizer and the
 * CJKTokenizer (id=23545) is that they have different
 * token parsing logic.
 * 
 * Let me use an example. If having a Chinese text
 * "C1C2C3C4" to be indexed, the tokens returned from the
 * ChineseTokenizer are C1, C2, C3, C4. And the tokens
 * returned from the CJKTokenizer are C1C2, C2C3, C3C4.
 *
 * Therefore the index the CJKTokenizer created is much
 * larger.
 *
 * The problem is that when searching for C1, C1C2, C1C3,
 * C4C2, C1C2C3 ... the ChineseTokenizer works, but the
 * CJKTokenizer will not work.
 *
 * @version 1.0
 *
 */

public final class ChineseTokenizer extends Tokenizer {


    public ChineseTokenizer(Reader in) {
        input = in;
    }

    private int offset = 0, bufferIndex=0, dataLen=0;
    private final static int MAX_WORD_LEN = 255;
    private final static int IO_BUFFER_SIZE = 1024;
    private final char[] buffer = new char[MAX_WORD_LEN];
    private final char[] ioBuffer = new char[IO_BUFFER_SIZE];


    private int length;
    private int start;


    private final void push(char c) {

        if (length == 0) start = offset-1;            // start of token
        buffer[length++] = Character.toLowerCase(c);  // buffer it

    }

    private final Token flush(final Token token) {

        if (length>0) {
            //System.out.println(new String(buffer, 0,
            //length));
          return token.reinit(buffer, 0, length, start, start+length);
        }
        else
            return null;
    }

    public final Token next(final Token reusableToken) throws java.io.IOException {
        assert reusableToken != null;

        length = 0;
        start = offset;


        while (true) {

            final char c;
            offset++;

            if (bufferIndex >= dataLen) {
                dataLen = input.read(ioBuffer);
                bufferIndex = 0;
            }

            if (dataLen == -1) return flush(reusableToken);
            else
                c = ioBuffer[bufferIndex++];


            switch(Character.getType(c)) {

            case Character.DECIMAL_DIGIT_NUMBER:
            case Character.LOWERCASE_LETTER:
            case Character.UPPERCASE_LETTER:
                push(c);
                if (length == MAX_WORD_LEN) return flush(reusableToken);
                break;

            case Character.OTHER_LETTER:
                if (length>0) {
                    bufferIndex--;
                    offset--;
                    return flush(reusableToken);
                }
                push(c);
                return flush(reusableToken);

            default:
                if (length>0) return flush(reusableToken);
                break;
            }
        }

    }
}
