package org.apache.lucene.analysis.cn;

import java.io.Reader;
import org.apache.lucene.analysis.*;


/**
 * Title: ChineseTokenizer
 * Description: Extract tokens from the Stream using Character.getType()
 *              Rule: A Chinese character as a single token
 * Copyright:   Copyright (c) 2001
 * Company:
 * @author Yiyi Sun
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

    private final Token flush() {

        if (length>0) {
            //System.out.println(new String(buffer, 0, length));
            return new Token(new String(buffer, 0, length), start, start+length);
        }
        else
            return null;
    }

    public final Token next() throws java.io.IOException {

        length = 0;
        start = offset;


        while (true) {

            final char c;
            offset++;

            if (bufferIndex >= dataLen) {
                dataLen = input.read(ioBuffer);
                bufferIndex = 0;
            };

            if (dataLen == -1) return flush();
            else
                c = ioBuffer[bufferIndex++];


            switch(Character.getType(c)) {

            case Character.DECIMAL_DIGIT_NUMBER:
            case Character.LOWERCASE_LETTER:
            case Character.UPPERCASE_LETTER:
                push(c);
                if (length == MAX_WORD_LEN) return flush();
                break;

            case Character.OTHER_LETTER:
                if (length>0) {
                    bufferIndex--;
                    return flush();
                }
                push(c);
                return flush();

            default:
                if (length>0) return flush();
                break;
            }
        }

    }
}