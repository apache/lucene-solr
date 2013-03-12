package org.apache.lucene.analysis.cn;

/*
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


import java.io.IOException;
import java.io.Reader;

import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;


/**
 * Tokenize Chinese text as individual chinese characters.
 * 
 * <p>
 * The difference between ChineseTokenizer and
 * CJKTokenizer is that they have different
 * token parsing logic.
 * </p>
 * <p>
 * For example, if the Chinese text
 * "C1C2C3C4" is to be indexed:
 * <ul>
 * <li>The tokens returned from ChineseTokenizer are C1, C2, C3, C4. 
 * <li>The tokens returned from the CJKTokenizer are C1C2, C2C3, C3C4.
 * </ul>
 * </p>
 * <p>
 * Therefore the index created by CJKTokenizer is much larger.
 * </p>
 * <p>
 * The problem is that when searching for C1, C1C2, C1C3,
 * C4C2, C1C2C3 ... the ChineseTokenizer works, but the
 * CJKTokenizer will not work.
 * </p>
 * @deprecated (3.1) Use {@link StandardTokenizer} instead, which has the same functionality.
 * This filter will be removed in Lucene 5.0
 */
@Deprecated
public final class ChineseTokenizer extends Tokenizer {


    public ChineseTokenizer(Reader in) {
      super(in);
    }

    public ChineseTokenizer(AttributeFactory factory, Reader in) {
      super(factory, in);
    }
       
    private int offset = 0, bufferIndex=0, dataLen=0;
    private final static int MAX_WORD_LEN = 255;
    private final static int IO_BUFFER_SIZE = 1024;
    private final char[] buffer = new char[MAX_WORD_LEN];
    private final char[] ioBuffer = new char[IO_BUFFER_SIZE];


    private int length;
    private int start;

    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
    
    private final void push(char c) {

        if (length == 0) start = offset-1;            // start of token
        buffer[length++] = Character.toLowerCase(c);  // buffer it

    }

    private final boolean flush() {

        if (length>0) {
            //System.out.println(new String(buffer, 0,
            //length));
          termAtt.copyBuffer(buffer, 0, length);
          offsetAtt.setOffset(correctOffset(start), correctOffset(start+length));
          return true;
        }
        else
            return false;
    }

    @Override
    public boolean incrementToken() throws IOException {
        clearAttributes();

        length = 0;
        start = offset;


        while (true) {

            final char c;
            offset++;

            if (bufferIndex >= dataLen) {
                dataLen = input.read(ioBuffer);
                bufferIndex = 0;
            }

            if (dataLen == -1) {
              offset--;
              return flush();
            } else
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
                    offset--;
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
    
    @Override
    public final void end() {
      // set final offset
      final int finalOffset = correctOffset(offset);
      this.offsetAtt.setOffset(finalOffset, finalOffset);
    }

    @Override
    public void reset() throws IOException {
      super.reset();
      offset = bufferIndex = dataLen = 0;
    }
}
