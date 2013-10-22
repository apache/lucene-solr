package org.apache.lucene.analysis.ko;

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

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.ClassicTokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;

public final class KoreanTokenizer extends Tokenizer {
  
  /** A private instance of the JFlex-constructed scanner */
  private final KoreanTokenizerImpl scanner;

  public static final int ALPHANUM          = 0;
  public static final int APOSTROPHE        = 1;
  public static final int ACRONYM           = 2;
  public static final int COMPANY           = 3;
  public static final int EMAIL             = 4;
  public static final int HOST              = 5;
  public static final int NUM               = 6;
  public static final int CJ                = 7;
  public static final int ACRONYM_DEP      = 8;
  public static final int KOREAN      = 9;
  public static final int CHINESE      = 10;
    

  /** String token types that correspond to token type int constants */
  public static final String [] TOKEN_TYPES = new String [] {
      ClassicTokenizer.TOKEN_TYPES[ClassicTokenizer.ALPHANUM],
      ClassicTokenizer.TOKEN_TYPES[ClassicTokenizer.APOSTROPHE],
      ClassicTokenizer.TOKEN_TYPES[ClassicTokenizer.ACRONYM],
      ClassicTokenizer.TOKEN_TYPES[ClassicTokenizer.COMPANY],
      ClassicTokenizer.TOKEN_TYPES[ClassicTokenizer.EMAIL],
      ClassicTokenizer.TOKEN_TYPES[ClassicTokenizer.HOST],
      ClassicTokenizer.TOKEN_TYPES[ClassicTokenizer.NUM],
      ClassicTokenizer.TOKEN_TYPES[ClassicTokenizer.CJ],
      ClassicTokenizer.TOKEN_TYPES[ClassicTokenizer.ACRONYM_DEP],
      "<KOREAN>",
      "<CHINESE>"
  };
    
  private int maxTokenLength = StandardAnalyzer.DEFAULT_MAX_TOKEN_LENGTH;

  /** Set the max allowed token length.  Any token longer
   *  than this is skipped. */
  public void setMaxTokenLength(int length) {
    this.maxTokenLength = length;
  }

  /** @see #setMaxTokenLength */
  public int getMaxTokenLength() {
    return maxTokenLength;
  }

  /**
   * Creates a new instance of the KoreanTokenizer.  Attaches
   * the <code>input</code> to the newly created JFlex scanner.
   *
   * @param input The input reader
   *
   * See http://issues.apache.org/jira/browse/LUCENE-1068
   */
  public KoreanTokenizer(Reader input) {
    super(input);
    this.scanner = new KoreanTokenizerImpl(this.input);
  }

  /**
   * Creates a new KoreanTokenizer with a given {@link org.apache.lucene.util.AttributeSource.AttributeFactory} 
   */
  public KoreanTokenizer(AttributeFactory factory, Reader input) {
    super(factory, input);
    this.scanner = new KoreanTokenizerImpl(this.input);
   }

  // this tokenizer generates three attributes:
  // term offset, positionIncrement and type
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
  private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);

  /*
     * (non-Javadoc)
     *
     * @see org.apache.lucene.analysis.TokenStream#next()
     */
  @Override
  public boolean incrementToken() throws IOException {
    clearAttributes();
    int posIncr = 1;

    while(true) {
      int tokenType = scanner.getNextToken();

      if (tokenType == KoreanTokenizerImpl.YYEOF) {
        return false;
      }
      
      if (scanner.yylength() <= maxTokenLength) {
        posIncrAtt.setPositionIncrement(posIncr);
        scanner.getText(termAtt);          
        final int start = scanner.yychar();
        offsetAtt.setOffset(correctOffset(start), correctOffset(start+termAtt.length())); 
        typeAtt.setType(KoreanTokenizer.TOKEN_TYPES[tokenType]);
          
        return true;
      } else
        // When we skip a too-long term, we still increment the
        // position increment
        posIncr++;
    }
  }
    
  @Override
  public void end() throws IOException {
    super.end();
    // set final offset
    int finalOffset = correctOffset(scanner.yychar() + scanner.yylength());
    offsetAtt.setOffset(finalOffset, finalOffset);
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    scanner.yyreset(input);
  }

  @Override
  public void close() throws IOException {
    super.close();
    scanner.yyreset(input);
  }
  
}
