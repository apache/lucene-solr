package org.apache.lucene.analysis.standard;

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
import org.apache.lucene.analysis.standard.std31.UAX29URLEmailTokenizerImpl31;
import org.apache.lucene.analysis.standard.std34.UAX29URLEmailTokenizerImpl34;
import org.apache.lucene.analysis.standard.std36.UAX29URLEmailTokenizerImpl36;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.Version;

/**
 * This class implements Word Break rules from the Unicode Text Segmentation 
 * algorithm, as specified in 
 * <a href="http://unicode.org/reports/tr29/">Unicode Standard Annex #29</a> 
 * URLs and email addresses are also tokenized according to the relevant RFCs.
 * <p/>
 * Tokens produced are of the following types:
 * <ul>
 *   <li>&lt;ALPHANUM&gt;: A sequence of alphabetic and numeric characters</li>
 *   <li>&lt;NUM&gt;: A number</li>
 *   <li>&lt;URL&gt;: A URL</li>
 *   <li>&lt;EMAIL&gt;: An email address</li>
 *   <li>&lt;SOUTHEAST_ASIAN&gt;: A sequence of characters from South and Southeast
 *       Asian languages, including Thai, Lao, Myanmar, and Khmer</li>
 *   <li>&lt;IDEOGRAPHIC&gt;: A single CJKV ideographic character</li>
 *   <li>&lt;HIRAGANA&gt;: A single hiragana character</li>
 * </ul>
 * <a name="version"/>
 * <p>You must specify the required {@link Version}
 * compatibility when creating UAX29URLEmailTokenizer:
 * <ul>
 *   <li> As of 3.4, Hiragana and Han characters are no longer wrongly split
 *   from their combining characters. If you use a previous version number,
 *   you get the exact broken behavior for backwards compatibility.
 * </ul>
 */

public final class UAX29URLEmailTokenizer extends Tokenizer {
  /** A private instance of the JFlex-constructed scanner */
  private final StandardTokenizerInterface scanner;
  
  public static final int ALPHANUM          = 0;
  public static final int NUM               = 1;
  public static final int SOUTHEAST_ASIAN   = 2;
  public static final int IDEOGRAPHIC       = 3;
  public static final int HIRAGANA          = 4;
  public static final int KATAKANA          = 5;
  public static final int HANGUL            = 6;
  public static final int URL               = 7;
  public static final int EMAIL             = 8;

  /** String token types that correspond to token type int constants */
  public static final String [] TOKEN_TYPES = new String [] {
    StandardTokenizer.TOKEN_TYPES[StandardTokenizer.ALPHANUM],
    StandardTokenizer.TOKEN_TYPES[StandardTokenizer.NUM],
    StandardTokenizer.TOKEN_TYPES[StandardTokenizer.SOUTHEAST_ASIAN],
    StandardTokenizer.TOKEN_TYPES[StandardTokenizer.IDEOGRAPHIC],
    StandardTokenizer.TOKEN_TYPES[StandardTokenizer.HIRAGANA],
    StandardTokenizer.TOKEN_TYPES[StandardTokenizer.KATAKANA],
    StandardTokenizer.TOKEN_TYPES[StandardTokenizer.HANGUL],
    "<URL>",
    "<EMAIL>",
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
   * Creates a new instance of the UAX29URLEmailTokenizer.  Attaches
   * the <code>input</code> to the newly created JFlex scanner.
   *
   * @param input The input reader
   */
  public UAX29URLEmailTokenizer(Version matchVersion, Reader input) {
    super(input);
    this.scanner = getScannerFor(matchVersion);
  }

  /**
   * Creates a new UAX29URLEmailTokenizer with a given {@link org.apache.lucene.util.AttributeSource.AttributeFactory}
   */
  public UAX29URLEmailTokenizer(Version matchVersion, AttributeFactory factory, Reader input) {
    super(factory, input);
    this.scanner = getScannerFor(matchVersion);
  }

  private static StandardTokenizerInterface getScannerFor(Version matchVersion) {
    // best effort NPE if you dont call reset
    if (matchVersion.onOrAfter(Version.LUCENE_40)) {
      return new UAX29URLEmailTokenizerImpl(null);
    } else if (matchVersion.onOrAfter(Version.LUCENE_36)) {
      return new UAX29URLEmailTokenizerImpl36(null);
    } else if (matchVersion.onOrAfter(Version.LUCENE_34)) {
      return new UAX29URLEmailTokenizerImpl34(null);
    } else {
      return new UAX29URLEmailTokenizerImpl31(null);
    }
  }

  // this tokenizer generates three attributes:
  // term offset, positionIncrement and type
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
  private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);

  @Override
  public final boolean incrementToken() throws IOException {
    clearAttributes();
    int posIncr = 1;

    while(true) {
      int tokenType = scanner.getNextToken();

      if (tokenType == StandardTokenizerInterface.YYEOF) {
        return false;
      }

      if (scanner.yylength() <= maxTokenLength) {
        posIncrAtt.setPositionIncrement(posIncr);
        scanner.getText(termAtt);
        final int start = scanner.yychar();
        offsetAtt.setOffset(correctOffset(start), correctOffset(start+termAtt.length()));
        typeAtt.setType(TOKEN_TYPES[tokenType]);
        return true;
      } else
        // When we skip a too-long term, we still increment the
        // position increment
        posIncr++;
    }
  }
  
  @Override
  public final void end() {
    // set final offset
    int finalOffset = correctOffset(scanner.yychar() + scanner.yylength());
    offsetAtt.setOffset(finalOffset, finalOffset);
  }

  @Override
  public void reset() throws IOException {
    scanner.yyreset(input);
  }
}
