package org.apache.lucene.analysis.standard;

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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.std31.StandardTokenizerImpl31;
import org.apache.lucene.analysis.standard.std31.UAX29URLEmailTokenizerImpl31;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.Version;
import org.apache.lucene.util.AttributeSource.AttributeFactory;

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

  /** Alphanumeric sequences
   * @deprecated use {@link #TOKEN_TYPES} instead */
  @Deprecated
  public static final String WORD_TYPE = TOKEN_TYPES[ALPHANUM];
  
  /** Numbers 
   * @deprecated use {@link #TOKEN_TYPES} instead */
  @Deprecated
  public static final String NUMERIC_TYPE = TOKEN_TYPES[NUM];
  
  /** URLs with scheme: HTTP(S), FTP, or FILE; no-scheme URLs match HTTP syntax 
   * @deprecated use {@link #TOKEN_TYPES} instead */
  @Deprecated
  public static final String URL_TYPE = TOKEN_TYPES[URL];
  
  /** E-mail addresses 
   * @deprecated use {@link #TOKEN_TYPES} instead */
  @Deprecated
  public static final String EMAIL_TYPE = TOKEN_TYPES[EMAIL];
  
  /**
   * Chars in class \p{Line_Break = Complex_Context} are from South East Asian
   * scripts (Thai, Lao, Myanmar, Khmer, etc.).  Sequences of these are kept 
   * together as as a single token rather than broken up, because the logic
   * required to break them at word boundaries is too complex for UAX#29.
   * <p>
   * See Unicode Line Breaking Algorithm: http://www.unicode.org/reports/tr14/#SA
   * @deprecated use {@link #TOKEN_TYPES} instead
   */
  @Deprecated
  public static final String SOUTH_EAST_ASIAN_TYPE = TOKEN_TYPES[SOUTHEAST_ASIAN];
  
  /** @deprecated use {@link #TOKEN_TYPES} instead */
  @Deprecated
  public static final String IDEOGRAPHIC_TYPE = TOKEN_TYPES[IDEOGRAPHIC];
  
  /** @deprecated use {@link #TOKEN_TYPES} instead */
  @Deprecated
  public static final String HIRAGANA_TYPE = TOKEN_TYPES[HIRAGANA];
  
  /** @deprecated use {@link #TOKEN_TYPES} instead */
  @Deprecated
  public static final String KATAKANA_TYPE = TOKEN_TYPES[KATAKANA];

  /** @deprecated use {@link #TOKEN_TYPES} instead */
  @Deprecated
  public static final String HANGUL_TYPE = TOKEN_TYPES[HANGUL];

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
  
  /** @deprecated use {@link #UAX29URLEmailTokenizer(Version, Reader)} instead. */
  @Deprecated
  public UAX29URLEmailTokenizer(Reader input) {
    this(Version.LUCENE_31, input);
  }
  
  /** @deprecated use {@link #UAX29URLEmailTokenizer(Version, Reader)} instead. */
  @Deprecated
  public UAX29URLEmailTokenizer(InputStream input) {
    this(Version.LUCENE_31, new InputStreamReader(input));
  }
  
  /** @deprecated use {@link #UAX29URLEmailTokenizer(Version, AttributeSource, Reader)} instead. */
  @Deprecated
  public UAX29URLEmailTokenizer(AttributeSource source, Reader input) {
    this(Version.LUCENE_31, source, input);
  }
  
  /** @deprecated use {@link #UAX29URLEmailTokenizer(Version, AttributeSource.AttributeFactory, Reader)} instead. */
  @Deprecated
  public UAX29URLEmailTokenizer(AttributeFactory factory, Reader input) {
    this(Version.LUCENE_31, factory, input);
  }

  /**
   * Creates a new instance of the UAX29URLEmailTokenizer.  Attaches
   * the <code>input</code> to the newly created JFlex scanner.
   *
   * @param input The input reader
   */
  public UAX29URLEmailTokenizer(Version matchVersion, Reader input) {
    super(input);
    this.scanner = getScannerFor(matchVersion, input);
  }

  /**
   * Creates a new UAX29URLEmailTokenizer with a given {@link AttributeSource}. 
   */
  public UAX29URLEmailTokenizer(Version matchVersion, AttributeSource source, Reader input) {
    super(source, input);
    this.scanner = getScannerFor(matchVersion, input);
  }

  /**
   * Creates a new UAX29URLEmailTokenizer with a given {@link AttributeFactory} 
   */
  public UAX29URLEmailTokenizer(Version matchVersion, AttributeFactory factory, Reader input) {
    super(factory, input);
    this.scanner = getScannerFor(matchVersion, input);
  }

  private static StandardTokenizerInterface getScannerFor(Version matchVersion, Reader input) {
    if (matchVersion.onOrAfter(Version.LUCENE_34)) {
      return new UAX29URLEmailTokenizerImpl(input);
    } else {
      return new UAX29URLEmailTokenizerImpl31(input);
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
  public void reset(Reader reader) throws IOException {
    super.reset(reader);
    scanner.yyreset(reader);
  }
}
