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

package org.apache.lucene.analysis.standard;

import java.io.IOException;
import java.io.Reader;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.Version;

/** A grammar-based tokenizer constructed with JFlex
 *
 * <p> This should be a good tokenizer for most European-language documents:
 *
 * <ul>
 *   <li>Splits words at punctuation characters, removing punctuation. However, a 
 *     dot that's not followed by whitespace is considered part of a token.
 *   <li>Splits words at hyphens, unless there's a number in the token, in which case
 *     the whole token is interpreted as a product number and is not split.
 *   <li>Recognizes email addresses and internet hostnames as one token.
 * </ul>
 *
 * <p>Many applications have specific tokenizer needs.  If this tokenizer does
 * not suit your application, please consider copying this source code
 * directory to your project and maintaining your own grammar-based tokenizer.
 *
 * <a name="version"/>
 * <p>You must specify the required {@link Version}
 * compatibility when creating StandardAnalyzer:
 * <ul>
 *   <li> As of 2.4, Tokens incorrectly identified as acronyms
 *        are corrected (see <a href="https://issues.apache.org/jira/browse/LUCENE-1068">LUCENE-1608</a>
 * </ul>
 */

public class StandardTokenizer extends Tokenizer {
  /** A private instance of the JFlex-constructed scanner */
  private final StandardTokenizerImpl scanner;

  public static final int ALPHANUM          = 0;
  public static final int APOSTROPHE        = 1;
  public static final int ACRONYM           = 2;
  public static final int COMPANY           = 3;
  public static final int EMAIL             = 4;
  public static final int HOST              = 5;
  public static final int NUM               = 6;
  public static final int CJ                = 7;

  /**
   * @deprecated this solves a bug where HOSTs that end with '.' are identified
   *             as ACRONYMs. It is deprecated and will be removed in the next
   *             release.
   */
  public static final int ACRONYM_DEP       = 8;

  /** String token types that correspond to token type int constants */
  public static final String [] TOKEN_TYPES = new String [] {
    "<ALPHANUM>",
    "<APOSTROPHE>",
    "<ACRONYM>",
    "<COMPANY>",
    "<EMAIL>",
    "<HOST>",
    "<NUM>",
    "<CJ>",
    "<ACRONYM_DEP>"
  };

  /** @deprecated Please use {@link #TOKEN_TYPES} instead */
  public static final String [] tokenImage = TOKEN_TYPES;

  /**
   * Specifies whether deprecated acronyms should be replaced with HOST type.
   * This is false by default to support backward compatibility.
   *<p/>
   * See http://issues.apache.org/jira/browse/LUCENE-1068
   * 
   * @deprecated this should be removed in the next release (3.0).
   */
  private boolean replaceInvalidAcronym;
    
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
   * Creates a new instance of the {@link StandardTokenizer}. Attaches the
   * <code>input</code> to a newly created JFlex scanner.
   *
   * @deprecated Use {@link #StandardTokenizer(Version,
   * Reader)} instead
   */
  public StandardTokenizer(Reader input) {
    this(Version.LUCENE_24, input);
  }

  /**
   * Creates a new instance of the {@link org.apache.lucene.analysis.standard.StandardTokenizer}.  Attaches
   * the <code>input</code> to the newly created JFlex scanner.
   *
   * @param input The input reader
   * @param replaceInvalidAcronym Set to true to replace mischaracterized acronyms with HOST.
   *
   * See http://issues.apache.org/jira/browse/LUCENE-1068
   *
   * @deprecated Use {@link #StandardTokenizer(Version, Reader)} instead
   */
  public StandardTokenizer(Reader input, boolean replaceInvalidAcronym) {
    super();
    this.scanner = new StandardTokenizerImpl(input);
    init(input, replaceInvalidAcronym);
  }

  /**
   * Creates a new instance of the {@link org.apache.lucene.analysis.standard.StandardTokenizer}.  Attaches
   * the <code>input</code> to the newly created JFlex scanner.
   *
   * @param input The input reader
   *
   * See http://issues.apache.org/jira/browse/LUCENE-1068
   */
  public StandardTokenizer(Version matchVersion, Reader input) {
    super();
    this.scanner = new StandardTokenizerImpl(input);
    init(input, matchVersion);
  }

  /**
   * Creates a new StandardTokenizer with a given {@link AttributeSource}. 
   *
   * @deprecated Use {@link #StandardTokenizer(Version, AttributeSource, Reader)} instead
   */
  public StandardTokenizer(AttributeSource source, Reader input, boolean replaceInvalidAcronym) {
    super(source);
    this.scanner = new StandardTokenizerImpl(input);
    init(input, replaceInvalidAcronym);
  }

  /**
   * Creates a new StandardTokenizer with a given {@link AttributeSource}. 
   */
  public StandardTokenizer(Version matchVersion, AttributeSource source, Reader input) {
    super(source);
    this.scanner = new StandardTokenizerImpl(input);
    init(input, matchVersion);
  }

  /**
   * Creates a new StandardTokenizer with a given {@link org.apache.lucene.util.AttributeSource.AttributeFactory} 
   *
   * @deprecated Use {@link #StandardTokenizer(Version, org.apache.lucene.util.AttributeSource.AttributeFactory, Reader)} instead
   */
  public StandardTokenizer(AttributeFactory factory, Reader input, boolean replaceInvalidAcronym) {
    super(factory);
    this.scanner = new StandardTokenizerImpl(input);
    init(input, replaceInvalidAcronym);
  }

  /**
   * Creates a new StandardTokenizer with a given {@link org.apache.lucene.util.AttributeSource.AttributeFactory} 
   */
  public StandardTokenizer(Version matchVersion, AttributeFactory factory, Reader input) {
    super(factory);
    this.scanner = new StandardTokenizerImpl(input);
    init(input, matchVersion);
  }

  private void init(Reader input, boolean replaceInvalidAcronym) {
    this.replaceInvalidAcronym = replaceInvalidAcronym;
    this.input = input;    
    termAtt = (TermAttribute) addAttribute(TermAttribute.class);
    offsetAtt = (OffsetAttribute) addAttribute(OffsetAttribute.class);
    posIncrAtt = (PositionIncrementAttribute) addAttribute(PositionIncrementAttribute.class);
    typeAtt = (TypeAttribute) addAttribute(TypeAttribute.class);
  }

  private void init(Reader input, Version matchVersion) {
    if (matchVersion.onOrAfter(Version.LUCENE_24)) {
      init(input, true);
    } else {
      init(input, false);
    }
  }
  
  // this tokenizer generates three attributes:
  // offset, positionIncrement and type
  private TermAttribute termAtt;
  private OffsetAttribute offsetAtt;
  private PositionIncrementAttribute posIncrAtt;
  private TypeAttribute typeAtt;

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lucene.analysis.TokenStream#next()
   */
  public final boolean incrementToken() throws IOException {
    clearAttributes();
    int posIncr = 1;

    while(true) {
      int tokenType = scanner.getNextToken();

      if (tokenType == StandardTokenizerImpl.YYEOF) {
        return false;
      }

      if (scanner.yylength() <= maxTokenLength) {
        posIncrAtt.setPositionIncrement(posIncr);
        scanner.getText(termAtt);
        final int start = scanner.yychar();
        offsetAtt.setOffset(correctOffset(start), correctOffset(start+termAtt.termLength()));
        // This 'if' should be removed in the next release. For now, it converts
        // invalid acronyms to HOST. When removed, only the 'else' part should
        // remain.
        if (tokenType == StandardTokenizerImpl.ACRONYM_DEP) {
          if (replaceInvalidAcronym) {
            typeAtt.setType(StandardTokenizerImpl.TOKEN_TYPES[StandardTokenizerImpl.HOST]);
            termAtt.setTermLength(termAtt.termLength() - 1); // remove extra '.'
          } else {
            typeAtt.setType(StandardTokenizerImpl.TOKEN_TYPES[StandardTokenizerImpl.ACRONYM]);
          }
        } else {
          typeAtt.setType(StandardTokenizerImpl.TOKEN_TYPES[tokenType]);
        }
        return true;
      } else
        // When we skip a too-long term, we still increment the
        // position increment
        posIncr++;
    }
  }
  
  public final void end() {
    // set final offset
    int finalOffset = correctOffset(scanner.yychar() + scanner.yylength());
    offsetAtt.setOffset(finalOffset, finalOffset);
  }

  /** @deprecated Will be removed in Lucene 3.0. This method is final, as it should
   * not be overridden. Delegates to the backwards compatibility layer. */
  public final Token next(final Token reusableToken) throws IOException {
    return super.next(reusableToken);
  }

  /** @deprecated Will be removed in Lucene 3.0. This method is final, as it should
   * not be overridden. Delegates to the backwards compatibility layer. */
  public final Token next() throws IOException {
    return super.next();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lucene.analysis.TokenStream#reset()
   */
  public void reset() throws IOException {
    super.reset();
    scanner.yyreset(input);
  }

  public void reset(Reader reader) throws IOException {
    super.reset(reader);
    reset();
  }

  /**
   * Prior to https://issues.apache.org/jira/browse/LUCENE-1068, StandardTokenizer mischaracterized as acronyms tokens like www.abc.com
   * when they should have been labeled as hosts instead.
   * @return true if StandardTokenizer now returns these tokens as Hosts, otherwise false
   *
   * @deprecated Remove in 3.X and make true the only valid value
   */
  public boolean isReplaceInvalidAcronym() {
    return replaceInvalidAcronym;
  }

  /**
   *
   * @param replaceInvalidAcronym Set to true to replace mischaracterized acronyms as HOST.
   * @deprecated Remove in 3.X and make true the only valid value
   *
   * See https://issues.apache.org/jira/browse/LUCENE-1068
   */
  public void setReplaceInvalidAcronym(boolean replaceInvalidAcronym) {
    this.replaceInvalidAcronym = replaceInvalidAcronym;
  }
}
