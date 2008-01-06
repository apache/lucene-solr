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
 */

public class StandardTokenizer extends Tokenizer {
    /** A private instance of the JFlex-constructed scanner */
    private final StandardTokenizerImpl scanner;
    
  /**
   * Specifies whether deprecated acronyms should be replaced with HOST type.
   * This is false by default to support backward compatibility.
   *<p/>
   * See http://issues.apache.org/jira/browse/LUCENE-1068
   * 
   * @deprecated this should be removed in the next release (3.0).
   */
  private boolean replaceInvalidAcronym = false;
    
  void setInput(Reader reader) {
    this.input = reader;
  }

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
     */
    public StandardTokenizer(Reader input) {
	    this.input = input;
	    this.scanner = new StandardTokenizerImpl(input);
    }

  /**
   * Creates a new instance of the {@link org.apache.lucene.analysis.standard.StandardTokenizer}.  Attaches
   * the <code>input</code> to the newly created JFlex scanner.
   *
   * @param input The input reader
   * @param replaceInvalidAcronym Set to true to replace mischaracterized acronyms with HOST.
   *
   * See http://issues.apache.org/jira/browse/LUCENE-1068
   */
  public StandardTokenizer(Reader input, boolean replaceInvalidAcronym) {
    this.replaceInvalidAcronym = replaceInvalidAcronym;
    this.input = input;
    this.scanner = new StandardTokenizerImpl(input);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lucene.analysis.TokenStream#next()
   */
  public Token next(Token result) throws IOException {
      int posIncr = 1;

      while(true) {
	int tokenType = scanner.getNextToken();

	if (tokenType == StandardTokenizerImpl.YYEOF) {
	    return null;
	}

        if (scanner.yylength() <= maxTokenLength) {
          result.clear();
          result.setPositionIncrement(posIncr);
          scanner.getText(result);
          final int start = scanner.yychar();
          result.setStartOffset(start);
          result.setEndOffset(start+result.termLength());
          // This 'if' should be removed in the next release. For now, it converts
          // invalid acronyms to HOST. When removed, only the 'else' part should
          // remain.
          if (tokenType == StandardTokenizerImpl.ACRONYM_DEP) {
            if (replaceInvalidAcronym) {
              result.setType(StandardTokenizerImpl.TOKEN_TYPES[StandardTokenizerImpl.HOST]);
              result.setTermLength(result.termLength() - 1); // remove extra '.'
            } else {
              result.setType(StandardTokenizerImpl.TOKEN_TYPES[StandardTokenizerImpl.ACRONYM]);
            }
          } else {
            result.setType(StandardTokenizerImpl.TOKEN_TYPES[tokenType]);
          }
          return result;
        } else
          // When we skip a too-long term, we still increment the
          // position increment
          posIncr++;
      }
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
        input = reader;
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
