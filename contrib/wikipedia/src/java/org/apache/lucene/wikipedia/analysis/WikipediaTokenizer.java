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

package org.apache.lucene.wikipedia.analysis;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.Tokenizer;

import java.io.Reader;
import java.io.IOException;


/**
 * Extension of StandardTokenizer that is aware of Wikipedia syntax.  It is based off of the
 * Wikipedia tutorial available at http://en.wikipedia.org/wiki/Wikipedia:Tutorial, but it may not be complete.
 *<p/>
 * EXPERIMENTAL !!!!!!!!!
 * NOTE: This Tokenizer is considered experimental and the grammar is subject to change in the trunk and in follow up releases.
 **/
public class WikipediaTokenizer extends Tokenizer {
  public static final String INTERNAL_LINK = "il";
  public static final String EXTERNAL_LINK = "el";
  //The URL part of the link, i.e. the first token
  public static final String EXTERNAL_LINK_URL = "elu";
  public static final String CITATION = "ci";
  public static final String CATEGORY = "c";
  public static final String BOLD = "b";
  public static final String ITALICS = "i";
  public static final String BOLD_ITALICS = "bi";
  public static final String HEADING = "h";
  public static final String SUB_HEADING = "sh";
  /**
   * A private instance of the JFlex-constructed scanner
   */
  private final WikipediaTokenizerImpl scanner;

  void setInput(Reader reader) {
    this.input = reader;
  }

  /**
   * Creates a new instance of the {@link WikipediaTokenizer}. Attaches the
   * <code>input</code> to a newly created JFlex scanner.
   * @param input The Input Reader
   */
  public WikipediaTokenizer(Reader input) {
    this.input = input;
    this.scanner = new WikipediaTokenizerImpl(input);
  }

  /*
  * (non-Javadoc)
  *
  * @see org.apache.lucene.analysis.TokenStream#next()
  */
  public Token next(Token result) throws IOException {
    int tokenType = scanner.getNextToken();

    if (tokenType == WikipediaTokenizerImpl.YYEOF) {
      return null;
    }

    scanner.getText(result, tokenType);
    final int start = scanner.yychar();
    result.setStartOffset(start);
    result.setEndOffset(start + result.termLength());
    result.setPositionIncrement(scanner.getPositionIncrement());
    result.setType(WikipediaTokenizerImpl.TOKEN_TYPES[tokenType]);
    return result;
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

}