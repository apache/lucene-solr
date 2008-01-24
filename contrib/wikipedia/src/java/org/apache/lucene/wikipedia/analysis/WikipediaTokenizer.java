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

import java.io.IOException;
import java.io.Reader;
import java.util.*;


/**
 * Extension of StandardTokenizer that is aware of Wikipedia syntax.  It is based off of the
 * Wikipedia tutorial available at http://en.wikipedia.org/wiki/Wikipedia:Tutorial, but it may not be complete.
 * <p/>
 * <p/>
 * EXPERIMENTAL !!!!!!!!!
 * NOTE: This Tokenizer is considered experimental and the grammar is subject to change in the trunk and in follow up releases.
 */
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

  public static final int TOKENS_ONLY = 0;
  public static final int UNTOKENIZED_ONLY = 1;
  public static final int BOTH = 2;

  public static final int UNTOKENIZED_TOKEN_FLAG = 1;
  /**
   * A private instance of the JFlex-constructed scanner
   */
  private final WikipediaTokenizerImpl scanner;

  private int tokenOutput = TOKENS_ONLY;
  private Set untokenizedTypes = Collections.EMPTY_SET;
  private Iterator tokens = null;

  void setInput(Reader reader) {
    this.input = reader;
  }

  /**
   * Creates a new instance of the {@link WikipediaTokenizer}. Attaches the
   * <code>input</code> to a newly created JFlex scanner.
   *
   * @param input The Input Reader
   */
  public WikipediaTokenizer(Reader input) {
    this(input, TOKENS_ONLY, Collections.EMPTY_SET);
  }


  public WikipediaTokenizer(Reader input, int tokenOutput, Set untokenizedTypes) {
    super(input);
    this.tokenOutput = tokenOutput;
    this.scanner = new WikipediaTokenizerImpl(input);
    this.untokenizedTypes = untokenizedTypes;
  }

  /*
  * (non-Javadoc)
  *
  * @see org.apache.lucene.analysis.TokenStream#next()
  */
  public Token next(Token result) throws IOException {
    if (tokens != null && tokens.hasNext()){
      return (Token)tokens.next();
    }
    int tokenType = scanner.getNextToken();

    if (tokenType == WikipediaTokenizerImpl.YYEOF) {
      return null;
    }
    String type = WikipediaTokenizerImpl.TOKEN_TYPES[tokenType];
    if (tokenOutput == TOKENS_ONLY || untokenizedTypes.contains(type) == false){
      setupToken(result);
    } else if (tokenOutput == UNTOKENIZED_ONLY && untokenizedTypes.contains(type) == true){
      collapseTokens(result, tokenType);

    }
    else if (tokenOutput == BOTH){
      //collapse into a single token, add it to tokens AND output the individual tokens
      //output the untokenized Token first
      collapseAndSaveTokens(result, tokenType, type);
    }
    result.setPositionIncrement(scanner.getPositionIncrement());
    result.setType(type);
    return result;
  }

  private void collapseAndSaveTokens(Token result, int tokenType, String type) throws IOException {
    //collapse
    StringBuffer buffer = new StringBuffer(32);
    int numAdded = scanner.setText(buffer);
    //TODO: how to know how much whitespace to add
    int theStart = scanner.yychar();
    int lastPos = theStart + numAdded;
    int tmpTokType;
    int numSeen = 0;
    List tmp = new ArrayList();
    Token saved = new Token();
    setupSavedToken(saved, 0, type);
    tmp.add(saved);
    //while we can get a token and that token is the same type and we have not transitioned to a new wiki-item of the same type
    while ((tmpTokType = scanner.getNextToken()) != WikipediaTokenizerImpl.YYEOF && tmpTokType == tokenType && scanner.getNumWikiTokensSeen() > numSeen){
      int currPos = scanner.yychar();
      //append whitespace
      for (int i = 0; i < (currPos - lastPos); i++){
        buffer.append(' ');
      }
      numAdded = scanner.setText(buffer);
      saved = new Token();
      setupSavedToken(saved, scanner.getPositionIncrement(), type);
      tmp.add(saved);
      numSeen++;
      lastPos = currPos + numAdded;
    }
    //trim the buffer
    String s = buffer.toString().trim();
    result.setTermBuffer(s.toCharArray(), 0, s.length());
    result.setStartOffset(theStart);
    result.setEndOffset(theStart + s.length());
    result.setFlags(UNTOKENIZED_TOKEN_FLAG);
    //The way the loop is written, we will have proceeded to the next token.  We need to pushback the scanner to lastPos
    if (tmpTokType != WikipediaTokenizerImpl.YYEOF){
      scanner.yypushback(scanner.yylength());
    }
    tokens = tmp.iterator();
  }

  private void setupSavedToken(Token saved, int positionInc, String type){
    setupToken(saved);
    saved.setPositionIncrement(positionInc);
    saved.setType(type);
  }

  private void collapseTokens(Token result, int tokenType) throws IOException {
    //collapse
    StringBuffer buffer = new StringBuffer(32);
    int numAdded = scanner.setText(buffer);
    //TODO: how to know how much whitespace to add
    int theStart = scanner.yychar();
    int lastPos = theStart + numAdded;
    int tmpTokType;
    int numSeen = 0;
    //while we can get a token and that token is the same type and we have not transitioned to a new wiki-item of the same type
    while ((tmpTokType = scanner.getNextToken()) != WikipediaTokenizerImpl.YYEOF && tmpTokType == tokenType && scanner.getNumWikiTokensSeen() > numSeen){
      int currPos = scanner.yychar();
      //append whitespace
      for (int i = 0; i < (currPos - lastPos); i++){
        buffer.append(' ');
      }
      numAdded = scanner.setText(buffer);
      numSeen++;
      lastPos = currPos + numAdded;
    }
    //trim the buffer
    String s = buffer.toString().trim();
    result.setTermBuffer(s.toCharArray(), 0, s.length());
    result.setStartOffset(theStart);
    result.setEndOffset(theStart + s.length());
    result.setFlags(UNTOKENIZED_TOKEN_FLAG);
    //The way the loop is written, we will have proceeded to the next token.  We need to pushback the scanner to lastPos
    if (tmpTokType != WikipediaTokenizerImpl.YYEOF){
      scanner.yypushback(scanner.yylength());
    } else {
      tokens = null;
    }
  }

  private void setupToken(Token result) {
    scanner.getText(result);
    final int start = scanner.yychar();
    result.setStartOffset(start);
    result.setEndOffset(start + result.termLength());
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