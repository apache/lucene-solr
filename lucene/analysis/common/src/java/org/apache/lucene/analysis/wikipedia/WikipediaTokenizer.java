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
package org.apache.lucene.analysis.wikipedia;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.FlagsAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.AttributeFactory;
import org.apache.lucene.util.AttributeSource;

import java.io.IOException;
import java.util.*;


/**
 * Extension of StandardTokenizer that is aware of Wikipedia syntax.  It is based off of the
 * Wikipedia tutorial available at http://en.wikipedia.org/wiki/Wikipedia:Tutorial, but it may not be complete.
 * @lucene.experimental
 */
public final class WikipediaTokenizer extends Tokenizer {
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

  public static final int ALPHANUM_ID          = 0;
  public static final int APOSTROPHE_ID        = 1;
  public static final int ACRONYM_ID           = 2;
  public static final int COMPANY_ID           = 3;
  public static final int EMAIL_ID             = 4;
  public static final int HOST_ID              = 5;
  public static final int NUM_ID               = 6;
  public static final int CJ_ID                = 7;
  public static final int INTERNAL_LINK_ID     = 8;
  public static final int EXTERNAL_LINK_ID     = 9;
  public static final int CITATION_ID          = 10;
  public static final int CATEGORY_ID          = 11;
  public static final int BOLD_ID              = 12;
  public static final int ITALICS_ID           = 13;
  public static final int BOLD_ITALICS_ID      = 14;
  public static final int HEADING_ID           = 15;
  public static final int SUB_HEADING_ID       = 16;
  public static final int EXTERNAL_LINK_URL_ID = 17;

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
    INTERNAL_LINK,
    EXTERNAL_LINK,
    CITATION,
    CATEGORY,
    BOLD,
    ITALICS,
    BOLD_ITALICS,
    HEADING,
    SUB_HEADING,
    EXTERNAL_LINK_URL
  };

  /**
   * Only output tokens
   */
  public static final int TOKENS_ONLY = 0;
  /**
   * Only output untokenized tokens, which are tokens that would normally be split into several tokens
   */
  public static final int UNTOKENIZED_ONLY = 1;
  /**
   * Output the both the untokenized token and the splits
   */
  public static final int BOTH = 2;
  /**
   * This flag is used to indicate that the produced "Token" would, if {@link #TOKENS_ONLY} was used, produce multiple tokens.
   */
  public static final int UNTOKENIZED_TOKEN_FLAG = 1;
  /**
   * A private instance of the JFlex-constructed scanner
   */
  private final WikipediaTokenizerImpl scanner;

  private int tokenOutput = TOKENS_ONLY;
  private Set<String> untokenizedTypes = Collections.emptySet();
  private Iterator<AttributeSource.State> tokens = null;
  
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);
  private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final FlagsAttribute flagsAtt = addAttribute(FlagsAttribute.class);
  
  private boolean first;

  /**
   * Creates a new instance of the {@link WikipediaTokenizer}. Attaches the
   * <code>input</code> to a newly created JFlex scanner.
   */
  public WikipediaTokenizer() {
    this(TOKENS_ONLY, Collections.<String>emptySet());
  }

  /**
   * Creates a new instance of the {@link org.apache.lucene.analysis.wikipedia.WikipediaTokenizer}.  Attaches the
   * <code>input</code> to the newly created JFlex scanner.
   *
   * @param tokenOutput One of {@link #TOKENS_ONLY}, {@link #UNTOKENIZED_ONLY}, {@link #BOTH}
   */
  public WikipediaTokenizer(int tokenOutput, Set<String> untokenizedTypes) {
    this.scanner = new WikipediaTokenizerImpl(this.input);
    init(tokenOutput, untokenizedTypes);
  }

  /**
   * Creates a new instance of the {@link org.apache.lucene.analysis.wikipedia.WikipediaTokenizer}.  Attaches the
   * <code>input</code> to the newly created JFlex scanner. Uses the given {@link org.apache.lucene.util.AttributeFactory}.
   *
   * @param tokenOutput One of {@link #TOKENS_ONLY}, {@link #UNTOKENIZED_ONLY}, {@link #BOTH}
   */
  public WikipediaTokenizer(AttributeFactory factory, int tokenOutput, Set<String> untokenizedTypes) {
    super(factory);
    this.scanner = new WikipediaTokenizerImpl(this.input);
    init(tokenOutput, untokenizedTypes);
  }
  
  private void init(int tokenOutput, Set<String> untokenizedTypes) {
    // TODO: cutover to enum
    if (tokenOutput != TOKENS_ONLY &&
        tokenOutput != UNTOKENIZED_ONLY &&
        tokenOutput != BOTH) {
      throw new IllegalArgumentException("tokenOutput must be TOKENS_ONLY, UNTOKENIZED_ONLY or BOTH");
    }
    this.tokenOutput = tokenOutput;
    this.untokenizedTypes = untokenizedTypes;    
  }
  
  /*
  * (non-Javadoc)
  *
  * @see org.apache.lucene.analysis.TokenStream#next()
  */
  @Override
  public final boolean incrementToken() throws IOException {
    if (tokens != null && tokens.hasNext()){
      AttributeSource.State state = tokens.next();
      restoreState(state);
      return true;
    }
    clearAttributes();
    int tokenType = scanner.getNextToken();

    if (tokenType == WikipediaTokenizerImpl.YYEOF) {
      return false;
    }
    String type = WikipediaTokenizerImpl.TOKEN_TYPES[tokenType];
    if (tokenOutput == TOKENS_ONLY || untokenizedTypes.contains(type) == false){
      setupToken();
    } else if (tokenOutput == UNTOKENIZED_ONLY && untokenizedTypes.contains(type) == true){
      collapseTokens(tokenType);

    }
    else if (tokenOutput == BOTH){
      //collapse into a single token, add it to tokens AND output the individual tokens
      //output the untokenized Token first
      collapseAndSaveTokens(tokenType, type);
    }
    int posinc = scanner.getPositionIncrement();
    if (first && posinc == 0) {
      posinc = 1; // don't emit posinc=0 for the first token!
    }
    posIncrAtt.setPositionIncrement(posinc);
    typeAtt.setType(type);
    first = false;
    return true;
  }

  private void collapseAndSaveTokens(int tokenType, String type) throws IOException {
    //collapse
    StringBuilder buffer = new StringBuilder(32);
    int numAdded = scanner.setText(buffer);
    //TODO: how to know how much whitespace to add
    int theStart = scanner.yychar();
    int lastPos = theStart + numAdded;
    int tmpTokType;
    int numSeen = 0;
    List<AttributeSource.State> tmp = new ArrayList<>();
    setupSavedToken(0, type);
    tmp.add(captureState());
    //while we can get a token and that token is the same type and we have not transitioned to a new wiki-item of the same type
    while ((tmpTokType = scanner.getNextToken()) != WikipediaTokenizerImpl.YYEOF && tmpTokType == tokenType && scanner.getNumWikiTokensSeen() > numSeen){
      int currPos = scanner.yychar();
      //append whitespace
      for (int i = 0; i < (currPos - lastPos); i++){
        buffer.append(' ');
      }
      numAdded = scanner.setText(buffer);
      setupSavedToken(scanner.getPositionIncrement(), type);
      tmp.add(captureState());
      numSeen++;
      lastPos = currPos + numAdded;
    }
    //trim the buffer
    // TODO: this is inefficient
    String s = buffer.toString().trim();
    termAtt.setEmpty().append(s);
    offsetAtt.setOffset(correctOffset(theStart), correctOffset(theStart + s.length()));
    flagsAtt.setFlags(UNTOKENIZED_TOKEN_FLAG);
    //The way the loop is written, we will have proceeded to the next token.  We need to pushback the scanner to lastPos
    if (tmpTokType != WikipediaTokenizerImpl.YYEOF){
      scanner.yypushback(scanner.yylength());
    }
    tokens = tmp.iterator();
  }

  private void setupSavedToken(int positionInc, String type){
    setupToken();
    posIncrAtt.setPositionIncrement(positionInc);
    typeAtt.setType(type);
  }

  private void collapseTokens(int tokenType) throws IOException {
    //collapse
    StringBuilder buffer = new StringBuilder(32);
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
    // TODO: this is inefficient
    String s = buffer.toString().trim();
    termAtt.setEmpty().append(s);
    offsetAtt.setOffset(correctOffset(theStart), correctOffset(theStart + s.length()));
    flagsAtt.setFlags(UNTOKENIZED_TOKEN_FLAG);
    //The way the loop is written, we will have proceeded to the next token.  We need to pushback the scanner to lastPos
    if (tmpTokType != WikipediaTokenizerImpl.YYEOF){
      scanner.yypushback(scanner.yylength());
    } else {
      tokens = null;
    }
  }

  private void setupToken() {
    scanner.getText(termAtt);
    final int start = scanner.yychar();
    offsetAtt.setOffset(correctOffset(start), correctOffset(start + termAtt.length()));
  }

  @Override
  public void close() throws IOException {
    super.close();
    scanner.yyreset(input);
  }

  /*
  * (non-Javadoc)
  *
  * @see org.apache.lucene.analysis.TokenStream#reset()
  */
  @Override
  public void reset() throws IOException {
    super.reset();
    scanner.yyreset(input);
    tokens = null;
    scanner.reset();
    first = true;
  }

  @Override
  public void end() throws IOException {
    super.end();
    // set final offset
    final int finalOffset = correctOffset(scanner.yychar() + scanner.yylength());
    this.offsetAtt.setOffset(finalOffset, finalOffset);
  }
}
