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

package org.apache.solr.handler;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharReader;
import org.apache.lucene.analysis.CharStream;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.*;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.analysis.CharFilterFactory;
import org.apache.solr.analysis.TokenFilterFactory;
import org.apache.solr.analysis.TokenizerChain;
import org.apache.solr.analysis.TokenizerFactory;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.SolrException;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.FieldType;

import java.io.IOException;
import java.io.StringReader;
import java.util.*;

/**
 * A base class for all analysis request handlers.
 *
 * @version $Id$
 * @since solr 1.4
 */
public abstract class AnalysisRequestHandlerBase extends RequestHandlerBase {

  public static final Set<String> EMPTY_STRING_SET = Collections.emptySet();

  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    rsp.add("analysis", doAnalysis(req));
  }

  /**
   * Performs the analysis based on the given solr request and returns the analysis result as a named list.
   *
   * @param req The solr request.
   *
   * @return The analysis result as a named list.
   *
   * @throws Exception When analysis fails.
   */
  protected abstract NamedList doAnalysis(SolrQueryRequest req) throws Exception;

  /**
   * Analyzes the given value using the given Analyzer.
   *
   * @param value   Value to analyze
   * @param context The {@link AnalysisContext analysis context}.
   *
   * @return NamedList containing the tokens produced by analyzing the given value
   */
  protected NamedList<? extends Object> analyzeValue(String value, AnalysisContext context) {

    Analyzer analyzer = context.getAnalyzer();

    if (!TokenizerChain.class.isInstance(analyzer)) {

      TokenStream tokenStream = null;
      try {
        tokenStream = analyzer.reusableTokenStream(context.getFieldName(), new StringReader(value));
        tokenStream.reset();
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
      }
      NamedList<List<NamedList>> namedList = new NamedList<List<NamedList>>();
      namedList.add(tokenStream.getClass().getName(), convertTokensToNamedLists(analyzeTokenStream(tokenStream), context));
      return namedList;
    }

    TokenizerChain tokenizerChain = (TokenizerChain) analyzer;
    CharFilterFactory[] cfiltfacs = tokenizerChain.getCharFilterFactories();
    TokenizerFactory tfac = tokenizerChain.getTokenizerFactory();
    TokenFilterFactory[] filtfacs = tokenizerChain.getTokenFilterFactories();

    NamedList<Object> namedList = new NamedList<Object>();

    if( cfiltfacs != null ){
      String source = value;
      for(CharFilterFactory cfiltfac : cfiltfacs ){
        CharStream reader = CharReader.get(new StringReader(source));
        reader = cfiltfac.create(reader);
        source = writeCharStream(namedList, reader);
      }
    }

    TokenStream tokenStream = tfac.create(tokenizerChain.charStream(new StringReader(value)));
    List<Token> tokens = analyzeTokenStream(tokenStream);

    namedList.add(tokenStream.getClass().getName(), convertTokensToNamedLists(tokens, context));

    ListBasedTokenStream listBasedTokenStream = new ListBasedTokenStream(tokens);

    for (TokenFilterFactory tokenFilterFactory : filtfacs) {
      tokenStream = tokenFilterFactory.create(listBasedTokenStream);
      List<Token> tokenList = analyzeTokenStream(tokenStream);
      namedList.add(tokenStream.getClass().getName(), convertTokensToNamedLists(tokenList, context));
      listBasedTokenStream = new ListBasedTokenStream(tokenList);
    }

    return namedList;
  }

  /**
   * Analyzes the given text using the given analyzer and returns the produced tokens.
   *
   * @param value    The value to analyze.
   * @param analyzer The analyzer to use.
   *
   * @return The produces token list.
   */
  protected List<Token> analyzeValue(String value, Analyzer analyzer) {
    TokenStream tokenStream = analyzer.tokenStream("", new StringReader(value));
    return analyzeTokenStream(tokenStream);
  }

  /**
   * Analyzes the given TokenStream, collecting the Tokens it produces.
   *
   * @param tokenStream TokenStream to analyze
   *
   * @return List of tokens produced from the TokenStream
   */
  private List<Token> analyzeTokenStream(TokenStream tokenStream) {
    List<Token> tokens = new ArrayList<Token>();
    
    // TODO change this API to support custom attributes
    CharTermAttribute termAtt = null;
    TermToBytesRefAttribute bytesAtt = null;
    if (tokenStream.hasAttribute(CharTermAttribute.class)) {
      termAtt = tokenStream.getAttribute(CharTermAttribute.class);
    } else if (tokenStream.hasAttribute(TermToBytesRefAttribute.class)) {
      bytesAtt = tokenStream.getAttribute(TermToBytesRefAttribute.class);
    }
    final OffsetAttribute offsetAtt = tokenStream.addAttribute(OffsetAttribute.class);
    final TypeAttribute typeAtt = tokenStream.addAttribute(TypeAttribute.class);
    final PositionIncrementAttribute posIncAtt = tokenStream.addAttribute(PositionIncrementAttribute.class);
    final FlagsAttribute flagsAtt = tokenStream.addAttribute(FlagsAttribute.class);
    final PayloadAttribute payloadAtt = tokenStream.addAttribute(PayloadAttribute.class);
    
    final BytesRef bytes = new BytesRef();
    try {
      while (tokenStream.incrementToken()) {
        Token token = new Token();
        if (termAtt != null) {
          token.setEmpty().append(termAtt);
        }
        if (bytesAtt != null) {
          bytesAtt.toBytesRef(bytes);
          // TODO: This is incorrect when numeric fields change in later lucene versions. It should use BytesRef directly!
          token.setEmpty().append(bytes.utf8ToString());
        }
        token.setOffset(offsetAtt.startOffset(), offsetAtt.endOffset());
        token.setType(typeAtt.type());
        token.setFlags(flagsAtt.getFlags());
        token.setPayload(payloadAtt.getPayload());
        token.setPositionIncrement(posIncAtt.getPositionIncrement());
        tokens.add((Token) token.clone());
      }
    } catch (IOException ioe) {
      throw new RuntimeException("Error occured while iterating over tokenstream", ioe);
    }

    return tokens;
  }

  /**
   * Converts the list of Tokens to a list of NamedLists representing the tokens.
   *
   * @param tokens  Tokens to convert
   * @param context The analysis context
   *
   * @return List of NamedLists containing the relevant information taken from the tokens
   */
  private List<NamedList> convertTokensToNamedLists(List<Token> tokens, AnalysisContext context) {
    List<NamedList> tokensNamedLists = new ArrayList<NamedList>();

    Collections.sort(tokens, new Comparator<Token>() {
      public int compare(Token o1, Token o2) {
        return o1.endOffset() - o2.endOffset();
      }
    });

    int position = 0;

    FieldType fieldType = context.getFieldType();

    for (Token token : tokens) {
      NamedList<Object> tokenNamedList = new SimpleOrderedMap<Object>();

      String text = fieldType.indexedToReadable(token.toString());
      tokenNamedList.add("text", text);
      if (!text.equals(token.toString())) {
        tokenNamedList.add("raw_text", token.toString());
      }
      tokenNamedList.add("type", token.type());
      tokenNamedList.add("start", token.startOffset());
      tokenNamedList.add("end", token.endOffset());

      position += token.getPositionIncrement();
      tokenNamedList.add("position", position);

      if (context.getTermsToMatch().contains(token.toString())) {
        tokenNamedList.add("match", true);
      }

      if (token.getPayload() != null) {
        tokenNamedList.add("payload", token.getPayload());
      }

      tokensNamedLists.add(tokenNamedList);
    }

    return tokensNamedLists;
  }
  
  private String writeCharStream(NamedList<Object> out, CharStream input ){
    final int BUFFER_SIZE = 1024;
    char[] buf = new char[BUFFER_SIZE];
    int len = 0;
    StringBuilder sb = new StringBuilder();
    do {
      try {
        len = input.read( buf, 0, BUFFER_SIZE );
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
      }
      if( len > 0 )
        sb.append(buf, 0, len);
    } while( len == BUFFER_SIZE );
    out.add( input.getClass().getName(), sb.toString());
    return sb.toString();
  }

  // ================================================= Inner classes =================================================

  /**
   * TokenStream that iterates over a list of pre-existing Tokens
   */
  // TODO refactor to support custom attributes
  protected final static class ListBasedTokenStream extends TokenStream {
    private final List<Token> tokens;
    private Iterator<Token> tokenIterator;

    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
    private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);
    private final FlagsAttribute flagsAtt = addAttribute(FlagsAttribute.class);
    private final PayloadAttribute payloadAtt = addAttribute(PayloadAttribute.class);
    private final PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);
    /**
     * Creates a new ListBasedTokenStream which uses the given tokens as its token source.
     *
     * @param tokens Source of tokens to be used
     */
    ListBasedTokenStream(List<Token> tokens) {
      this.tokens = tokens;
      tokenIterator = tokens.iterator();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean incrementToken() throws IOException {
      if (tokenIterator.hasNext()) {
        Token next = tokenIterator.next();
        termAtt.copyBuffer(next.buffer(), 0, next.length());
        typeAtt.setType(next.type());
        offsetAtt.setOffset(next.startOffset(), next.endOffset());
        flagsAtt.setFlags(next.getFlags());
        payloadAtt.setPayload(next.getPayload());
        posIncAtt.setPositionIncrement(next.getPositionIncrement());
        return true;
      } else {
        return false;
      }
    }

    @Override
    public void reset() throws IOException {
      super.reset();
      tokenIterator = tokens.iterator();
    }
  }

  /**
   * Serves as the context of an analysis process. This context contains the following constructs
   */
  protected static class AnalysisContext {

    private final String fieldName;
    private final FieldType fieldType;
    private final Analyzer analyzer;
    private final Set<String> termsToMatch;

    /**
     * Constructs a new AnalysisContext with a given field tpe, analyzer and 
     * termsToMatch. By default the field name in this context will be 
     * {@code null}. During the analysis processs, The produced tokens will 
     * be compaired to the terms in the {@code termsToMatch} set. When found, 
     * these tokens will be marked as a match.
     *
     * @param fieldType    The type of the field the analysis is performed on.
     * @param analyzer     The analyzer to be used.
     * @param termsToMatch Holds all the terms that should match during the 
     *                     analysis process.
     */
    public AnalysisContext(FieldType fieldType, Analyzer analyzer, Set<String> termsToMatch) {
      this(null, fieldType, analyzer, termsToMatch);
    }

    /**
     * Constructs an AnalysisContext with a given field name, field type 
     * and analyzer. By default this context will hold no terms to match
     *
     * @param fieldName The name of the field the analysis is performed on 
     *                  (may be {@code null}).
     * @param fieldType The type of the field the analysis is performed on.
     * @param analyzer  The analyzer to be used during the analysis process.
     *
     */
    public AnalysisContext(String fieldName, FieldType fieldType, Analyzer analyzer) {
      this(fieldName, fieldType, analyzer, EMPTY_STRING_SET);
    }

    /**
     * Constructs a new AnalysisContext with a given field tpe, analyzer and
     * termsToMatch. During the analysis processs, The produced tokens will be 
     * compaired to the termes in the {@code termsToMatch} set. When found, 
     * these tokens will be marked as a match.
     *
     * @param fieldName    The name of the field the analysis is performed on 
     *                     (may be {@code null}).
     * @param fieldType    The type of the field the analysis is performed on.
     * @param analyzer     The analyzer to be used.
     * @param termsToMatch Holds all the terms that should match during the 
     *                     analysis process.
     */
    public AnalysisContext(String fieldName, FieldType fieldType, Analyzer analyzer, Set<String> termsToMatch) {
      this.fieldName = fieldName;
      this.fieldType = fieldType;
      this.analyzer = analyzer;
      this.termsToMatch = termsToMatch;
    }

    public String getFieldName() {
      return fieldName;
    }

    public FieldType getFieldType() {
      return fieldType;
    }

    public Analyzer getAnalyzer() {
      return analyzer;
    }

    public Set<String> getTermsToMatch() {
      return termsToMatch;
    }
  }
}
