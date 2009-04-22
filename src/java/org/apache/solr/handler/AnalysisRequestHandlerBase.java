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
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.solr.analysis.TokenFilterFactory;
import org.apache.solr.analysis.TokenizerChain;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryResponse;
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
  protected NamedList<List<NamedList>> analyzeValue(String value, AnalysisContext context) {

    Analyzer analyzer = context.getAnalyzer();

    if (!TokenizerChain.class.isInstance(analyzer)) {
      TokenStream tokenStream = analyzer.tokenStream(context.getFieldName(), new StringReader(value));
      NamedList<List<NamedList>> namedList = new SimpleOrderedMap<List<NamedList>>();
      namedList.add(tokenStream.getClass().getName(), convertTokensToNamedLists(analyzeTokenStream(tokenStream), context));
      return namedList;
    }

    TokenizerChain tokenizerChain = (TokenizerChain) analyzer;

    NamedList<List<NamedList>> namedList = new SimpleOrderedMap<List<NamedList>>();

    TokenStream tokenStream = tokenizerChain.getTokenizerFactory().create(new StringReader(value));
    List<Token> tokens = analyzeTokenStream(tokenStream);

    namedList.add(tokenStream.getClass().getName(), convertTokensToNamedLists(tokens, context));

    ListBasedTokenStream listBasedTokenStream = new ListBasedTokenStream(tokens);

    for (TokenFilterFactory tokenFilterFactory : tokenizerChain.getTokenFilterFactories()) {
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
    Token reusableToken = new Token();
    Token token = null;

    try {
      while ((token = tokenStream.next(reusableToken)) != null) {
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

      String text = fieldType.indexedToReadable(token.term());
      tokenNamedList.add("text", text);
      if (!text.equals(token.term())) {
        tokenNamedList.add("raw_text", token.term());
      }
      tokenNamedList.add("type", token.type());
      tokenNamedList.add("start", token.startOffset());
      tokenNamedList.add("end", token.endOffset());

      position += token.getPositionIncrement();
      tokenNamedList.add("position", position);

      if (context.getTermsToMatch().contains(token.term())) {
        tokenNamedList.add("match", true);
      }

      if (token.getPayload() != null) {
        tokenNamedList.add("payload", token.getPayload());
      }

      tokensNamedLists.add(tokenNamedList);
    }

    return tokensNamedLists;
  }


  // ================================================= Inner classes =================================================

  /**
   * TokenStream that iterates over a list of pre-existing Tokens
   */
  protected static class ListBasedTokenStream extends TokenStream {

    private final Iterator<Token> tokenIterator;

    /**
     * Creates a new ListBasedTokenStream which uses the given tokens as its token source.
     *
     * @param tokens Source of tokens to be used
     */
    ListBasedTokenStream(List<Token> tokens) {
      tokenIterator = tokens.iterator();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Token next(Token token) throws IOException {
      return (tokenIterator.hasNext()) ? tokenIterator.next() : null;
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
     * Constructs a new AnalysisContext with a given field tpe, analyzer and termsToMatch. By default the field name in
     * this context will be {@code null}. During the analysis processs, The produced tokens will be compaired to the
     * termes in the {@code termsToMatch} set. When found, these tokens will be marked as a match.
     *
     * @param fieldType    The type of the field the analysis is performed on.
     * @param analyzer     The analyzer to be used.
     * @param termsToMatch Holds all the terms that should match during the analysis process.
     */
    public AnalysisContext(FieldType fieldType, Analyzer analyzer, Set<String> termsToMatch) {
      this(null, fieldType, analyzer, termsToMatch);
    }

    /**
     * Constructs an AnalysisContext with a given field name, field type and analyzer. By default this context will hold
     * no terms to match
     *
     * @param fieldName The name of the field the analysis is performed on (may be {@code nuill}).
     * @param fieldType The type of the field the analysis is performed on.
     * @param analyzer  The analyzer to be used during the analysis process.
     *
     * @see #AnalysisContext(String, org.apache.solr.schema.FieldType, org.apache.lucene.analysis.Analyzer,
     *      java.util.Set)
     */
    public AnalysisContext(String fieldName, FieldType fieldType, Analyzer analyzer) {
      this(fieldName, fieldType, analyzer, Collections.EMPTY_SET);
    }

    /**
     * Constructs a new AnalysisContext with a given field tpe, analyzer and termsToMatch. During the analysis processs,
     * The produced tokens will be compaired to the termes in the {@codce termsToMatch} set. When found, these tokens
     * will be marked as a match.
     *
     * @param fieldName    The name of the field the analysis is performed on (may be {@code null}).
     * @param fieldType    The type of the field the analysis is performed on.
     * @param analyzer     The analyzer to be used.
     * @param termsToMatch Holds all the terms that should match during the analysis process.
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
