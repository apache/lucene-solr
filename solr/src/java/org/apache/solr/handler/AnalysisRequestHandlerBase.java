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
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.index.Payload;
import org.apache.lucene.util.Attribute;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.AttributeReflector;
import org.apache.lucene.util.SorterTemplate;
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
import java.math.BigInteger;

/**
 * A base class for all analysis request handlers.
 *
 * @version $Id$
 * @since solr 1.4
 */
public abstract class AnalysisRequestHandlerBase extends RequestHandlerBase {

  @Override
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

    NamedList<List<NamedList>> namedList = new NamedList<List<NamedList>>();

    if( cfiltfacs != null ){
      String source = value;
      for(CharFilterFactory cfiltfac : cfiltfacs ){
        CharStream reader = CharReader.get(new StringReader(source));
        reader = cfiltfac.create(reader);
        source = writeCharStream(namedList, reader);
      }
    }

    TokenStream tokenStream = tfac.create(tokenizerChain.charStream(new StringReader(value)));
    List<AttributeSource> tokens = analyzeTokenStream(tokenStream);

    namedList.add(tokenStream.getClass().getName(), convertTokensToNamedLists(tokens, context));

    ListBasedTokenStream listBasedTokenStream = new ListBasedTokenStream(tokens);

    for (TokenFilterFactory tokenFilterFactory : filtfacs) {
      tokenStream = tokenFilterFactory.create(listBasedTokenStream);
      List<AttributeSource> tokenList = analyzeTokenStream(tokenStream);
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
   * @deprecated This method is no longer used by Solr
   * @see #getQueryTokenSet
   */
  @Deprecated
  protected List<AttributeSource> analyzeValue(String value, Analyzer analyzer) {
    TokenStream tokenStream = analyzer.tokenStream("", new StringReader(value));
    return analyzeTokenStream(tokenStream);
  }

  /**
   * Analyzes the given text using the given analyzer and returns the produced tokens.
   *
   * @param query    The query to analyze.
   * @param analyzer The analyzer to use.
   */
  protected Set<String> getQueryTokenSet(String query, Analyzer analyzer) {
    final Set<String> tokens = new HashSet<String>();
    final TokenStream tokenStream = analyzer.tokenStream("", new StringReader(query));
    final CharTermAttribute termAtt = tokenStream.addAttribute(CharTermAttribute.class);
    try {
      tokenStream.reset();
      while (tokenStream.incrementToken()) {
        tokens.add(termAtt.toString());
      }
    } catch (IOException ioe) {
      throw new RuntimeException("Error occured while iterating over tokenstream", ioe);
    }
    return tokens;
  }

  /**
   * Analyzes the given TokenStream, collecting the Tokens it produces.
   *
   * @param tokenStream TokenStream to analyze
   *
   * @return List of tokens produced from the TokenStream
   */
  private List<AttributeSource> analyzeTokenStream(TokenStream tokenStream) {
    List<AttributeSource> tokens = new ArrayList<AttributeSource>();
    // for backwards compatibility, add all "common" attributes
    tokenStream.addAttribute(CharTermAttribute.class);
    tokenStream.addAttribute(PositionIncrementAttribute.class);
    tokenStream.addAttribute(OffsetAttribute.class);
    tokenStream.addAttribute(TypeAttribute.class);
    try {
      tokenStream.reset();
      while (tokenStream.incrementToken()) {
        tokens.add(tokenStream.cloneAttributes());
      }
    } catch (IOException ioe) {
      throw new RuntimeException("Error occured while iterating over tokenstream", ioe);
    }

    return tokens;
  }

  // a static mapping of the reflected attribute keys to the names used in Solr 1.4
  static Map<String,String> ATTRIBUTE_MAPPING = Collections.unmodifiableMap(new HashMap<String,String>() {{
    put(OffsetAttribute.class.getName() + "#startOffset", "start");
    put(OffsetAttribute.class.getName() + "#endOffset", "end");
    put(TypeAttribute.class.getName() + "#type", "type");
  }});

  /**
   * Converts the list of Tokens to a list of NamedLists representing the tokens.
   *
   * @param tokens  Tokens to convert
   * @param context The analysis context
   *
   * @return List of NamedLists containing the relevant information taken from the tokens
   */
  private List<NamedList> convertTokensToNamedLists(final List<AttributeSource> tokens, AnalysisContext context) {
    final List<NamedList> tokensNamedLists = new ArrayList<NamedList>();

    final int[] positions = new int[tokens.size()];
    int position = 0;    
    for (int i = 0, c = tokens.size(); i < c; i++) {
      AttributeSource token = tokens.get(i);
      position += token.addAttribute(PositionIncrementAttribute.class).getPositionIncrement();
      positions[i] = position;
    }
    
    // sort the tokens by absoulte position
    new SorterTemplate() {
      @Override
      protected void swap(int i, int j) {
        final int p = positions[i];
        positions[i] = positions[j];
        positions[j] = p;
        Collections.swap(tokens, i, j);
      }
      
      @Override
      protected int compare(int i, int j) {
        return positions[i] - positions[j];
      }

      @Override
      protected void setPivot(int i) {
        pivot = positions[i];
      }
  
      @Override
      protected int comparePivot(int j) {
        return pivot - positions[j];
      }
      
      private int pivot;
    }.mergeSort(0, tokens.size() - 1);

    FieldType fieldType = context.getFieldType();

    for (int i = 0, c = tokens.size(); i < c; i++) {
      AttributeSource token = tokens.get(i);
      final NamedList<Object> tokenNamedList = new SimpleOrderedMap<Object>();
      final String rawText = token.addAttribute(CharTermAttribute.class).toString();

      String text = fieldType.indexedToReadable(rawText);
      tokenNamedList.add("text", text);
      if (!text.equals(rawText)) {
        tokenNamedList.add("raw_text", rawText);
      }

      if (context.getTermsToMatch().contains(rawText)) {
        tokenNamedList.add("match", true);
      }

      tokenNamedList.add("position", positions[i]);

      token.reflectWith(new AttributeReflector() {
        public void reflect(Class<? extends Attribute> attClass, String key, Object value) {
          // leave out position and term
          if (CharTermAttribute.class.isAssignableFrom(attClass))
            return;
          if (PositionIncrementAttribute.class.isAssignableFrom(attClass))
            return;
          
          String k = attClass.getName() + '#' + key;
          
          // map keys for "standard attributes":
          if (ATTRIBUTE_MAPPING.containsKey(k)) {
            k = ATTRIBUTE_MAPPING.get(k);
          }
          
          // TODO: special handling for payloads - move this to ResponseWriter?
          if (value instanceof Payload) {
            Payload p = (Payload) value;
            if( null != p ) {
              BigInteger bi = new BigInteger( p.getData() );
              String ret = bi.toString( 16 );
              if (ret.length() % 2 != 0) {
                // Pad with 0
                ret = "0"+ret;
              }
              value = ret;
            } else { 
              value = null;
            }
          }

          tokenNamedList.add(k, value);
        }
      });

      tokensNamedLists.add(tokenNamedList);
    }

    return tokensNamedLists;
  }
  
  private String writeCharStream(NamedList out, CharStream input ){
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
    private final List<AttributeSource> tokens;
    private Iterator<AttributeSource> tokenIterator;

    /**
     * Creates a new ListBasedTokenStream which uses the given tokens as its token source.
     *
     * @param tokens Source of tokens to be used
     */
    ListBasedTokenStream(List<AttributeSource> tokens) {
      this.tokens = tokens;
      tokenIterator = tokens.iterator();
    }

    @Override
    public boolean incrementToken() throws IOException {
      if (tokenIterator.hasNext()) {
        AttributeSource next = tokenIterator.next();
        Iterator<Class<? extends Attribute>> atts = next.getAttributeClassesIterator();
        while (atts.hasNext()) // make sure all att impls in the token exist here
          addAttribute(atts.next());
        next.copyTo(this);
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
      this(fieldName, fieldType, analyzer, Collections.EMPTY_SET);
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
