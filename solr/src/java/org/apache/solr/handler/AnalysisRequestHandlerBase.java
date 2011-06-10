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
import org.apache.lucene.analysis.tokenattributes.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.index.Payload;
import org.apache.lucene.util.Attribute;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.AttributeReflector;
import org.apache.lucene.util.CharsRef;
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

/**
 * A base class for all analysis request handlers.
 *
 *
 * @since solr 1.4
 */
public abstract class AnalysisRequestHandlerBase extends RequestHandlerBase {

  public static final Set<BytesRef> EMPTY_BYTES_SET = Collections.emptySet();

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
   * @param query    The query to analyze.
   * @param analyzer The analyzer to use.
   */
  protected Set<BytesRef> getQueryTokenSet(String query, Analyzer analyzer) {
    final Set<BytesRef> tokens = new HashSet<BytesRef>();
    final TokenStream tokenStream = analyzer.tokenStream("", new StringReader(query));
    final TermToBytesRefAttribute bytesAtt = tokenStream.getAttribute(TermToBytesRefAttribute.class);
    final BytesRef bytes = bytesAtt.getBytesRef();
    try {
      tokenStream.reset();
      while (tokenStream.incrementToken()) {
        bytesAtt.fillBytesRef();
        tokens.add(new BytesRef(bytes));
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
    tokenStream.addAttribute(PositionIncrementAttribute.class);
    tokenStream.addAttribute(OffsetAttribute.class);
    tokenStream.addAttribute(TypeAttribute.class);
    final BytesRef bytes = new BytesRef();
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
      final TermToBytesRefAttribute termAtt = token.getAttribute(TermToBytesRefAttribute.class);
      BytesRef rawBytes = termAtt.getBytesRef();
      termAtt.fillBytesRef();
      final String text = fieldType.indexedToReadable(rawBytes, new CharsRef(rawBytes.length)).toString();
      tokenNamedList.add("text", text);
      
      if (token.hasAttribute(CharTermAttribute.class)) {
        final String rawText = token.getAttribute(CharTermAttribute.class).toString();
        if (!rawText.equals(text)) {
          tokenNamedList.add("raw_text", rawText);
        }
      }

      tokenNamedList.add("raw_bytes", rawBytes.toString());

      if (context.getTermsToMatch().contains(rawBytes)) {
        tokenNamedList.add("match", true);
      }

      tokenNamedList.add("position", positions[i]);

      token.reflectWith(new AttributeReflector() {
        public void reflect(Class<? extends Attribute> attClass, String key, Object value) {
          // leave out position and bytes term
          if (TermToBytesRefAttribute.class.isAssignableFrom(attClass))
            return;
          if (CharTermAttribute.class.isAssignableFrom(attClass))
            return;
          if (PositionIncrementAttribute.class.isAssignableFrom(attClass))
            return;
          
          String k = attClass.getName() + '#' + key;
          
          // map keys for "standard attributes":
          if (ATTRIBUTE_MAPPING.containsKey(k)) {
            k = ATTRIBUTE_MAPPING.get(k);
          }
          
          if (value instanceof Payload) {
            final Payload p = (Payload) value;
            value = new BytesRef(p.getData()).toString();
          }

          tokenNamedList.add(k, value);
        }
      });

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
        clearAttributes();
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
    private final Set<BytesRef> termsToMatch;

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
    public AnalysisContext(FieldType fieldType, Analyzer analyzer, Set<BytesRef> termsToMatch) {
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
      this(fieldName, fieldType, analyzer, EMPTY_BYTES_SET);
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
    public AnalysisContext(String fieldName, FieldType fieldType, Analyzer analyzer, Set<BytesRef> termsToMatch) {
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

    public Set<BytesRef> getTermsToMatch() {
      return termsToMatch;
    }
  }
}
