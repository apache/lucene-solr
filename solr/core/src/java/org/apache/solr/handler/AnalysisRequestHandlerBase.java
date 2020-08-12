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
package org.apache.solr.handler;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.BytesTermAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.analysis.util.CharFilterFactory;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Attribute;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.AttributeReflector;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.analysis.TokenizerChain;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.FieldType;

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
  @SuppressWarnings({"rawtypes"})
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

      try (TokenStream tokenStream = analyzer.tokenStream(context.getFieldName(), value)) {
        @SuppressWarnings({"rawtypes"})
        NamedList<List<NamedList>> namedList = new NamedList<>();
        namedList.add(tokenStream.getClass().getName(), convertTokensToNamedLists(analyzeTokenStream(tokenStream), context));
        return namedList;
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
      }
    }

    TokenizerChain tokenizerChain = (TokenizerChain) analyzer;
    CharFilterFactory[] cfiltfacs = tokenizerChain.getCharFilterFactories();
    TokenizerFactory tfac = tokenizerChain.getTokenizerFactory();
    TokenFilterFactory[] filtfacs = tokenizerChain.getTokenFilterFactories();

    NamedList<Object> namedList = new NamedList<>();

    if (0 < cfiltfacs.length) {
      String source = value;
      for(CharFilterFactory cfiltfac : cfiltfacs ){
        try (Reader sreader = new StringReader(source);
             Reader reader = cfiltfac.create(sreader)) {
          source = writeCharStream(namedList, reader);
        } catch (IOException e) {
          // do nothing.
        }
      }
    }

    TokenStream tokenStream = tfac.create();
    ((Tokenizer)tokenStream).setReader(tokenizerChain.initReader(null, new StringReader(value)));
    List<AttributeSource> tokens = analyzeTokenStream(tokenStream);

    namedList.add(tokenStream.getClass().getName(), convertTokensToNamedLists(tokens, context));

    ListBasedTokenStream listBasedTokenStream = new ListBasedTokenStream(tokenStream, tokens);

    for (TokenFilterFactory tokenFilterFactory : filtfacs) {
      for (final AttributeSource tok : tokens) {
        tok.getAttribute(TokenTrackingAttribute.class).freezeStage();
      }
      // overwrite the vars "tokenStream", "tokens", and "listBasedTokenStream"
      tokenStream = tokenFilterFactory.create(listBasedTokenStream);
      tokens = analyzeTokenStream(tokenStream);
      namedList.add(tokenStream.getClass().getName(), convertTokensToNamedLists(tokens, context));
      try {
        listBasedTokenStream.close();
      } catch (IOException e) {
        // do nothing;
      }
      listBasedTokenStream = new ListBasedTokenStream(listBasedTokenStream, tokens);
    }

    try {
      listBasedTokenStream.close();
    } catch (IOException e) {
      // do nothing.
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
    try (TokenStream tokenStream = analyzer.tokenStream("", query)){
      final Set<BytesRef> tokens = new HashSet<>();
      final TermToBytesRefAttribute bytesAtt = tokenStream.getAttribute(TermToBytesRefAttribute.class);

      tokenStream.reset();

      while (tokenStream.incrementToken()) {
        tokens.add(BytesRef.deepCopyOf(bytesAtt.getBytesRef()));
      }

      tokenStream.end();
      return tokens;
    } catch (IOException ioe) {
      throw new RuntimeException("Error occurred while iterating over tokenstream", ioe);
    }
  }

  /**
   * Analyzes the given TokenStream, collecting the Tokens it produces.
   *
   * @param tokenStream TokenStream to analyze
   *
   * @return List of tokens produced from the TokenStream
   */
  private List<AttributeSource> analyzeTokenStream(TokenStream tokenStream) {
    final List<AttributeSource> tokens = new ArrayList<>();
    final PositionIncrementAttribute posIncrAtt = tokenStream.addAttribute(PositionIncrementAttribute.class);
    final TokenTrackingAttribute trackerAtt = tokenStream.addAttribute(TokenTrackingAttribute.class);
    // for backwards compatibility, add all "common" attributes
    tokenStream.addAttribute(OffsetAttribute.class);
    tokenStream.addAttribute(TypeAttribute.class);
    try {
      tokenStream.reset();
      int position = 0;
      while (tokenStream.incrementToken()) {
        position += posIncrAtt.getPositionIncrement();
        trackerAtt.setActPosition(position);
        tokens.add(tokenStream.cloneAttributes());
      }
      tokenStream.end(); // TODO should we capture?
    } catch (IOException ioe) {
      throw new RuntimeException("Error occurred while iterating over tokenstream", ioe);
    } finally {
      IOUtils.closeWhileHandlingException(tokenStream);
    }

    return tokens;
  }

  // a static mapping of the reflected attribute keys to the names used in Solr 1.4
  static Map<String,String> ATTRIBUTE_MAPPING = Collections.unmodifiableMap(new HashMap<String,String>() {{
    put(OffsetAttribute.class.getName() + "#startOffset", "start");
    put(OffsetAttribute.class.getName() + "#endOffset", "end");
    put(TypeAttribute.class.getName() + "#type", "type");
    put(TokenTrackingAttribute.class.getName() + "#position", "position");
    put(TokenTrackingAttribute.class.getName() + "#positionHistory", "positionHistory");
  }});

  /**
   * Converts the list of Tokens to a list of NamedLists representing the tokens.
   *
   * @param tokenList  Tokens to convert
   * @param context The analysis context
   *
   * @return List of NamedLists containing the relevant information taken from the tokens
   */
  @SuppressWarnings({"rawtypes"})
  private List<NamedList> convertTokensToNamedLists(final List<AttributeSource> tokenList, AnalysisContext context) {
    final List<NamedList> tokensNamedLists = new ArrayList<>();
    final FieldType fieldType = context.getFieldType();
    final AttributeSource[] tokens = tokenList.toArray(new AttributeSource[tokenList.size()]);
    
    // sort the tokens by absolute position
    ArrayUtil.timSort(tokens, new Comparator<AttributeSource>() {
      @Override
      public int compare(AttributeSource a, AttributeSource b) {
        return arrayCompare(
          a.getAttribute(TokenTrackingAttribute.class).getPositions(),
          b.getAttribute(TokenTrackingAttribute.class).getPositions()
        );
      }
      
      private int arrayCompare(int[] a, int[] b) {
        int p = 0;
        final int stop = Math.min(a.length, b.length);
        while(p < stop) {
          int diff = a[p] - b[p];
          if (diff != 0) return diff;
          p++;
        }
        // One is a prefix of the other, or, they are equal:
        return a.length - b.length;
      }
    });

    for (int i = 0; i < tokens.length; i++) {
      AttributeSource token = tokens[i];
      final NamedList<Object> tokenNamedList = new SimpleOrderedMap<>();
      final BytesRef rawBytes;
      if (token.hasAttribute(BytesTermAttribute.class)) {
        final BytesTermAttribute bytesAtt = token.getAttribute(BytesTermAttribute.class);
        rawBytes = bytesAtt.getBytesRef(); 
      } else {
        final TermToBytesRefAttribute termAtt = token.getAttribute(TermToBytesRefAttribute.class);
        rawBytes = termAtt.getBytesRef();
      }
      final String text = fieldType.indexedToReadable(rawBytes, new CharsRefBuilder()).toString();
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

      token.reflectWith(new AttributeReflector() {
        @Override
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
          
          if (value instanceof BytesRef) {
            final BytesRef p = (BytesRef) value;
            value = p.toString();
          }

          tokenNamedList.add(k, value);
        }
      });

      tokensNamedLists.add(tokenNamedList);
    }

    return tokensNamedLists;
  }
  
  private String writeCharStream(NamedList<Object> out, Reader input ){
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
   * @lucene.internal
   */
  protected final static class ListBasedTokenStream extends TokenStream {
    private final List<AttributeSource> tokens;
    private Iterator<AttributeSource> tokenIterator;

    /**
     * Creates a new ListBasedTokenStream which uses the given tokens as its token source.
     *
     * @param attributeSource source of the attribute factory and attribute impls
     * @param tokens Source of tokens to be used
     */
    ListBasedTokenStream(AttributeSource attributeSource, List<AttributeSource> tokens) {
      super(attributeSource.getAttributeFactory());
      this.tokens = tokens;
      // Make sure all the attributes of the source are here too
      addAttributes(attributeSource);
    }

    @Override
    public void reset() throws IOException {
      super.reset();
      tokenIterator = tokens.iterator();
    }

    @Override
    public boolean incrementToken() {
      if (tokenIterator.hasNext()) {
        clearAttributes();
        AttributeSource next = tokenIterator.next();

        addAttributes(next); // just in case there were delayed attribute additions

        next.copyTo(this);
        return true;
      } else {
        return false;
      }
    }


    protected void addAttributes(AttributeSource attributeSource) {
      // note: ideally we wouldn't call addAttributeImpl which is marked internal. But nonetheless it's possible
      //  this method is used by some custom attributes, especially since Solr doesn't provide a way to customize the
      //  AttributeFactory which is the recommended way to choose which classes implement which attributes.
      Iterator<AttributeImpl> atts = attributeSource.getAttributeImplsIterator();
      while (atts.hasNext()) {
        addAttributeImpl(atts.next()); // adds both impl & interfaces
      }
    }
  }

  /** This is an {@link Attribute} used to track the positions of tokens
   * in the analysis chain.
   * @lucene.internal This class is only public for usage by the {@link AttributeSource} API.
   */
  public interface TokenTrackingAttribute extends Attribute {
    void freezeStage();
    void setActPosition(int pos);
    int[] getPositions();
    void reset(int[] basePositions, int position);
  }

  /** Implementation of {@link TokenTrackingAttribute}.
   * @lucene.internal This class is only public for usage by the {@link AttributeSource} API.
   */
  public static final class TokenTrackingAttributeImpl extends AttributeImpl implements TokenTrackingAttribute {
    private int[] basePositions = new int[0];
    private int position = 0;
    private transient int[] cachedPositions = null;

    @Override
    public void freezeStage() {
      this.basePositions = getPositions();
      this.position = 0;
      this.cachedPositions = null;
    }
    
    @Override
    public void setActPosition(int pos) {
      this.position = pos;
      this.cachedPositions = null;
    }
    
    @Override
    public int[] getPositions() {
      if (cachedPositions == null) {
        cachedPositions = ArrayUtils.add(basePositions, position);
      }
      return cachedPositions;
    }
    
    @Override
    public void reset(int[] basePositions, int position) {
      this.basePositions = basePositions;
      this.position = position;
      this.cachedPositions = null;
    }
    
    @Override
    public void clear() {
      // we do nothing here, as all attribute values are controlled externally by consumer
    }
    
    @Override
    public void reflectWith(AttributeReflector reflector) {
      reflector.reflect(TokenTrackingAttribute.class, "position", position);
      // convert to Integer[] array, as only such one can be serialized by ResponseWriters
      reflector.reflect(TokenTrackingAttribute.class, "positionHistory", ArrayUtils.toObject(getPositions()));
    }

    @Override
    public void copyTo(AttributeImpl target) {
      final TokenTrackingAttribute t = (TokenTrackingAttribute) target;
      t.reset(basePositions, position);
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
     * compared to the terms in the {@code termsToMatch} set. When found, 
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
