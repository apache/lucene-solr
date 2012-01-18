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
package org.apache.solr.highlight;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;


import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.highlight.*;
import org.apache.lucene.search.vectorhighlight.BoundaryScanner;
import org.apache.lucene.search.vectorhighlight.FastVectorHighlighter;
import org.apache.lucene.search.vectorhighlight.FieldQuery;
import org.apache.lucene.search.vectorhighlight.FragListBuilder;
import org.apache.lucene.search.vectorhighlight.FragmentsBuilder;
import org.apache.lucene.util.AttributeSource.State;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.HighlightParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.plugin.PluginInfoInitialized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @since solr 1.3
 */
public class DefaultSolrHighlighter extends SolrHighlighter implements PluginInfoInitialized
{

  public static Logger log = LoggerFactory.getLogger(DefaultSolrHighlighter.class);
  
  private SolrCore solrCore;

  public DefaultSolrHighlighter() {
  }

  public DefaultSolrHighlighter(SolrCore solrCore) {
    this.solrCore = solrCore;
  }

  public void init(PluginInfo info) {
    formatters.clear();
    encoders.clear();
    fragmenters.clear();
    fragListBuilders.clear();
    fragmentsBuilders.clear();
    boundaryScanners.clear();

    // Load the fragmenters
    SolrFragmenter frag = solrCore.initPlugins(info.getChildren("fragmenter") , fragmenters,SolrFragmenter.class,null);
    if (frag == null) frag = new GapFragmenter();
    fragmenters.put("", frag);
    fragmenters.put(null, frag);

    // Load the formatters
    SolrFormatter fmt = solrCore.initPlugins(info.getChildren("formatter"), formatters,SolrFormatter.class,null);
    if (fmt == null) fmt = new HtmlFormatter();
    formatters.put("", fmt);
    formatters.put(null, fmt);

    // Load the formatters
    SolrEncoder enc = solrCore.initPlugins(info.getChildren("encoder"), encoders,SolrEncoder.class,null);
    if (enc == null) enc = new DefaultEncoder();
    encoders.put("", enc);
    encoders.put(null, enc);

    // Load the FragListBuilders
    SolrFragListBuilder fragListBuilder = solrCore.initPlugins(info.getChildren("fragListBuilder"),
        fragListBuilders, SolrFragListBuilder.class, null );
    if( fragListBuilder == null ) fragListBuilder = new SimpleFragListBuilder();
    fragListBuilders.put( "", fragListBuilder );
    fragListBuilders.put( null, fragListBuilder );
    
    // Load the FragmentsBuilders
    SolrFragmentsBuilder fragsBuilder = solrCore.initPlugins(info.getChildren("fragmentsBuilder"),
        fragmentsBuilders, SolrFragmentsBuilder.class, null);
    if( fragsBuilder == null ) fragsBuilder = new ScoreOrderFragmentsBuilder();
    fragmentsBuilders.put( "", fragsBuilder );
    fragmentsBuilders.put( null, fragsBuilder );

    // Load the BoundaryScanners
    SolrBoundaryScanner boundaryScanner = solrCore.initPlugins(info.getChildren("boundaryScanner"),
        boundaryScanners, SolrBoundaryScanner.class, null);
    if(boundaryScanner == null) boundaryScanner = new SimpleBoundaryScanner();
    boundaryScanners.put("", boundaryScanner);
    boundaryScanners.put(null, boundaryScanner);
    
    initialized = true;
  }
  //just for back-compat with the deprecated method
  private boolean initialized = false;
  @Override
  @Deprecated
  public void initalize( SolrConfig config) {
    if (initialized) return;
    SolrFragmenter frag = new GapFragmenter();
    fragmenters.put("", frag);
    fragmenters.put(null, frag);

    SolrFormatter fmt = new HtmlFormatter();
    formatters.put("", fmt);
    formatters.put(null, fmt);    

    SolrEncoder enc = new DefaultEncoder();
    encoders.put("", enc);
    encoders.put(null, enc);    

    SolrFragListBuilder fragListBuilder = new SimpleFragListBuilder();
    fragListBuilders.put( "", fragListBuilder );
    fragListBuilders.put( null, fragListBuilder );
    
    SolrFragmentsBuilder fragsBuilder = new ScoreOrderFragmentsBuilder();
    fragmentsBuilders.put( "", fragsBuilder );
    fragmentsBuilders.put( null, fragsBuilder );
    
    SolrBoundaryScanner boundaryScanner = new SimpleBoundaryScanner();
    boundaryScanners.put("", boundaryScanner);
    boundaryScanners.put(null, boundaryScanner);
  }

  /**
   * Return a phrase {@link org.apache.lucene.search.highlight.Highlighter} appropriate for this field.
   * @param query The current Query
   * @param fieldName The name of the field
   * @param request The current SolrQueryRequest
   * @param tokenStream document text CachingTokenStream
   * @throws IOException 
   */
  protected Highlighter getPhraseHighlighter(Query query, String fieldName, SolrQueryRequest request, CachingTokenFilter tokenStream) throws IOException {
    SolrParams params = request.getParams();
    Highlighter highlighter = null;
    
    highlighter = new Highlighter(
        getFormatter(fieldName, params),
        getEncoder(fieldName, params),
        getSpanQueryScorer(query, fieldName, tokenStream, request));
    
    highlighter.setTextFragmenter(getFragmenter(fieldName, params));

    return highlighter;
  }
  
  /**
   * Return a {@link org.apache.lucene.search.highlight.Highlighter} appropriate for this field.
   * @param query The current Query
   * @param fieldName The name of the field
   * @param request The current SolrQueryRequest
   */
  protected Highlighter getHighlighter(Query query, String fieldName, SolrQueryRequest request) {
    SolrParams params = request.getParams(); 
    Highlighter highlighter = new Highlighter(
           getFormatter(fieldName, params), 
           getEncoder(fieldName, params),
           getQueryScorer(query, fieldName, request));
     highlighter.setTextFragmenter(getFragmenter(fieldName, params));
       return highlighter;
  }
  
  /**
   * Return a {@link org.apache.lucene.search.highlight.QueryScorer} suitable for this Query and field.
   * @param query The current query
   * @param tokenStream document text CachingTokenStream
   * @param fieldName The name of the field
   * @param request The SolrQueryRequest
   * @throws IOException 
   */
  private QueryScorer getSpanQueryScorer(Query query, String fieldName, TokenStream tokenStream, SolrQueryRequest request) throws IOException {
    boolean reqFieldMatch = request.getParams().getFieldBool(fieldName, HighlightParams.FIELD_MATCH, false);
    Boolean highlightMultiTerm = request.getParams().getBool(HighlightParams.HIGHLIGHT_MULTI_TERM, true);
    if(highlightMultiTerm == null) {
      highlightMultiTerm = false;
    }
    QueryScorer scorer;
    if (reqFieldMatch) {
      scorer = new QueryScorer(query, fieldName);
    }
    else {
      scorer = new QueryScorer(query, null);
    }
    scorer.setExpandMultiTermQuery(highlightMultiTerm);
    return scorer;
  }

  /**
   * Return a {@link org.apache.lucene.search.highlight.Scorer} suitable for this Query and field.
   * @param query The current query
   * @param fieldName The name of the field
   * @param request The SolrQueryRequest
   */
  private Scorer getQueryScorer(Query query, String fieldName, SolrQueryRequest request) {
     boolean reqFieldMatch = request.getParams().getFieldBool(fieldName, HighlightParams.FIELD_MATCH, false);
     if (reqFieldMatch) {
        return new QueryTermScorer(query, request.getSearcher().getIndexReader(), fieldName);
     }
     else {
        return new QueryTermScorer(query);
     }
  }
  
  /**
   * Return the max number of snippets for this field. If this has not
   * been configured for this field, fall back to the configured default
   * or the solr default.
   * @param fieldName The name of the field
   * @param params The params controlling Highlighting
   */
  protected int getMaxSnippets(String fieldName, SolrParams params) {
     return params.getFieldInt(fieldName, HighlightParams.SNIPPETS,1);
  }

  /**
   * Return whether adjacent fragments should be merged.
   * @param fieldName The name of the field
   * @param params The params controlling Highlighting
   */
  protected boolean isMergeContiguousFragments(String fieldName, SolrParams params){
    return params.getFieldBool(fieldName, HighlightParams.MERGE_CONTIGUOUS_FRAGMENTS, false);
  }
  
  /**
   * Return a {@link org.apache.lucene.search.highlight.Formatter} appropriate for this field. If a formatter
   * has not been configured for this field, fall back to the configured
   * default or the solr default ({@link org.apache.lucene.search.highlight.SimpleHTMLFormatter}).
   * 
   * @param fieldName The name of the field
   * @param params The params controlling Highlighting
   * @return An appropriate {@link org.apache.lucene.search.highlight.Formatter}.
   */
  protected Formatter getFormatter(String fieldName, SolrParams params ) 
  {
    String str = params.getFieldParam( fieldName, HighlightParams.FORMATTER );
    SolrFormatter formatter = formatters.get( str );
    if( formatter == null ) {
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "Unknown formatter: "+str );
    }
    return formatter.getFormatter( fieldName, params );
  }

  /**
   * Return an {@link org.apache.lucene.search.highlight.Encoder} appropriate for this field. If an encoder
   * has not been configured for this field, fall back to the configured
   * default or the solr default ({@link org.apache.lucene.search.highlight.DefaultEncoder}).
   * 
   * @param fieldName The name of the field
   * @param params The params controlling Highlighting
   * @return An appropriate {@link org.apache.lucene.search.highlight.Encoder}.
   */
  protected Encoder getEncoder(String fieldName, SolrParams params){
    String str = params.getFieldParam( fieldName, HighlightParams.ENCODER );
    SolrEncoder encoder = encoders.get( str );
    if( encoder == null ) {
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "Unknown encoder: "+str );
    }
    return encoder.getEncoder( fieldName, params );
  }
  
  /**
   * Return a {@link org.apache.lucene.search.highlight.Fragmenter} appropriate for this field. If a fragmenter
   * has not been configured for this field, fall back to the configured
   * default or the solr default ({@link GapFragmenter}).
   * 
   * @param fieldName The name of the field
   * @param params The params controlling Highlighting
   * @return An appropriate {@link org.apache.lucene.search.highlight.Fragmenter}.
   */
  protected Fragmenter getFragmenter(String fieldName, SolrParams params) 
  {
    String fmt = params.getFieldParam( fieldName, HighlightParams.FRAGMENTER );
    SolrFragmenter frag = fragmenters.get( fmt );
    if( frag == null ) {
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "Unknown fragmenter: "+fmt );
    }
    return frag.getFragmenter( fieldName, params );
  }
  
  protected FragListBuilder getFragListBuilder( String fieldName, SolrParams params ){
    String flb = params.getFieldParam( fieldName, HighlightParams.FRAG_LIST_BUILDER );
    SolrFragListBuilder solrFlb = fragListBuilders.get( flb );
    if( solrFlb == null ){
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "Unknown fragListBuilder: " + flb );
    }
    return solrFlb.getFragListBuilder( params );
  }
  
  protected FragmentsBuilder getFragmentsBuilder( String fieldName, SolrParams params ){
    BoundaryScanner bs = getBoundaryScanner(fieldName, params);
    return getSolrFragmentsBuilder( fieldName, params ).getFragmentsBuilder( params, bs );
  }
  
  private SolrFragmentsBuilder getSolrFragmentsBuilder( String fieldName, SolrParams params ){
    String fb = params.getFieldParam( fieldName, HighlightParams.FRAGMENTS_BUILDER );
    SolrFragmentsBuilder solrFb = fragmentsBuilders.get( fb );
    if( solrFb == null ){
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "Unknown fragmentsBuilder: " + fb );
    }
    return solrFb;
  }
  
  private BoundaryScanner getBoundaryScanner(String fieldName, SolrParams params){
    String bs = params.getFieldParam(fieldName, HighlightParams.BOUNDARY_SCANNER);
    SolrBoundaryScanner solrBs = boundaryScanners.get(bs);
    if(solrBs == null){
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unknown boundaryScanner: " + bs);
    }
    return solrBs.getBoundaryScanner(fieldName, params);
  }
  
  /**
   * Generates a list of Highlighted query fragments for each item in a list
   * of documents, or returns null if highlighting is disabled.
   *
   * @param docs query results
   * @param query the query
   * @param req the current request
   * @param defaultFields default list of fields to summarize
   *
   * @return NamedList containing a NamedList for each document, which in 
   * turns contains sets (field, summary) pairs.
   */
  @Override
  @SuppressWarnings("unchecked")
  public NamedList<Object> doHighlighting(DocList docs, Query query, SolrQueryRequest req, String[] defaultFields) throws IOException {
    SolrParams params = req.getParams(); 
    if (!isHighlightingEnabled(params))
        return null;
     
    SolrIndexSearcher searcher = req.getSearcher();
    IndexSchema schema = searcher.getSchema();
    NamedList fragments = new SimpleOrderedMap();
    String[] fieldNames = getHighlightFields(query, req, defaultFields);
    Set<String> fset = new HashSet<String>();
     
    {
      // pre-fetch documents using the Searcher's doc cache
      for(String f : fieldNames) { fset.add(f); }
      // fetch unique key if one exists.
      SchemaField keyField = schema.getUniqueKeyField();
      if(null != keyField)
        fset.add(keyField.getName());  
    }

    // get FastVectorHighlighter instance out of the processing loop
    FastVectorHighlighter fvh = new FastVectorHighlighter(
        // FVH cannot process hl.usePhraseHighlighter parameter per-field basis
        params.getBool( HighlightParams.USE_PHRASE_HIGHLIGHTER, true ),
        // FVH cannot process hl.requireFieldMatch parameter per-field basis
        params.getBool( HighlightParams.FIELD_MATCH, false ) );
    fvh.setPhraseLimit(params.getInt(HighlightParams.PHRASE_LIMIT, Integer.MAX_VALUE));
    FieldQuery fieldQuery = fvh.getFieldQuery( query, searcher.getIndexReader() );

    // Highlight each document
    DocIterator iterator = docs.iterator();
    for (int i = 0; i < docs.size(); i++) {
      int docId = iterator.nextDoc();
      Document doc = searcher.doc(docId, fset);
      NamedList docSummaries = new SimpleOrderedMap();
      for (String fieldName : fieldNames) {
        fieldName = fieldName.trim();
        if( useFastVectorHighlighter( params, schema, fieldName ) )
          doHighlightingByFastVectorHighlighter( fvh, fieldQuery, req, docSummaries, docId, doc, fieldName );
        else
          doHighlightingByHighlighter( query, req, docSummaries, docId, doc, fieldName );
      }
      String printId = schema.printableUniqueKey(doc);
      fragments.add(printId == null ? null : printId, docSummaries);
    }
    return fragments;
  }
  
  /*
   * If fieldName is undefined, this method returns false, then
   * doHighlightingByHighlighter() will do nothing for the field.
   */
  private boolean useFastVectorHighlighter( SolrParams params, IndexSchema schema, String fieldName ){
    SchemaField schemaField = schema.getFieldOrNull( fieldName );
    if( schemaField == null ) return false;
    boolean useFvhParam = params.getFieldBool( fieldName, HighlightParams.USE_FVH, false );
    if( !useFvhParam ) return false;
    boolean termPosOff = schemaField.storeTermPositions() && schemaField.storeTermOffsets();
    if( !termPosOff ) {
      log.warn( "Solr will use Highlighter instead of FastVectorHighlighter because {} field does not store TermPositions and TermOffsets.", fieldName );
    }
    return termPosOff;
  }
  
  private void doHighlightingByHighlighter( Query query, SolrQueryRequest req, NamedList docSummaries,
      int docId, Document doc, String fieldName ) throws IOException {
    final SolrIndexSearcher searcher = req.getSearcher();
    final IndexSchema schema = searcher.getSchema();
    
    // TODO: Currently in trunk highlighting numeric fields is broken (Lucene) -
    // so we disable them until fixed (see LUCENE-3080)!
    // BEGIN: Hack
    final SchemaField schemaField = schema.getFieldOrNull(fieldName);
    if (schemaField != null && (
      (schemaField.getType() instanceof org.apache.solr.schema.TrieField) ||
      (schemaField.getType() instanceof org.apache.solr.schema.TrieDateField)
    )) return;
    // END: Hack
    
    SolrParams params = req.getParams(); 
    IndexableField[] docFields = doc.getFields(fieldName);
    List<String> listFields = new ArrayList<String>();
    for (IndexableField field : docFields) {
      listFields.add(field.stringValue());
    }

    String[] docTexts = (String[]) listFields.toArray(new String[listFields.size()]);
   
    // according to Document javadoc, doc.getValues() never returns null. check empty instead of null
    if (docTexts.length == 0) return;
    
    TokenStream tstream = null;
    int numFragments = getMaxSnippets(fieldName, params);
    boolean mergeContiguousFragments = isMergeContiguousFragments(fieldName, params);

    String[] summaries = null;
    List<TextFragment> frags = new ArrayList<TextFragment>();

    TermOffsetsTokenStream tots = null; // to be non-null iff we're using TermOffsets optimization
    try {
        TokenStream tvStream = TokenSources.getTokenStream(searcher.getIndexReader(), docId, fieldName);
        if (tvStream != null) {
          tots = new TermOffsetsTokenStream(tvStream);
        }
    }
    catch (IllegalArgumentException e) {
      // No problem. But we can't use TermOffsets optimization.
    }

    for (int j = 0; j < docTexts.length; j++) {
      if( tots != null ) {
        // if we're using TermOffsets optimization, then get the next
        // field value's TokenStream (i.e. get field j's TokenStream) from tots:
        tstream = tots.getMultiValuedTokenStream( docTexts[j].length() );
      } else {
        // fall back to analyzer
        tstream = createAnalyzerTStream(schema, fieldName, docTexts[j]);
      }
      
      int maxCharsToAnalyze = params.getFieldInt(fieldName,
          HighlightParams.MAX_CHARS,
          Highlighter.DEFAULT_MAX_CHARS_TO_ANALYZE);
      
      Highlighter highlighter;
      if (Boolean.valueOf(req.getParams().get(HighlightParams.USE_PHRASE_HIGHLIGHTER, "true"))) {
        if (maxCharsToAnalyze < 0) {
          tstream = new CachingTokenFilter(tstream);
        } else {
          tstream = new CachingTokenFilter(new OffsetLimitTokenFilter(tstream, maxCharsToAnalyze));
        }
        
        // get highlighter
        highlighter = getPhraseHighlighter(query, fieldName, req, (CachingTokenFilter) tstream);
         
        // after highlighter initialization, reset tstream since construction of highlighter already used it
        tstream.reset();
      }
      else {
        // use "the old way"
        highlighter = getHighlighter(query, fieldName, req);
      }
      
      if (maxCharsToAnalyze < 0) {
        highlighter.setMaxDocCharsToAnalyze(docTexts[j].length());
      } else {
        highlighter.setMaxDocCharsToAnalyze(maxCharsToAnalyze);
      }

      try {
        TextFragment[] bestTextFragments = highlighter.getBestTextFragments(tstream, docTexts[j], mergeContiguousFragments, numFragments);
        for (int k = 0; k < bestTextFragments.length; k++) {
          if ((bestTextFragments[k] != null) && (bestTextFragments[k].getScore() > 0)) {
            frags.add(bestTextFragments[k]);
          }
        }
      } catch (InvalidTokenOffsetsException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
    }
    // sort such that the fragments with the highest score come first
    Collections.sort(frags, new Comparator<TextFragment>() {
      public int compare(TextFragment arg0, TextFragment arg1) {
        return Math.round(arg1.getScore() - arg0.getScore());
      }
    });
    
     // convert fragments back into text
     // TODO: we can include score and position information in output as snippet attributes
    if (frags.size() > 0) {
      ArrayList<String> fragTexts = new ArrayList<String>();
      for (TextFragment fragment: frags) {
        if ((fragment != null) && (fragment.getScore() > 0)) {
          fragTexts.add(fragment.toString());
        }
        if (fragTexts.size() >= numFragments) break;
      }
      summaries = fragTexts.toArray(new String[0]);
      if (summaries.length > 0) 
      docSummaries.add(fieldName, summaries);
    }
    // no summeries made, copy text from alternate field
    if (summaries == null || summaries.length == 0) {
      alternateField( docSummaries, params, doc, fieldName );
    }
  }

  private void doHighlightingByFastVectorHighlighter( FastVectorHighlighter highlighter, FieldQuery fieldQuery,
      SolrQueryRequest req, NamedList docSummaries, int docId, Document doc,
      String fieldName ) throws IOException {
    SolrParams params = req.getParams(); 
    SolrFragmentsBuilder solrFb = getSolrFragmentsBuilder( fieldName, params );
    String[] snippets = highlighter.getBestFragments( fieldQuery, req.getSearcher().getIndexReader(), docId, fieldName,
        params.getFieldInt( fieldName, HighlightParams.FRAGSIZE, 100 ),
        params.getFieldInt( fieldName, HighlightParams.SNIPPETS, 1 ),
        getFragListBuilder( fieldName, params ),
        getFragmentsBuilder( fieldName, params ),
        solrFb.getPreTags( params, fieldName ),
        solrFb.getPostTags( params, fieldName ),
        getEncoder( fieldName, params ) );
    if( snippets != null && snippets.length > 0 )
      docSummaries.add( fieldName, snippets );
    else
      alternateField( docSummaries, params, doc, fieldName );
  }
  
  private void alternateField( NamedList docSummaries, SolrParams params, Document doc, String fieldName ){
    String alternateField = params.getFieldParam(fieldName, HighlightParams.ALTERNATE_FIELD);
    if (alternateField != null && alternateField.length() > 0) {
      IndexableField[] docFields = doc.getFields(alternateField);
      List<String> listFields = new ArrayList<String>();
      for (IndexableField field : docFields) {
        if (field.binaryValue() == null)
          listFields.add(field.stringValue());
      }

      String[] altTexts = listFields.toArray(new String[listFields.size()]);

      if (altTexts != null && altTexts.length > 0){
        Encoder encoder = getEncoder(fieldName, params);
        int alternateFieldLen = params.getFieldInt(fieldName, HighlightParams.ALTERNATE_FIELD_LENGTH,0);
        List<String> altList = new ArrayList<String>();
        int len = 0;
        for( String altText: altTexts ){
          if( alternateFieldLen <= 0 ){
            altList.add(encoder.encodeText(altText));
          }
          else{
            altList.add( len + altText.length() > alternateFieldLen ?
                encoder.encodeText(new String(altText.substring( 0, alternateFieldLen - len ))) :
                encoder.encodeText(altText) );
            len += altText.length();
            if( len >= alternateFieldLen ) break;
          }
        }
        docSummaries.add(fieldName, altList);
      }
    }
  }
  
  private TokenStream createAnalyzerTStream(IndexSchema schema, String fieldName, String docText) throws IOException {

    TokenStream tstream;
    TokenStream ts = schema.getAnalyzer().tokenStream(fieldName, new StringReader(docText));
    ts.reset();
    tstream = new TokenOrderingFilter(ts, 10);
    return tstream;
  }
}

/** Orders Tokens in a window first by their startOffset ascending.
 * endOffset is currently ignored.
 * This is meant to work around fickleness in the highlighter only.  It
 * can mess up token positions and should not be used for indexing or querying.
 */
final class TokenOrderingFilter extends TokenFilter {
  private final int windowSize;
  private final LinkedList<OrderedToken> queue = new LinkedList<OrderedToken>();
  private boolean done=false;
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  
  protected TokenOrderingFilter(TokenStream input, int windowSize) {
    super(input);
    this.windowSize = windowSize;
  }

  @Override
  public boolean incrementToken() throws IOException {
    while (!done && queue.size() < windowSize) {
      if (!input.incrementToken()) {
        done = true;
        break;
      }

      // reverse iterating for better efficiency since we know the
      // list is already sorted, and most token start offsets will be too.
      ListIterator<OrderedToken> iter = queue.listIterator(queue.size());
      while(iter.hasPrevious()) {
        if (offsetAtt.startOffset() >= iter.previous().startOffset) {
          // insertion will be before what next() would return (what
          // we just compared against), so move back one so the insertion
          // will be after.
          iter.next();
          break;
        }
      }
      OrderedToken ot = new OrderedToken();
      ot.state = captureState();
      ot.startOffset = offsetAtt.startOffset();
      iter.add(ot);
    }

    if (queue.isEmpty()) {
      return false;
    } else {
      restoreState(queue.removeFirst().state);
      return true;
    }
  }
}

// for TokenOrderingFilter, so it can easily sort by startOffset
class OrderedToken {
  State state;
  int startOffset;
}

class TermOffsetsTokenStream {

  TokenStream bufferedTokenStream = null;
  OffsetAttribute bufferedOffsetAtt;
  State bufferedToken;
  int bufferedStartOffset;
  int bufferedEndOffset;
  int startOffset;
  int endOffset;

  public TermOffsetsTokenStream( TokenStream tstream ){
    bufferedTokenStream = tstream;
    bufferedOffsetAtt = bufferedTokenStream.addAttribute(OffsetAttribute.class);
    startOffset = 0;
    bufferedToken = null;
  }

  public TokenStream getMultiValuedTokenStream( final int length ){
    endOffset = startOffset + length;
    return new MultiValuedStream(length);
  }
  
  final class MultiValuedStream extends TokenStream {
    private final int length;
    OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

      MultiValuedStream(int length) { 
        super(bufferedTokenStream.cloneAttributes());
        this.length = length;
      }
      
      @Override
      public boolean incrementToken() throws IOException {
        while( true ){
          if( bufferedToken == null ) {
            if (!bufferedTokenStream.incrementToken())
              return false;
            bufferedToken = bufferedTokenStream.captureState();
            bufferedStartOffset = bufferedOffsetAtt.startOffset();
            bufferedEndOffset = bufferedOffsetAtt.endOffset();
          }
          
          if( startOffset <= bufferedStartOffset &&
              bufferedEndOffset <= endOffset ){
            restoreState(bufferedToken);
            bufferedToken = null;
            offsetAtt.setOffset( offsetAtt.startOffset() - startOffset, offsetAtt.endOffset() - startOffset );
            return true;
          }
          else if( bufferedEndOffset > endOffset ){
            startOffset += length + 1;
            return false;
          }
          bufferedToken = null;
        }
      }

  };
};
