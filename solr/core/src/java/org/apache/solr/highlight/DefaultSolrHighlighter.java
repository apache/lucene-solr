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
package org.apache.solr.highlight;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.index.StorableField;
import org.apache.lucene.index.StoredDocument;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.highlight.Encoder;
import org.apache.lucene.search.highlight.Formatter;
import org.apache.lucene.search.highlight.Fragmenter;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.InvalidTokenOffsetsException;
import org.apache.lucene.search.highlight.OffsetLimitTokenFilter;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.QueryTermScorer;
import org.apache.lucene.search.highlight.Scorer;
import org.apache.lucene.search.highlight.TextFragment;
import org.apache.lucene.search.highlight.TokenSources;
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

  protected final SolrCore solrCore;

  //Will be invoked via reflection
  public DefaultSolrHighlighter(SolrCore solrCore) {
    this.solrCore = solrCore;
  }

  // Thread safe registry
  protected final Map<String,SolrFormatter> formatters =
      new HashMap<>();

  // Thread safe registry
  protected final Map<String,SolrEncoder> encoders =
      new HashMap<>();

  // Thread safe registry
  protected final Map<String,SolrFragmenter> fragmenters =
      new HashMap<>() ;

  // Thread safe registry
  protected final Map<String, SolrFragListBuilder> fragListBuilders =
      new HashMap<>() ;

  // Thread safe registry
  protected final Map<String, SolrFragmentsBuilder> fragmentsBuilders =
      new HashMap<>() ;

  // Thread safe registry
  protected final Map<String, SolrBoundaryScanner> boundaryScanners =
      new HashMap<>() ;

  @Override
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

    // Load the encoders
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

  }

  /**
   * Return a phrase {@link org.apache.lucene.search.highlight.Highlighter} appropriate for this field.
   * @param query The current Query
   * @param fieldName The name of the field
   * @param request The current SolrQueryRequest
   * @param tokenStream document text tokenStream that implements reset() efficiently (e.g. CachingTokenFilter).
   *                    If it's used, call reset() first.
   * @throws IOException If there is a low-level I/O error.
   */
  protected Highlighter getPhraseHighlighter(Query query, String fieldName, SolrQueryRequest request, TokenStream tokenStream) throws IOException {
    SolrParams params = request.getParams();
    Highlighter highlighter = new Highlighter(
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
   * @param tokenStream document text tokenStream that implements reset() efficiently (e.g. CachingTokenFilter).
   *                    If it's used, call reset() first.
   * @param fieldName The name of the field
   * @param request The SolrQueryRequest
   */
  protected QueryScorer getSpanQueryScorer(Query query, String fieldName, TokenStream tokenStream, SolrQueryRequest request) {
    QueryScorer scorer = new QueryScorer(query,
        request.getParams().getFieldBool(fieldName, HighlightParams.FIELD_MATCH, false) ? fieldName : null);
    scorer.setExpandMultiTermQuery(request.getParams().getBool(HighlightParams.HIGHLIGHT_MULTI_TERM, true));
    scorer.setUsePayloads(request.getParams().getFieldBool(fieldName, HighlightParams.PAYLOADS,
        request.getSearcher().getLeafReader().getFieldInfos().fieldInfo(fieldName).hasPayloads()));
    return scorer;
  }

  /**
   * Return a {@link org.apache.lucene.search.highlight.Scorer} suitable for this Query and field.
   * @param query The current query
   * @param fieldName The name of the field
   * @param request The SolrQueryRequest
   */
  protected Scorer getQueryScorer(Query query, String fieldName, SolrQueryRequest request) {
    boolean reqFieldMatch = request.getParams().getFieldBool(fieldName, HighlightParams.FIELD_MATCH, false);
    if (reqFieldMatch) {
      return new QueryTermScorer(query, request.getSearcher().getIndexReader(), fieldName);
    } else {
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
    return params.getFieldInt(fieldName, HighlightParams.SNIPPETS, 1);
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
    SolrFormatter formatter = formatters.get(str);
    if( formatter == null ) {
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "Unknown formatter: "+str );
    }
    return formatter.getFormatter(fieldName, params);
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
    return encoder.getEncoder(fieldName, params);
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
    SolrFragmenter frag = fragmenters.get(fmt);
    if( frag == null ) {
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "Unknown fragmenter: "+fmt );
    }
    return frag.getFragmenter(fieldName, params);
  }

  protected FragListBuilder getFragListBuilder( String fieldName, SolrParams params ){
    String flb = params.getFieldParam( fieldName, HighlightParams.FRAG_LIST_BUILDER );
    SolrFragListBuilder solrFlb = fragListBuilders.get(flb);
    if( solrFlb == null ){
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "Unknown fragListBuilder: " + flb );
    }
    return solrFlb.getFragListBuilder(params);
  }

  protected FragmentsBuilder getFragmentsBuilder( String fieldName, SolrParams params ){
    BoundaryScanner bs = getBoundaryScanner(fieldName, params);
    return getSolrFragmentsBuilder( fieldName, params ).getFragmentsBuilder(params, bs);
  }

  protected SolrFragmentsBuilder getSolrFragmentsBuilder( String fieldName, SolrParams params ){
    String fb = params.getFieldParam( fieldName, HighlightParams.FRAGMENTS_BUILDER );
    SolrFragmentsBuilder solrFb = fragmentsBuilders.get(fb);
    if( solrFb == null ){
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "Unknown fragmentsBuilder: " + fb );
    }
    return solrFb;
  }

  protected BoundaryScanner getBoundaryScanner(String fieldName, SolrParams params){
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
    if (!isHighlightingEnabled(params)) // also returns early if no unique key field
      return null;

    SolrIndexSearcher searcher = req.getSearcher();
    IndexSchema schema = searcher.getSchema();

    // fetch unique key if one exists.
    SchemaField keyField = schema.getUniqueKeyField();
    if (keyField == null) {
      return null;//exit early; we need a unique key field to populate the response
    }

    String[] fieldNames = getHighlightFields(query, req, defaultFields);

    Set<String> preFetchFieldNames = getDocPrefetchFieldNames(fieldNames, req);
    if (preFetchFieldNames != null) {
      preFetchFieldNames.add(keyField.getName());
    }

    FastVectorHighlighter fvh = null; // lazy
    FieldQuery fvhFieldQuery = null; // lazy

    // Highlight each document
    NamedList fragments = new SimpleOrderedMap();
    DocIterator iterator = docs.iterator();
    for (int i = 0; i < docs.size(); i++) {
      int docId = iterator.nextDoc();
      StoredDocument doc = searcher.doc(docId, preFetchFieldNames);

      NamedList docSummaries = new SimpleOrderedMap();
      for (String fieldName : fieldNames) {
        if (useFastVectorHighlighter(params, schema, fieldName)) {
          if (fvhFieldQuery == null) {
            fvh = new FastVectorHighlighter(
                // FVH cannot process hl.usePhraseHighlighter parameter per-field basis
                params.getBool(HighlightParams.USE_PHRASE_HIGHLIGHTER, true),
                // FVH cannot process hl.requireFieldMatch parameter per-field basis
                params.getBool(HighlightParams.FIELD_MATCH, false));
            fvh.setPhraseLimit(params.getInt(HighlightParams.PHRASE_LIMIT, SolrHighlighter.DEFAULT_PHRASE_LIMIT));
            fvhFieldQuery = fvh.getFieldQuery(query, searcher.getIndexReader());
          }
          doHighlightingByFastVectorHighlighter(fvh, fvhFieldQuery, req, docSummaries, docId, doc, fieldName);
        } else {
          doHighlightingByHighlighter(query, req, docSummaries, docId, doc, fieldName);
        }
      } // for each field
      fragments.add(schema.printableUniqueKey(doc), docSummaries);
    } // for each doc
    return fragments;
  }

  /** Returns the field names to be passed to {@link SolrIndexSearcher#doc(int, Set)}.
   * Subclasses might over-ride to include fields in search-results and other stored field values needed so as to avoid
   * the possibility of extra trips to disk.  The uniqueKey will be added after if the result isn't null. */
  protected Set<String> getDocPrefetchFieldNames(String[] hlFieldNames, SolrQueryRequest req) {
    Set<String> preFetchFieldNames = new HashSet<>(hlFieldNames.length + 1);//+1 for uniqueyKey added after
    Collections.addAll(preFetchFieldNames, hlFieldNames);
    return preFetchFieldNames;
  }

  /**
   * If fieldName is undefined, this method returns false, then
   * doHighlightingByHighlighter() will do nothing for the field.
   */
  protected boolean useFastVectorHighlighter( SolrParams params, IndexSchema schema, String fieldName ){
    SchemaField schemaField = schema.getFieldOrNull(fieldName);
    if( schemaField == null ) return false;
    boolean useFvhParam = params.getFieldBool(fieldName, HighlightParams.USE_FVH, false);
    if( !useFvhParam ) return false;
    boolean termPosOff = schemaField.storeTermPositions() && schemaField.storeTermOffsets();
    if( !termPosOff ) {
      log.warn( "Solr will use Highlighter instead of FastVectorHighlighter because {} field does not store TermPositions and TermOffsets.", fieldName );
    }
    return termPosOff;
  }

  @SuppressWarnings("unchecked")
  protected void doHighlightingByFastVectorHighlighter( FastVectorHighlighter highlighter, FieldQuery fieldQuery,
                                                        SolrQueryRequest req, NamedList docSummaries, int docId, StoredDocument doc,
                                                        String fieldName ) throws IOException {
    SolrParams params = req.getParams();
    SolrFragmentsBuilder solrFb = getSolrFragmentsBuilder(fieldName, params);
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

  @SuppressWarnings("unchecked")
  protected void doHighlightingByHighlighter( Query query, SolrQueryRequest req, NamedList docSummaries,
                                              int docId, StoredDocument doc, String fieldName ) throws IOException {
    final SolrIndexSearcher searcher = req.getSearcher();
    final IndexSchema schema = searcher.getSchema();
    final SolrParams params = req.getParams();

    // TODO: highlighting numeric fields is broken (Lucene) - so we disable them until fixed (see LUCENE-3080)!
    // BEGIN: Hack
    final SchemaField schemaField = schema.getFieldOrNull(fieldName);
    if (schemaField != null && schemaField.getType() instanceof org.apache.solr.schema.TrieField) return;
    // END: Hack

    final int mvToExamine =
        req.getParams().getFieldInt(fieldName, HighlightParams.MAX_MULTIVALUED_TO_EXAMINE,
            (schemaField != null && schemaField.multiValued()) ? Integer.MAX_VALUE : 1);
    // Technically this is the max *fragments* (snippets), not max values:
    int mvToMatch =
        req.getParams().getFieldInt(fieldName, HighlightParams.MAX_MULTIVALUED_TO_MATCH, Integer.MAX_VALUE);
    if (mvToExamine <= 0 || mvToMatch <= 0) {
      return;
    }

    int maxCharsToAnalyze = params.getFieldInt(fieldName,
        HighlightParams.MAX_CHARS,
        Highlighter.DEFAULT_MAX_CHARS_TO_ANALYZE);
    if (maxCharsToAnalyze < 0) {//e.g. -1
      maxCharsToAnalyze = Integer.MAX_VALUE;
    }

    List<String> fieldValues = getFieldValues(req, doc, fieldName, mvToExamine, maxCharsToAnalyze);
    if (fieldValues.isEmpty()) {
      return;
    }

    // preserve order of values in a multiValued list
    boolean preserveMulti = params.getFieldBool(fieldName, HighlightParams.PRESERVE_MULTI, false);

    int numFragments = getMaxSnippets(fieldName, params);
    boolean mergeContiguousFragments = isMergeContiguousFragments(fieldName, params);

    List<TextFragment> frags = new ArrayList<>();

    //Try term vectors, which is faster
    final TokenStream tvStream =
        TokenSources.getTokenStreamWithOffsets(searcher.getIndexReader(), docId, fieldName);
    //  We need to wrap in OffsetWindowTokenFilter if multi-valued
    final OffsetWindowTokenFilter tvWindowStream;
    if (tvStream != null && fieldValues.size() > 1) {
      tvWindowStream = new OffsetWindowTokenFilter(tvStream);
    } else {
      tvWindowStream = null;
    }

    for (String thisText : fieldValues) {
      if (mvToMatch <= 0 || maxCharsToAnalyze <= 0) {
        break;
      }

      TokenStream tstream;
      if (tvWindowStream != null) {
        // if we have a multi-valued field with term vectors, then get the next offset window
        tstream = tvWindowStream.advanceToNextWindowOfLength(thisText.length());
      } else if (tvStream != null) {
        tstream = tvStream; // single-valued with term vectors
      } else {
        // fall back to analyzer
        tstream = createAnalyzerTStream(schema, fieldName, thisText);
      }

      Highlighter highlighter;
      if (req.getParams().getFieldBool(fieldName, HighlightParams.USE_PHRASE_HIGHLIGHTER, true)) {
        // We're going to call getPhraseHighlighter and it might consume the tokenStream. If it does, the tokenStream
        // needs to implement reset() efficiently.

        //If the tokenStream is right from the term vectors, then CachingTokenFilter is unnecessary.
        //  It should be okay if OffsetLimit won't get applied in this case.
        final TokenStream tempTokenStream;
        if (tstream != tvStream) {
          if (maxCharsToAnalyze >= thisText.length()) {
            tempTokenStream = new CachingTokenFilter(tstream);
          } else {
            tempTokenStream = new CachingTokenFilter(new OffsetLimitTokenFilter(tstream, maxCharsToAnalyze));
          }
        } else {
          tempTokenStream = tstream;
        }

        // get highlighter
        highlighter = getPhraseHighlighter(query, fieldName, req, tempTokenStream);

        // if the CachingTokenFilter was consumed then use it going forward.
        if (tempTokenStream instanceof CachingTokenFilter && ((CachingTokenFilter)tempTokenStream).isCached()) {
          tstream = tempTokenStream;
        }
        //tstream.reset(); not needed; getBestTextFragments will reset it.
      } else {
        // use "the old way"
        highlighter = getHighlighter(query, fieldName, req);
      }

      highlighter.setMaxDocCharsToAnalyze(maxCharsToAnalyze);
      maxCharsToAnalyze -= thisText.length();

      // Highlight!
      try {
        TextFragment[] bestTextFragments =
            highlighter.getBestTextFragments(tstream, thisText, mergeContiguousFragments, numFragments);
        for (TextFragment bestTextFragment : bestTextFragments) {
          if (bestTextFragment == null)//can happen via mergeContiguousFragments
            continue;
          // normally we want a score (must be highlighted), but if preserveMulti then we return a snippet regardless.
          if (bestTextFragment.getScore() > 0 || preserveMulti) {
            frags.add(bestTextFragment);
            if (bestTextFragment.getScore() > 0)
              --mvToMatch; // note: limits fragments (for multi-valued fields), not quite the number of values
          }
        }
      } catch (InvalidTokenOffsetsException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
    }//end field value loop

    // Put the fragments onto the Solr response (docSummaries)
    if (frags.size() > 0) {
      // sort such that the fragments with the highest score come first
      if (!preserveMulti) {
        Collections.sort(frags, new Comparator<TextFragment>() {//TODO make TextFragment Comparable
          @Override
          public int compare(TextFragment arg0, TextFragment arg1) {
            return Float.compare(arg1.getScore(), arg0.getScore());
          }
        });
      }

      // Truncate list to hl.snippets, but not when hl.preserveMulti
      if (frags.size() > numFragments && !preserveMulti) {
        frags = frags.subList(0, numFragments);
      }
      docSummaries.add(fieldName, getResponseForFragments(frags, req));
    } else {
      // no summaries made, copy text from alternate field
      alternateField(docSummaries, params, doc, fieldName);
    }
  }

  /** Fetches field values to highlight. If the field value should come from an atypical place (or another aliased
   * field name, then a subclass could override to implement that.
   */
  protected List<String> getFieldValues(SolrQueryRequest req, StoredDocument doc, String fieldName,
                                        int maxValues, int maxCharsToAnalyze) {
    // Collect the Fields we will examine (could be more than one if multi-valued)
    List<String> result = new ArrayList<>();
    for (StorableField thisField : doc.getFields()) {
      if (! thisField.name().equals(fieldName)) {
        continue;
      }
      String value = thisField.stringValue();
      result.add(value);

      maxCharsToAnalyze -= value.length();//we exit early if we'll never get to analyze the value
      maxValues--;
      if (maxValues <= 0 || maxCharsToAnalyze <= 0) {
        break;
      }
    }
    return result;
  }

  /** Given the fragments, return the result to be put in the field {@link NamedList}. This is an extension
   * point to allow adding other metadata like the absolute offsets or scores.
   */
  protected Object getResponseForFragments(List<TextFragment> frags, SolrQueryRequest req) {
    // TODO: we can include score and position information in output as snippet attributes
    ArrayList<String> fragTexts = new ArrayList<>();
    for (TextFragment fragment : frags) {
      fragTexts.add(fragment.toString());
    }
    return fragTexts.toArray(new String[fragTexts.size()]);
  }

  @SuppressWarnings("unchecked")
  protected void alternateField( NamedList docSummaries, SolrParams params, StoredDocument doc, String fieldName ){
    String alternateField = params.getFieldParam(fieldName, HighlightParams.ALTERNATE_FIELD);
    if (alternateField != null && alternateField.length() > 0) {
      StorableField[] docFields = doc.getFields(alternateField);
      if (docFields.length == 0) {
        // The alternate field did not exist, treat the original field as fallback instead
        docFields = doc.getFields(fieldName);
      }
      List<String> listFields = new ArrayList<>();
      for (StorableField field : docFields) {
        if (field.binaryValue() == null)
          listFields.add(field.stringValue());
      }

      String[] altTexts = listFields.toArray(new String[listFields.size()]);

      if (altTexts.length > 0){
        Encoder encoder = getEncoder(fieldName, params);
        int alternateFieldLen = params.getFieldInt(fieldName, HighlightParams.ALTERNATE_FIELD_LENGTH,0);
        List<String> altList = new ArrayList<>();
        int len = 0;
        for( String altText: altTexts ){
          if( alternateFieldLen <= 0 ){
            altList.add(encoder.encodeText(altText));
          }
          else{
            //note: seemingly redundant new String(...) releases memory to the larger text
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

  protected TokenStream createAnalyzerTStream(IndexSchema schema, String fieldName, String docText) throws IOException {
    return new TokenOrderingFilter(schema.getIndexAnalyzer().tokenStream(fieldName, docText), 10);
  }
}

/** Orders Tokens in a window first by their startOffset ascending.
 * endOffset is currently ignored.
 * This is meant to work around fickleness in the highlighter only.  It
 * can mess up token positions and should not be used for indexing or querying.
 */
final class TokenOrderingFilter extends TokenFilter {
  private final int windowSize;
  private final LinkedList<OrderedToken> queue = new LinkedList<>(); //TODO replace with Deque, Array impl
  private boolean done=false;
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

  protected TokenOrderingFilter(TokenStream input, int windowSize) {
    super(input);
    this.windowSize = windowSize;
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    queue.clear();
    done = false;
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

/** For use with term vectors of multi-valued fields. We want an offset based window into its TokenStream. */
final class OffsetWindowTokenFilter extends TokenFilter {

  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  private final PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);
  private int windowStartOffset;
  private int windowEndOffset = -1;//exclusive
  private boolean windowTokenIncremented = false;
  private boolean inputWasReset = false;
  private State capturedState;//only used for first token of each subsequent window

  OffsetWindowTokenFilter(TokenStream input) {//input should not have been reset already
    super(input);
  }

  //Called at the start of each value/window
  OffsetWindowTokenFilter advanceToNextWindowOfLength(int length) {
    windowStartOffset = windowEndOffset + 1;//unclear why there's a single offset gap between values, but tests show it
    windowEndOffset = windowStartOffset + length;
    windowTokenIncremented = false;//thereby permit reset()
    return this;
  }

  @Override
  public void reset() throws IOException {
    //we do some state checking to ensure this is being used correctly
    if (windowTokenIncremented) {
      throw new IllegalStateException("This TokenStream does not support being subsequently reset()");
    }
    if (!inputWasReset) {
      super.reset();
      inputWasReset = true;
    }
  }

  @Override
  public boolean incrementToken() throws IOException {
    assert inputWasReset;
    windowTokenIncremented = true;
    while (true) {
      //increment Token
      if (capturedState == null) {
        if (!input.incrementToken()) {
          return false;
        }
      } else {
        restoreState(capturedState);
        capturedState = null;
        //Set posInc to 1 on first token of subsequent windows. To be thorough, we could subtract posIncGap?
        posIncAtt.setPositionIncrement(1);
      }

      final int startOffset = offsetAtt.startOffset();
      final int endOffset = offsetAtt.endOffset();
      if (startOffset >= windowEndOffset) {//end of window
        capturedState = captureState();
        return false;
      }
      if (startOffset >= windowStartOffset) {//in this window
        offsetAtt.setOffset(startOffset - windowStartOffset, endOffset - windowStartOffset);
        return true;
      }
      //otherwise this token is before the window; continue to advance
    }
  }
}
