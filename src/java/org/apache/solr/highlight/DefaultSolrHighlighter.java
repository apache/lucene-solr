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
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.highlight.*;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.ResourceLoader;
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

/**
 * 
 * @since solr 1.3
 */
public class DefaultSolrHighlighter extends SolrHighlighter implements PluginInfoInitialized
{

  private SolrCore solrCore;

  public DefaultSolrHighlighter() {
  }

  public DefaultSolrHighlighter(SolrCore solrCore) {
    this.solrCore = solrCore;
  }

  public void init(PluginInfo info) {
    formatters.clear();
    fragmenters.clear();

    SolrFragmenter frag = solrCore.initPlugins(info.getChildren("fragmenter") , fragmenters,SolrFragmenter.class,null);
    if (frag == null) frag = new GapFragmenter();
    fragmenters.put("", frag);
    fragmenters.put(null, frag);
    // Load the formatters
    SolrFormatter fmt = solrCore.initPlugins(info.getChildren("formatter"), formatters,SolrFormatter.class,null);
    if (fmt == null) fmt = new HtmlFormatter();
    formatters.put("", fmt);
    formatters.put(null, fmt);
    initialized = true;

  }
  //just for back-compat with the deprecated method
  private boolean initialized = false;
  @Deprecated
  public void initalize( SolrConfig config) {
    if (initialized) return;
    SolrFragmenter frag = new GapFragmenter();
    fragmenters.put("", frag);
    fragmenters.put(null, frag);

    SolrFormatter fmt = new HtmlFormatter();
    formatters.put("", fmt);
    formatters.put(null, fmt);    


  }

  /**
   * Return a phrase Highlighter appropriate for this field.
   * @param query The current Query
   * @param fieldName The name of the field
   * @param request The current SolrQueryRequest
   * @param tokenStream document text CachingTokenStream
   * @throws IOException 
   */
  protected Highlighter getPhraseHighlighter(Query query, String fieldName, SolrQueryRequest request, CachingTokenFilter tokenStream) throws IOException {
    SolrParams params = request.getParams();
    Highlighter highlighter = null;
    
    highlighter = new Highlighter(getFormatter(fieldName, params), getSpanQueryScorer(query, fieldName, tokenStream, request));
    
    highlighter.setTextFragmenter(getFragmenter(fieldName, params));

    return highlighter;
  }
  
  /**
   * Return a Highlighter appropriate for this field.
   * @param query The current Query
   * @param fieldName The name of the field
   * @param request The current SolrQueryRequest
   */
  protected Highlighter getHighlighter(Query query, String fieldName, SolrQueryRequest request) {
    SolrParams params = request.getParams(); 
    Highlighter highlighter = new Highlighter(
           getFormatter(fieldName, params), 
           getQueryScorer(query, fieldName, request));
     highlighter.setTextFragmenter(getFragmenter(fieldName, params));
       return highlighter;
  }
  
  /**
   * Return a SpanScorer suitable for this Query and field.
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
   * Return a QueryScorer suitable for this Query and field.
   * @param query The current query
   * @param fieldName The name of the field
   * @param request The SolrQueryRequest
   */
  private Scorer getQueryScorer(Query query, String fieldName, SolrQueryRequest request) {
     boolean reqFieldMatch = request.getParams().getFieldBool(fieldName, HighlightParams.FIELD_MATCH, false);
     if (reqFieldMatch) {
        return new QueryTermScorer(query, request.getSearcher().getReader(), fieldName);
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
   * Return a formatter appropriate for this field. If a formatter
   * has not been configured for this field, fall back to the configured
   * default or the solr default (SimpleHTMLFormatter).
   * 
   * @param fieldName The name of the field
   * @param params The params controlling Highlighting
   * @return An appropriate Formatter.
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
   * Return a fragmenter appropriate for this field. If a fragmenter
   * has not been configured for this field, fall back to the configured
   * default or the solr default (GapFragmenter).
   * 
   * @param fieldName The name of the field
   * @param params The params controlling Highlighting
   * @return An appropriate Fragmenter.
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


    // Highlight each document
    DocIterator iterator = docs.iterator();
    for (int i = 0; i < docs.size(); i++) {
       int docId = iterator.nextDoc();
       Document doc = searcher.doc(docId, fset);
       NamedList docSummaries = new SimpleOrderedMap();
       for (String fieldName : fieldNames) {
          fieldName = fieldName.trim();
          String[] docTexts = doc.getValues(fieldName);
          if (docTexts == null) continue;
          
          TokenStream tstream = null;
          int numFragments = getMaxSnippets(fieldName, params);
          boolean mergeContiguousFragments = isMergeContiguousFragments(fieldName, params);

          String[] summaries = null;
          List<TextFragment> frags = new ArrayList<TextFragment>();
          TermOffsetsTokenStream tots = null;
          for (int j = 0; j < docTexts.length; j++) {
            // create TokenStream
            try {
              // attempt term vectors
              if( tots == null ) {
                TokenStream tvStream = TokenSources.getTokenStream(searcher.getReader(), docId, fieldName);
                if (tvStream != null) {
                  tots = new TermOffsetsTokenStream(tvStream);
                  tstream = tots.getMultiValuedTokenStream( docTexts[j].length() );
                } else {
                  // fall back to analyzer
                  tstream = createAnalyzerTStream(schema, fieldName, docTexts[j]);
                }
              }
            }
            catch (IllegalArgumentException e) {
              // fall back to analyzer
              tstream = createAnalyzerTStream(schema, fieldName, docTexts[j]);
            }
                         
            Highlighter highlighter;
            if (Boolean.valueOf(req.getParams().get(HighlightParams.USE_PHRASE_HIGHLIGHTER, "true"))) {
              // TODO: this is not always necessary - eventually we would like to avoid this wrap
              //       when it is not needed.
              tstream = new CachingTokenFilter(tstream);
              
              // get highlighter
              highlighter = getPhraseHighlighter(query, fieldName, req, (CachingTokenFilter) tstream);
               
              // after highlighter initialization, reset tstream since construction of highlighter already used it
              tstream.reset();
            }
            else {
              // use "the old way"
              highlighter = getHighlighter(query, fieldName, req);
            }
            
            int maxCharsToAnalyze = params.getFieldInt(fieldName,
                HighlightParams.MAX_CHARS,
                Highlighter.DEFAULT_MAX_CHARS_TO_ANALYZE);
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
              String alternateField = req.getParams().getFieldParam(fieldName, HighlightParams.ALTERNATE_FIELD);
              if (alternateField != null && alternateField.length() > 0) {
                String[] altTexts = doc.getValues(alternateField);
                if (altTexts != null && altTexts.length > 0){
                  int alternateFieldLen = req.getParams().getFieldInt(fieldName, HighlightParams.ALTERNATE_FIELD_LENGTH,0);
                  if( alternateFieldLen <= 0 ){
                    docSummaries.add(fieldName, altTexts);
                  }
                  else{
                    List<String> altList = new ArrayList<String>();
                    int len = 0;
                    for( String altText: altTexts ){
                      altList.add( len + altText.length() > alternateFieldLen ?
                                   new String(altText.substring( 0, alternateFieldLen - len )) : altText );
                      len += altText.length();
                      if( len >= alternateFieldLen ) break;
                    }
                    docSummaries.add(fieldName, altList);
                  }
                }
              }
           }
 
        }
        String printId = schema.printableUniqueKey(doc);
        fragments.add(printId == null ? null : printId, docSummaries);
     }
     return fragments;
  }

  private TokenStream createAnalyzerTStream(IndexSchema schema, String fieldName, String docText) throws IOException {

    TokenStream tstream;
    TokenStream ts = schema.getAnalyzer().reusableTokenStream(fieldName, new StringReader(docText));
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
class TokenOrderingFilter extends TokenFilter {
  private final int windowSize;
  private final LinkedList<Token> queue = new LinkedList<Token>();
  private boolean done=false;

  protected TokenOrderingFilter(TokenStream input, int windowSize) {
    super(input);
    this.windowSize = windowSize;
  }

  @Override
  public Token next() throws IOException {
    while (!done && queue.size() < windowSize) {
      Token newTok = input.next();
      if (newTok==null) {
        done=true;
        break;
      }

      // reverse iterating for better efficiency since we know the
      // list is already sorted, and most token start offsets will be too.
      ListIterator<Token> iter = queue.listIterator(queue.size());
      while(iter.hasPrevious()) {
        if (newTok.startOffset() >= iter.previous().startOffset()) {
          // insertion will be before what next() would return (what
          // we just compared against), so move back one so the insertion
          // will be after.
          iter.next();
          break;
        }
      }
      iter.add(newTok);
    }

    return queue.isEmpty() ? null : queue.removeFirst();
  }
}

class TermOffsetsTokenStream {

  TokenStream bufferedTokenStream = null;
  Token bufferedToken;
  int startOffset;
  int endOffset;

  public TermOffsetsTokenStream( TokenStream tstream ){
    bufferedTokenStream = tstream;
    startOffset = 0;
    bufferedToken = null;
  }

  public TokenStream getMultiValuedTokenStream( final int length ){
    endOffset = startOffset + length;
    return new TokenStream(){
      Token token;
      public Token next() throws IOException {
        while( true ){
          if( bufferedToken == null )
            bufferedToken = bufferedTokenStream.next();
          if( bufferedToken == null ) return null;
          if( startOffset <= bufferedToken.startOffset() &&
              bufferedToken.endOffset() <= endOffset ){
            token = bufferedToken;
            bufferedToken = null;
            token.setStartOffset( token.startOffset() - startOffset );
            token.setEndOffset( token.endOffset() - startOffset );
            return token;
          }
          else if( bufferedToken.endOffset() > endOffset ){
            startOffset += length + 1;
            return null;
          }
          bufferedToken = null;
        }
      }
    };
  }
}
