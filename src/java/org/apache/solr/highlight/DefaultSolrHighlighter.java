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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import javax.xml.xpath.XPathConstants;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.highlight.Formatter;
import org.apache.lucene.search.highlight.Fragmenter;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SpanScorer;
import org.apache.lucene.search.highlight.TextFragment;
import org.apache.lucene.search.highlight.TokenSources;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.HighlightParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.Config;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.plugin.NamedListPluginLoader;
import org.w3c.dom.NodeList;

/**
 * 
 * @since solr 1.3
 */
public class DefaultSolrHighlighter extends SolrHighlighter
{
  
  public void initalize( final Config config )
  {
    formatters.clear();
    fragmenters.clear();
    
    // Load the fragmenters
    String xpath = "highlighting/fragmenter";
    NamedListPluginLoader<SolrFragmenter> fragloader = new NamedListPluginLoader<SolrFragmenter>( xpath, fragmenters );
    SolrFragmenter frag = fragloader.load( config.getResourceLoader(), (NodeList)config.evaluate( xpath, XPathConstants.NODESET ) );
    if( frag == null ) {
      frag = new GapFragmenter();
    }
    fragmenters.put( "", frag );
    fragmenters.put( null, frag );
    
    // Load the formatters
    xpath = "highlighting/formatter";
    NamedListPluginLoader<SolrFormatter> fmtloader = new NamedListPluginLoader<SolrFormatter>( xpath, formatters );
    SolrFormatter fmt = fmtloader.load( config.getResourceLoader(), (NodeList)config.evaluate( xpath, XPathConstants.NODESET ) );
    if( fmt == null ) {
      fmt = new HtmlFormatter();
    }
    formatters.put( "", fmt );
    formatters.put( null, fmt );
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
    highlighter.setMaxDocBytesToAnalyze(params.getFieldInt(
        fieldName, HighlightParams.MAX_CHARS, 
        Highlighter.DEFAULT_MAX_DOC_BYTES_TO_ANALYZE));

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
     highlighter.setMaxDocBytesToAnalyze(params.getFieldInt(
           fieldName, HighlightParams.MAX_CHARS, 
           Highlighter.DEFAULT_MAX_DOC_BYTES_TO_ANALYZE));
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
  private SpanScorer getSpanQueryScorer(Query query, String fieldName, CachingTokenFilter tokenStream, SolrQueryRequest request) throws IOException {
    boolean reqFieldMatch = request.getParams().getFieldBool(fieldName, HighlightParams.FIELD_MATCH, false);
    if (reqFieldMatch) {
      return new SpanScorer(query, fieldName, tokenStream);
    }
    else {
      return new SpanScorer(query, null, tokenStream);
    }
  }

  /**
   * Return a QueryScorer suitable for this Query and field.
   * @param query The current query
   * @param fieldName The name of the field
   * @param request The SolrQueryRequest
   */
  protected QueryScorer getQueryScorer(Query query, String fieldName, SolrQueryRequest request) {
     boolean reqFieldMatch = request.getParams().getFieldBool(fieldName, HighlightParams.FIELD_MATCH, false);
     if (reqFieldMatch) {
        return new QueryScorer(query, request.getSearcher().getReader(), fieldName);
     }
     else {
        return new QueryScorer(query);
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
     Document[] readDocs = new Document[docs.size()];
     {
       // pre-fetch documents using the Searcher's doc cache
       Set<String> fset = new HashSet<String>();
       for(String f : fieldNames) { fset.add(f); }
       // fetch unique key if one exists.
       SchemaField keyField = schema.getUniqueKeyField();
       if(null != keyField)
         fset.add(keyField.getName());  
       searcher.readDocs(readDocs, docs, fset);
     }


    // Highlight each document
    DocIterator iterator = docs.iterator();
    for (int i = 0; i < docs.size(); i++) {
       int docId = iterator.nextDoc();
       Document doc = readDocs[i];
       NamedList docSummaries = new SimpleOrderedMap();
       for (String fieldName : fieldNames) {
          fieldName = fieldName.trim();
          String[] docTexts = doc.getValues(fieldName);
          if (docTexts == null) continue;
          
          TokenStream tstream = null;

          // create TokenStream
          if (docTexts.length == 1) {
            // single-valued field
            try {
              // attempt term vectors
              tstream = TokenSources.getTokenStream(searcher.getReader(), docId, fieldName);
            }
            catch (IllegalArgumentException e) {
              // fall back to anaylzer
              tstream = new TokenOrderingFilter(schema.getAnalyzer().tokenStream(fieldName, new StringReader(docTexts[0])), 10);
            }
          }
          else {
            // multi-valued field
            tstream = new MultiValueTokenStream(fieldName, docTexts, schema.getAnalyzer(), true);
          }
          
          Highlighter highlighter;
          
          if (Boolean.valueOf(req.getParams().get(HighlightParams.USE_PHRASE_HIGHLIGHTER))) {
            // wrap CachingTokenFilter around TokenStream for reuse
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

          int numFragments = getMaxSnippets(fieldName, params);
          boolean mergeContiguousFragments = isMergeContiguousFragments(fieldName, params);

           String[] summaries = null;
           TextFragment[] frag;
           if (docTexts.length == 1) {
              frag = highlighter.getBestTextFragments(tstream, docTexts[0], mergeContiguousFragments, numFragments);
           }
           else {
               StringBuilder singleValue = new StringBuilder();
               
               for (String txt:docTexts) {
             	  singleValue.append(txt);
               }
             
              frag = highlighter.getBestTextFragments(tstream, singleValue.toString(), false, numFragments);
           }
           // convert fragments back into text
           // TODO: we can include score and position information in output as snippet attributes
           if (frag.length > 0) {
              ArrayList<String> fragTexts = new ArrayList<String>();
              for (int j = 0; j < frag.length; j++) {
                 if ((frag[j] != null) && (frag[j].getScore() > 0)) {
                    fragTexts.add(frag[j].toString());
                 }
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
                                   altText.substring( 0, alternateFieldLen - len ) : altText );
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
}

/** 
 * Creates a single TokenStream out multi-value field values.
 */
class MultiValueTokenStream extends TokenStream {
  private String fieldName;
  private String[] values;
  private Analyzer analyzer;
  private int curIndex;                  // next index into the values array
  private int curOffset;                 // offset into concatenated string
  private TokenStream currentStream;     // tokenStream currently being iterated
  private boolean orderTokenOffsets;

  /** Constructs a TokenStream for consecutively-analyzed field values
   *
   * @param fieldName name of the field
   * @param values array of field data
   * @param analyzer analyzer instance
   */
  public MultiValueTokenStream(String fieldName, String[] values, 
                               Analyzer analyzer, boolean orderTokenOffsets) {
    this.fieldName = fieldName;
    this.values = values;
    this.analyzer = analyzer;
    curIndex = -1;
    curOffset = 0;
    currentStream = null;
    this.orderTokenOffsets=orderTokenOffsets;
  }

  /** Returns the next token in the stream, or null at EOS. */
  @Override
  public Token next() throws IOException {
    int extra = 0;
    if(currentStream == null) {
      curIndex++;        
      if(curIndex < values.length) {
        currentStream = analyzer.tokenStream(fieldName, 
                                             new StringReader(values[curIndex]));
        if (orderTokenOffsets) currentStream = new TokenOrderingFilter(currentStream,10);
        // add extra space between multiple values
        if(curIndex > 0) 
          extra = analyzer.getPositionIncrementGap(fieldName);
      } else {
        return null;
      }
    }
    Token nextToken = currentStream.next();
    if(nextToken == null) {
      curOffset += values[curIndex].length();
      currentStream = null;
      return next();
    }
    // create an modified token which is the offset into the concatenated
    // string of all values
    Token offsetToken = new Token(nextToken.termText(), 
                                  nextToken.startOffset() + curOffset,
                                  nextToken.endOffset() + curOffset);
    offsetToken.setPositionIncrement(nextToken.getPositionIncrement() + extra*10);
    return offsetToken;
  }

  /**
   * Returns all values as a single String into which the Tokens index with
   * their offsets.
   */
  public String asSingleValue() {
    StringBuilder sb = new StringBuilder();
    for(String str : values)
      sb.append(str);
    return sb.toString();
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
