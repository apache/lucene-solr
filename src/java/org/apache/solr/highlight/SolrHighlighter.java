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
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import javax.xml.xpath.XPathConstants;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.highlight.Formatter;
import org.apache.lucene.search.highlight.Fragmenter;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.QueryScorer;
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
import org.apache.solr.util.SolrPluginUtils;
import org.apache.solr.util.plugin.NamedListPluginLoader;
import org.w3c.dom.NodeList;

/**
 * 
 * @since solr 1.3
 */
public class SolrHighlighter 
{
  public static Logger log = Logger.getLogger(SolrHighlighter.class.getName());
  
  // Thread safe registry
  protected final Map<String,SolrFormatter> formatters = 
    Collections.synchronizedMap( new HashMap<String, SolrFormatter>() );

  // Thread safe registry
  protected final Map<String,SolrFragmenter> fragmenters = 
    Collections.synchronizedMap( new HashMap<String, SolrFragmenter>() );
  
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
   * Check whether Highlighting is enabled for this request.
   * @param params The params controlling Highlighting
   * @return <code>true</code> if highlighting enabled, <code>false</code> if not.
   */
  public boolean isHighlightingEnabled(SolrParams params) {
    return params.getBool(HighlightParams.HIGHLIGHT, false);
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
   * Return a String array of the fields to be highlighted.
   * Falls back to the programatic defaults, or the default search field if the list of fields
   * is not specified in either the handler configuration or the request.
   * @param query The current Query
   * @param request The current SolrQueryRequest
   * @param defaultFields Programmatic default highlight fields, used if nothing is specified in the handler config or the request.
   */
  public String[] getHighlightFields(Query query, SolrQueryRequest request, String[] defaultFields) {
     String fields[] = request.getParams().getParams(HighlightParams.FIELDS);
     
     // if no fields specified in the request, or the handler, fall back to programmatic default, or default search field.
     if(emptyArray(fields)) {
        // use default search field if highlight fieldlist not specified.
        if (emptyArray(defaultFields)) {
           fields = new String[]{request.getSchema().getDefaultSearchFieldName()};
        }  
        else {
           fields = defaultFields;
        }
     }
     else if (fields.length == 1) {
        // if there's a single request/handler value, it may be a space/comma separated list
        fields = SolrPluginUtils.split(fields[0]);
     }
     
     return fields;
  }
  
  protected boolean emptyArray(String[] arr) {
     return (arr == null || arr.length == 0 || arr[0] == null || arr[0].trim().length() == 0);
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

          // get highlighter, and number of fragments for this field
          Highlighter highlighter = getHighlighter(query, fieldName, req);
          int numFragments = getMaxSnippets(fieldName, params);
          boolean mergeContiguousFragments = isMergeContiguousFragments(fieldName, params);

           String[] summaries = null;
           TextFragment[] frag;
           if (docTexts.length == 1) {
              // single-valued field
              TokenStream tstream;
              try {
                 // attempt term vectors
                 tstream = TokenSources.getTokenStream(searcher.getReader(), docId, fieldName);
              }
              catch (IllegalArgumentException e) {
                 // fall back to analyzer
                 tstream = new TokenOrderingFilter(schema.getAnalyzer().tokenStream(fieldName, new StringReader(docTexts[0])), 10);
              }
              frag = highlighter.getBestTextFragments(tstream, docTexts[0], mergeContiguousFragments, numFragments);
           }
           else {
              // multi-valued field
              MultiValueTokenStream tstream;
              tstream = new MultiValueTokenStream(fieldName, docTexts, schema.getAnalyzer(), true);
              frag = highlighter.getBestTextFragments(tstream, tstream.asSingleValue(), false, numFragments);
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
                    if (altTexts != null && altTexts.length > 0)
                       docSummaries.add(fieldName, altTexts);
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
 * Helper class which creates a single TokenStream out of values from a 
 * multi-valued field.
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


