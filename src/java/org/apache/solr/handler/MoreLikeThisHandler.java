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

import java.io.IOException;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.similar.MoreLikeThis;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.MoreLikeThisParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.MoreLikeThisParams.TermStyle;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SimpleFacets;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.DocList;
import org.apache.solr.search.DocListAndSet;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.SolrPluginUtils;

/**
 * Solr MoreLikeThis --
 * 
 * Return similar documents either based on a single document or based on posted text.
 * 
 * @since solr 1.3
 */
public class MoreLikeThisHandler extends RequestHandlerBase  
{
  // Pattern is thread safe -- TODO? share this with general 'fl' param
  private static final Pattern splitList = Pattern.compile(",| ");
  
  @Override
  public void init(NamedList args) {
    super.init(args);
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception 
  {
    RequestHandlerUtils.addExperimentalFormatWarning( rsp );
    
    SolrParams params = req.getParams();
    SolrIndexSearcher searcher = req.getSearcher();
    
    // Parse Required Params
    // This will either have a single Reader or valid query
    Reader reader = null;
    String q = params.get( CommonParams.Q );
    if( q == null || q.trim().length() <1 ) {
      Iterable<ContentStream> streams = req.getContentStreams();
      if( streams != null ) {
        Iterator<ContentStream> iter = streams.iterator();
        if( iter.hasNext() ) {
          reader = iter.next().getReader();
        }
        if( iter.hasNext() ) {
          throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, 
              "MoreLikeThis does not support multiple ContentStreams" );
        }
      }
    }

    MoreLikeThisHelper mlt = new MoreLikeThisHelper( params, searcher );
    List<Query> filters = SolrPluginUtils.parseFilterQueries(req);
    
    // Hold on to the interesting terms if relevant
    TermStyle termStyle = TermStyle.get( params.get( MoreLikeThisParams.INTERESTING_TERMS ) );
    List<InterestingTerm> interesting = (termStyle == TermStyle.NONE )
      ? null : new ArrayList<InterestingTerm>( mlt.mlt.getMaxQueryTerms() );
    
    // What fields do we need to return
    String fl = params.get(CommonParams.FL);
    int flags = 0; 
    if (fl != null) {
      flags |= SolrPluginUtils.setReturnFields(fl, rsp);
    }

    int start = params.getInt( CommonParams.START, 0 );
    int rows  = params.getInt( CommonParams.ROWS, 10 );
    
    DocListAndSet mltDocs = null;
    
    // Find documents MoreLikeThis - either with a reader or a query
    //--------------------------------------------------------------------------------
    if( reader != null ) {
      mltDocs = mlt.getMoreLikeThis( reader, start, rows, filters, interesting, flags );
    }
    else if( q != null ) {
      // Matching options
      boolean includeMatch = params.getBool( MoreLikeThisParams.MATCH_INCLUDE, true );
      int matchOffset = params.getInt( MoreLikeThisParams.MATCH_OFFSET, 0 );
      
      // Find the base match  
      Query query = QueryParsing.parseQuery(q, params.get(CommonParams.DF), params, req.getSchema());
      DocList match = searcher.getDocList(query, null, null, matchOffset, 1, flags ); // only get the first one...
      if( includeMatch ) {
        rsp.add( "match", match );
      }

      // This is an iterator, but we only handle the first match
      DocIterator iterator = match.iterator();
      if( iterator.hasNext() ) {
        // do a MoreLikeThis query for each document in results
        int id = iterator.nextDoc();
        mltDocs = mlt.getMoreLikeThis( id, start, rows, filters, interesting, flags );
      }
    }
    else {
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, 
          "MoreLikeThis requires either a query (?q=) or text to find similar documents." );
    }
    if( mltDocs == null ) {
      mltDocs = new DocListAndSet(); // avoid NPE
    }
    rsp.add( "response", mltDocs.docList );
    
  
    if( interesting != null ) {
      if( termStyle == TermStyle.DETAILS ) {
        NamedList<Float> it = new NamedList<Float>();
        for( InterestingTerm t : interesting ) {
          it.add( t.term.toString(), t.boost );
        }
        rsp.add( "interestingTerms", it );
      }
      else {
        List<String> it = new ArrayList<String>( interesting.size() );
        for( InterestingTerm t : interesting ) {
          it.add( t.term.text());
        }
        rsp.add( "interestingTerms", it );
      }
    }
    
    // maybe facet the results
    if (params.getBool(FacetParams.FACET,false)) {
      if( mltDocs.docSet == null ) {
        rsp.add( "facet_counts", null );
      }
      else {
        SimpleFacets f = new SimpleFacets(req, mltDocs.docSet, params );
        rsp.add( "facet_counts", f.getFacetCounts() );
      }
    }
    
    // Copied from StandardRequestHandler... perhaps it should be added to doStandardDebug?
    try {
      NamedList<Object> dbg = SolrPluginUtils.doStandardDebug(req, q, mlt.mltquery, mltDocs.docList );
      if (null != dbg) {
        if (null != filters) {
          dbg.add("filter_queries",req.getParams().getParams(CommonParams.FQ));
          List<String> fqs = new ArrayList<String>(filters.size());
          for (Query fq : filters) {
            fqs.add(QueryParsing.toString(fq, req.getSchema()));
          }
          dbg.add("parsed_filter_queries",fqs);
        }
        rsp.add("debug", dbg);
      }
    } catch (Exception e) {
      SolrException.logOnce(SolrCore.log, "Exception during debug", e);
      rsp.add("exception_during_debug", SolrException.toStr(e));
    }
  }
  
  public static class InterestingTerm
  {
    public Term term;
    public float boost;
        
    public static Comparator<InterestingTerm> BOOST_ORDER = new Comparator<InterestingTerm>() {
      public int compare(InterestingTerm t1, InterestingTerm t2) {
        float d = t1.boost - t2.boost;
        if( d == 0 ) {
          return 0;
        }
        return (d>0)?1:-1;
      }
    };
  }
  
  /**
   * Helper class for MoreLikeThis that can be called from other request handlers
   */
  public static class MoreLikeThisHelper 
  { 
    final SolrIndexSearcher searcher;
    final MoreLikeThis mlt;
    final IndexReader reader;
    final SchemaField uniqueKeyField;
    final boolean needDocSet;
    
    Query mltquery;  // expose this for debugging
    
    public MoreLikeThisHelper( SolrParams params, SolrIndexSearcher searcher )
    {
      this.searcher = searcher;
      this.reader = searcher.getReader();
      this.uniqueKeyField = searcher.getSchema().getUniqueKeyField();
      this.needDocSet = params.getBool(FacetParams.FACET,false);
      
      SolrParams required = params.required();
      String[] fields = splitList.split( required.get(MoreLikeThisParams.SIMILARITY_FIELDS) );
      if( fields.length < 1 ) {
        throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, 
            "MoreLikeThis requires at least one similarity field: "+MoreLikeThisParams.SIMILARITY_FIELDS );
      }
      
      this.mlt = new MoreLikeThis( reader ); // TODO -- after LUCENE-896, we can use , searcher.getSimilarity() );
      mlt.setFieldNames(fields);
      mlt.setAnalyzer( searcher.getSchema().getAnalyzer() );
      
      // configurable params
      mlt.setMinTermFreq(       params.getInt(MoreLikeThisParams.MIN_TERM_FREQ,         MoreLikeThis.DEFAULT_MIN_TERM_FREQ));
      mlt.setMinDocFreq(        params.getInt(MoreLikeThisParams.MIN_DOC_FREQ,          MoreLikeThis.DEFAULT_MIN_DOC_FREQ));
      mlt.setMinWordLen(        params.getInt(MoreLikeThisParams.MIN_WORD_LEN,          MoreLikeThis.DEFAULT_MIN_WORD_LENGTH));
      mlt.setMaxWordLen(        params.getInt(MoreLikeThisParams.MAX_WORD_LEN,          MoreLikeThis.DEFAULT_MAX_WORD_LENGTH));
      mlt.setMaxQueryTerms(     params.getInt(MoreLikeThisParams.MAX_QUERY_TERMS,       MoreLikeThis.DEFAULT_MAX_QUERY_TERMS));
      mlt.setMaxNumTokensParsed(params.getInt(MoreLikeThisParams.MAX_NUM_TOKENS_PARSED, MoreLikeThis.DEFAULT_MAX_NUM_TOKENS_PARSED));
      mlt.setBoost(            params.getBool(MoreLikeThisParams.BOOST, false ) );
    }
    
    public DocListAndSet getMoreLikeThis( int id, int start, int rows, List<Query> filters, List<InterestingTerm> terms, int flags ) throws IOException
    {
      Document doc = reader.document(id);
      mltquery = mlt.like(id);
      if( terms != null ) {
        fillInteristingTermsFromMLTQuery( mltquery, terms );
      }

      // exclude current document from results
      BooleanQuery mltQuery = new BooleanQuery();
      mltQuery.add(mltquery, BooleanClause.Occur.MUST);
      mltQuery.add(
          new TermQuery(new Term(uniqueKeyField.getName(), doc.get(uniqueKeyField.getName()))), 
            BooleanClause.Occur.MUST_NOT);
      
      DocListAndSet results = new DocListAndSet();
      if (this.needDocSet) {
        results = searcher.getDocListAndSet(mltQuery, filters, null, start, rows, flags);
      } else {
        results.docList = searcher.getDocList(mltQuery, filters, null, start, rows, flags);
      }
      return results;
    }

    public DocListAndSet getMoreLikeThis( Reader reader, int start, int rows, List<Query> filters, List<InterestingTerm> terms, int flags ) throws IOException
    {
      mltquery = mlt.like(reader);
      if( terms != null ) {
        fillInteristingTermsFromMLTQuery( mltquery, terms );
      }
      DocListAndSet results = new DocListAndSet();
      if (this.needDocSet) {
        results = searcher.getDocListAndSet(mltquery, filters, null, start, rows, flags);
      } else {
        results.docList = searcher.getDocList(mltquery, filters, null, start, rows, flags);
      }
      return results;
    }
    
    public NamedList<DocList> getMoreLikeThese( DocList docs, int rows, int flags ) throws IOException
    {
      IndexSchema schema = searcher.getSchema();
      NamedList<DocList> mlt = new SimpleOrderedMap<DocList>();
      DocIterator iterator = docs.iterator();
      while( iterator.hasNext() ) {
        int id = iterator.nextDoc();
        
        DocListAndSet sim = getMoreLikeThis( id, 0, rows, null, null, flags );
        String name = schema.printableUniqueKey( reader.document( id ) );

        mlt.add(name, sim.docList);
      }
      return mlt;
    }
    
    private void fillInteristingTermsFromMLTQuery( Query query, List<InterestingTerm> terms )
    { 
      List clauses = ((BooleanQuery)mltquery).clauses();
      for( Object o : clauses ) {
        TermQuery q = (TermQuery)((BooleanClause)o).getQuery();
        InterestingTerm it = new InterestingTerm();
        it.boost = q.getBoost();
        it.term = q.getTerm();
        terms.add( it );
      } 
      // alternatively we could use
      // mltquery.extractTerms( terms );
    }
    
    public MoreLikeThis getMoreLikeThis()
    {
      return mlt;
    }
  }
  
  
  //////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getVersion() {
    return "$Revision$";
  }

  @Override
  public String getDescription() {
    return "Solr MoreLikeThis";
  }

  @Override
  public String getSourceId() {
    return "$Id$";
  }

  @Override
  public String getSource() {
    return "$URL$";
  }

  @Override
  public URL[] getDocs() {
    try {
      return new URL[] { new URL("http://wiki.apache.org/solr/MoreLikeThis") };
    }
    catch( MalformedURLException ex ) { return null; }
  }
}
