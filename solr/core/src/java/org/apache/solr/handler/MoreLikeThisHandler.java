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
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.ExitableDirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.mlt.MoreLikeThis;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.StringUtils;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.MoreLikeThisParams.TermStyle;
import org.apache.solr.common.params.MoreLikeThisParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.FacetComponent;
import org.apache.solr.request.SimpleFacets;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.DocListAndSet;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.ReturnFields;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SolrQueryTimeoutImpl;
import org.apache.solr.search.SolrReturnFields;
import org.apache.solr.search.SortSpec;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.util.SolrPluginUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static final String ERR_MSG_QUERY_OR_TEXT_REQUIRED =
      "MoreLikeThis requires either a query (?q=) or text to find similar documents.";

  static final String ERR_MSG_SINGLE_STREAM_ONLY =
      "MoreLikeThis does not support multiple ContentStreams";

  @Override
  public void init(@SuppressWarnings({"rawtypes"})NamedList args) {
    super.init(args);
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception 
  {
    SolrParams params = req.getParams();

    SolrQueryTimeoutImpl.set(req);
      try {

        // Set field flags
        ReturnFields returnFields = new SolrReturnFields(req);
        rsp.setReturnFields(returnFields);
        int flags = 0;
        if (returnFields.wantsScore()) {
          flags |= SolrIndexSearcher.GET_SCORES;
        }

        String defType = params.get(QueryParsing.DEFTYPE, QParserPlugin.DEFAULT_QTYPE);
        String q = params.get(CommonParams.Q);
        Query query = null;
        SortSpec sortSpec = null;
        List<Query> filters = null;

        try {
          if (q != null) {
            QParser parser = QParser.getParser(q, defType, req);
            query = parser.getQuery();
            sortSpec = parser.getSortSpec(true);
          }

          String[] fqs = req.getParams().getParams(CommonParams.FQ);
          if (fqs != null && fqs.length != 0) {
            filters = new ArrayList<>();
            for (String fq : fqs) {
              if (fq != null && fq.trim().length() != 0) {
                QParser fqp = QParser.getParser(fq, req);
                filters.add(fqp.getQuery());
              }
            }
          }
        } catch (SyntaxError e) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
        }

        SolrIndexSearcher searcher = req.getSearcher();

        MoreLikeThisHelper mlt = new MoreLikeThisHelper(params, searcher);

        // Hold on to the interesting terms if relevant
        TermStyle termStyle = TermStyle.get(params.get(MoreLikeThisParams.INTERESTING_TERMS));
        List<InterestingTerm> interesting = (termStyle == TermStyle.NONE)
            ? null : new ArrayList<>(mlt.mlt.getMaxQueryTerms());

        DocListAndSet mltDocs = null;

        // Parse Required Params
        // This will either have a single Reader or valid query
        Reader reader = null;
        try {
          if (q == null || q.trim().length() < 1) {
            Iterable<ContentStream> streams = req.getContentStreams();
            if (streams != null) {
              Iterator<ContentStream> iter = streams.iterator();
              if (iter.hasNext()) {
                reader = iter.next().getReader();
              }
              if (iter.hasNext()) {
                throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                    ERR_MSG_SINGLE_STREAM_ONLY);
              }
            }
          }

          int start = params.getInt(CommonParams.START, CommonParams.START_DEFAULT);
          int rows = params.getInt(CommonParams.ROWS, CommonParams.ROWS_DEFAULT);

          // Find documents MoreLikeThis - either with a reader or a query
          // --------------------------------------------------------------------------------
          if (reader != null) {
            mltDocs = mlt.getMoreLikeThis(reader, start, rows, filters,
                interesting, flags);
          } else if (q != null) {
            // Matching options
            boolean includeMatch = params.getBool(MoreLikeThisParams.MATCH_INCLUDE,
                true);
            int matchOffset = params.getInt(MoreLikeThisParams.MATCH_OFFSET, 0);
            // Find the base match
            DocList match = searcher.getDocList(query, null, null, matchOffset, 1,
                flags); // only get the first one...
            if (includeMatch) {
              rsp.add("match", match);
            }

            // This is an iterator, but we only handle the first match
            DocIterator iterator = match.iterator();
            if (iterator.hasNext()) {
              // do a MoreLikeThis query for each document in results
              int id = iterator.nextDoc();
              mltDocs = mlt.getMoreLikeThis(id, start, rows, filters, interesting,
                  flags);
            }
          } else {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                ERR_MSG_QUERY_OR_TEXT_REQUIRED);
          }

        } finally {
          if (reader != null) {
            reader.close();
          }
        }

        if (mltDocs == null) {
          mltDocs = new DocListAndSet(); // avoid NPE
        }
        rsp.addResponse(mltDocs.docList);


        if (interesting != null) {
          if (termStyle == TermStyle.DETAILS) {
            NamedList<Float> it = new NamedList<>();
            for (InterestingTerm t : interesting) {
              it.add(t.term.toString(), t.boost);
            }
            rsp.add("interestingTerms", it);
          } else {
            List<String> it = new ArrayList<>(interesting.size());
            for (InterestingTerm t : interesting) {
              it.add(t.term.text());
            }
            rsp.add("interestingTerms", it);
          }
        }

        // maybe facet the results
        if (params.getBool(FacetParams.FACET, false)) {
          if (mltDocs.docSet == null) {
            rsp.add("facet_counts", null);
          } else {
            SimpleFacets f = new SimpleFacets(req, mltDocs.docSet, params);
            rsp.add("facet_counts", FacetComponent.getFacetCounts(f));
          }
        }
        boolean dbg = req.getParams().getBool(CommonParams.DEBUG_QUERY, false);

        boolean dbgQuery = false, dbgResults = false;
        if (dbg == false) {//if it's true, we are doing everything anyway.
          String[] dbgParams = req.getParams().getParams(CommonParams.DEBUG);
          if (dbgParams != null) {
            for (String dbgParam : dbgParams) {
              if (dbgParam.equals(CommonParams.QUERY)) {
                dbgQuery = true;
              } else if (dbgParam.equals(CommonParams.RESULTS)) {
                dbgResults = true;
              }
            }
          }
        } else {
          dbgQuery = true;
          dbgResults = true;
        }
        // TODO resolve duplicated code with DebugComponent.  Perhaps it should be added to doStandardDebug?
        if (dbg == true) {
          try {
            @SuppressWarnings({"unchecked"})
            NamedList<Object> dbgInfo = SolrPluginUtils.doStandardDebug(req, q, mlt.getRawMLTQuery(), mltDocs.docList, dbgQuery, dbgResults);
            if (null != dbgInfo) {
              if (null != filters) {
                dbgInfo.add("filter_queries", req.getParams().getParams(CommonParams.FQ));
                List<String> fqs = new ArrayList<>(filters.size());
                for (Query fq : filters) {
                  fqs.add(QueryParsing.toString(fq, req.getSchema()));
                }
                dbgInfo.add("parsed_filter_queries", fqs);
              }
              rsp.add("debug", dbgInfo);
            }
          } catch (Exception e) {
            SolrException.log(log, "Exception during debug", e);
            rsp.add("exception_during_debug", SolrException.toStr(e));
          }
        }
      } catch (ExitableDirectoryReader.ExitingReaderException ex) {
        log.warn( "Query: {}; ", req.getParamString(), ex);
      } finally {
        SolrQueryTimeoutImpl.reset();
      }
  }
  
  public static class InterestingTerm
  {
    public Term term;
    public float boost;

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
    Map<String,Float> boostFields;
    
    public MoreLikeThisHelper( SolrParams params, SolrIndexSearcher searcher )
    {
      this.searcher = searcher;
      this.reader = searcher.getIndexReader();
      this.uniqueKeyField = searcher.getSchema().getUniqueKeyField();
      this.needDocSet = params.getBool(FacetParams.FACET,false);
      
      SolrParams required = params.required();
      String[] fl = required.getParams(MoreLikeThisParams.SIMILARITY_FIELDS);
      List<String> list = new ArrayList<>();
      for (String f : fl) {
        if (!StringUtils.isEmpty(f))  {
          String[] strings = splitList.split(f);
          for (String string : strings) {
            if (!StringUtils.isEmpty(string)) {
              list.add(string);
            }
          }
        }
      }
      String[] fields = list.toArray(new String[list.size()]);
      if( fields.length < 1 ) {
        throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, 
            "MoreLikeThis requires at least one similarity field: "+MoreLikeThisParams.SIMILARITY_FIELDS );
      }
      
      this.mlt = new MoreLikeThis( reader ); // TODO -- after LUCENE-896, we can use , searcher.getSimilarity() );
      mlt.setFieldNames(fields);
      mlt.setAnalyzer( searcher.getSchema().getIndexAnalyzer() );
      
      // configurable params
      
      mlt.setMinTermFreq(       params.getInt(MoreLikeThisParams.MIN_TERM_FREQ,         MoreLikeThis.DEFAULT_MIN_TERM_FREQ));
      mlt.setMinDocFreq(        params.getInt(MoreLikeThisParams.MIN_DOC_FREQ,          MoreLikeThis.DEFAULT_MIN_DOC_FREQ));
      mlt.setMaxDocFreq(        params.getInt(MoreLikeThisParams.MAX_DOC_FREQ,          MoreLikeThis.DEFAULT_MAX_DOC_FREQ));
      mlt.setMinWordLen(        params.getInt(MoreLikeThisParams.MIN_WORD_LEN,          MoreLikeThis.DEFAULT_MIN_WORD_LENGTH));
      mlt.setMaxWordLen(        params.getInt(MoreLikeThisParams.MAX_WORD_LEN,          MoreLikeThis.DEFAULT_MAX_WORD_LENGTH));
      mlt.setMaxQueryTerms(     params.getInt(MoreLikeThisParams.MAX_QUERY_TERMS,       MoreLikeThis.DEFAULT_MAX_QUERY_TERMS));
      mlt.setMaxNumTokensParsed(params.getInt(MoreLikeThisParams.MAX_NUM_TOKENS_PARSED, MoreLikeThis.DEFAULT_MAX_NUM_TOKENS_PARSED));
      mlt.setBoost(            params.getBool(MoreLikeThisParams.BOOST, false ) );
      
      // There is no default for maxDocFreqPct. Also, it's a bit oddly expressed as an integer value 
      // (percentage of the collection's documents count). We keep Lucene's convention here. 
      if (params.getInt(MoreLikeThisParams.MAX_DOC_FREQ_PCT) != null) {
        mlt.setMaxDocFreqPct(params.getInt(MoreLikeThisParams.MAX_DOC_FREQ_PCT));
      }

      boostFields = SolrPluginUtils.parseFieldBoosts(params.getParams(MoreLikeThisParams.QF));
    }
    
    private Query rawMLTQuery;
    private Query boostedMLTQuery;
    private BooleanQuery realMLTQuery;
    
    public Query getRawMLTQuery(){
      return rawMLTQuery;
    }
    
    public Query getBoostedMLTQuery(){
      return boostedMLTQuery;
    }
    
    public Query getRealMLTQuery(){
      return realMLTQuery;
    }
    
    private Query getBoostedQuery(Query mltquery) {
      BooleanQuery boostedQuery = (BooleanQuery)mltquery;
      if (boostFields.size() > 0) {
        BooleanQuery.Builder newQ = new BooleanQuery.Builder();
        newQ.setMinimumNumberShouldMatch(boostedQuery.getMinimumNumberShouldMatch());
        for (BooleanClause clause : boostedQuery) {
          Query q = clause.getQuery();
          float originalBoost = 1f;
          if (q instanceof BoostQuery) {
            BoostQuery bq = (BoostQuery) q;
            q = bq.getQuery();
            originalBoost = bq.getBoost();
          }
          Float fieldBoost = boostFields.get(((TermQuery) q).getTerm().field());
          q = ((fieldBoost != null) ? new BoostQuery(q, fieldBoost * originalBoost) : clause.getQuery());
          newQ.add(q, clause.getOccur());
        }
        boostedQuery = newQ.build();
      }
      return boostedQuery;
    }
    
    public DocListAndSet getMoreLikeThis( int id, int start, int rows, List<Query> filters, List<InterestingTerm> terms, int flags ) throws IOException
    {
      Document doc = reader.document(id);
      rawMLTQuery = mlt.like(id);
      boostedMLTQuery = getBoostedQuery( rawMLTQuery );
      if( terms != null ) {
        fillInterestingTermsFromMLTQuery( boostedMLTQuery, terms );
      }

      // exclude current document from results
      BooleanQuery.Builder realMLTQuery = new BooleanQuery.Builder();
      realMLTQuery.add(boostedMLTQuery, BooleanClause.Occur.MUST);
      realMLTQuery.add(
          new TermQuery(new Term(uniqueKeyField.getName(), uniqueKeyField.getType().storedToIndexed(doc.getField(uniqueKeyField.getName())))), 
            BooleanClause.Occur.MUST_NOT);
      this.realMLTQuery = realMLTQuery.build();
      
      DocListAndSet results = new DocListAndSet();
      if (this.needDocSet) {
        results = searcher.getDocListAndSet(this.realMLTQuery, filters, null, start, rows, flags);
      } else {
        results.docList = searcher.getDocList(this.realMLTQuery, filters, null, start, rows, flags);
      }
      return results;
    }

    public DocListAndSet getMoreLikeThis( Reader reader, int start, int rows, List<Query> filters, List<InterestingTerm> terms, int flags ) throws IOException
    {
      // SOLR-5351: if only check against a single field, use the reader directly. Otherwise we
      // repeat the stream's content for multiple fields so that query terms can be pulled from any
      // of those fields.
      String [] fields = mlt.getFieldNames();
      if (fields.length == 1) {
        rawMLTQuery = mlt.like(fields[0], reader);
      } else {
        CharsRefBuilder buffered = new CharsRefBuilder();
        char [] chunk = new char [1024];
        int len;
        while ((len = reader.read(chunk)) >= 0) {
          buffered.append(chunk, 0, len);
        }

        Collection<Object> streamValue = Collections.singleton(buffered.get().toString());
        Map<String, Collection<Object>> multifieldDoc = new HashMap<>(fields.length);
        for (String field : fields) {
          multifieldDoc.put(field, streamValue);
        }

        rawMLTQuery = mlt.like(multifieldDoc);
      }

      boostedMLTQuery = getBoostedQuery( rawMLTQuery );
      if (terms != null) {
        fillInterestingTermsFromMLTQuery( boostedMLTQuery, terms );
      }
      DocListAndSet results = new DocListAndSet();
      if (this.needDocSet) {
        results = searcher.getDocListAndSet( boostedMLTQuery, filters, null, start, rows, flags);
      } else {
        results.docList = searcher.getDocList( boostedMLTQuery, filters, null, start, rows, flags);
      }
      return results;
    }

    public NamedList<BooleanQuery> getMoreLikeTheseQuery(DocList docs)
        throws IOException {
      IndexSchema schema = searcher.getSchema();
      NamedList<BooleanQuery> result = new NamedList<>();
      DocIterator iterator = docs.iterator();
      while (iterator.hasNext()) {
        int id = iterator.nextDoc();
        String uniqueId = schema.printableUniqueKey(reader.document(id));

        BooleanQuery mltquery = (BooleanQuery) mlt.like(id);
        if (mltquery.clauses().size() == 0) {
          return result;
        }
        mltquery = (BooleanQuery) getBoostedQuery(mltquery);
        
        // exclude current document from results
        BooleanQuery.Builder mltQuery = new BooleanQuery.Builder();
        mltQuery.add(mltquery, BooleanClause.Occur.MUST);
        
        mltQuery.add(
            new TermQuery(new Term(uniqueKeyField.getName(), uniqueId)), BooleanClause.Occur.MUST_NOT);
        result.add(uniqueId, mltQuery.build());
      }

      return result;
    }
    
    private void fillInterestingTermsFromMLTQuery( Query query, List<InterestingTerm> terms )
    { 
      Collection<BooleanClause> clauses = ((BooleanQuery)query).clauses();
      for( BooleanClause o : clauses ) {
        Query q = o.getQuery();
        float boost = 1f;
        if (q instanceof BoostQuery) {
          BoostQuery bq = (BoostQuery) q;
          q = bq.getQuery();
          boost = bq.getBoost();
        }
        InterestingTerm it = new InterestingTerm();
        it.boost = boost;
        it.term = ((TermQuery) q).getTerm();
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
  public String getDescription() {
    return "Solr MoreLikeThis";
  }
}
