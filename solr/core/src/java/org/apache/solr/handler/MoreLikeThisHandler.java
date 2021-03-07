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

import java.io.Reader;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.lucene.index.ExitableDirectoryReader;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.MoreLikeThisParams;
import org.apache.solr.common.params.MoreLikeThisParams.TermStyle;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.FacetComponent;
import org.apache.solr.request.SimpleFacets;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
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

  //////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getDescription() {
    return "Solr MoreLikeThis";
  }
}
