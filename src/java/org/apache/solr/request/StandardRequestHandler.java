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

package org.apache.solr.request;

import org.apache.lucene.search.*;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.util.MoreLikeThisParams;
import org.apache.solr.util.StrUtils;
import org.apache.solr.util.NamedList;
import org.apache.solr.util.HighlightingUtils;
import org.apache.solr.util.SolrPluginUtils;
import org.apache.solr.search.*;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrException;
import org.apache.solr.handler.MoreLikeThisHandler;
import org.apache.solr.handler.RequestHandlerBase;

import static org.apache.solr.request.SolrParams.*;

/**
 * @author yonik
 * @version $Id$
 *
 * All of the following options may be configured for this handler
 * in the solrconfig as defaults, and may be overriden as request parameters.
 * (TODO: complete documentation of request parameters here, rather than only
 * on the wiki).
 * </p>
 *
 * <ul>
 * <li> highlight - Set to any value not .equal() to "false" to enable highlight
 * generation</li>
 * <li> highlightFields - Set to a comma- or space-delimited list of fields to
 * highlight.  If unspecified, uses the default query field</li>
 * <li> maxSnippets - maximum number of snippets to generate per field-highlight.
 * </li>
 * </ul>
 *
 */
public class StandardRequestHandler extends RequestHandlerBase {

  /** shorten the class references for utilities */
  private static class U extends SolrPluginUtils {
    /* :NOOP */
  }


  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception
  {
    
      SolrParams p = req.getParams();
      String qstr = p.required().get(Q);

      String defaultField = p.get(DF);

      // find fieldnames to return (fieldlist)
      String fl = p.get(SolrParams.FL);
      int flags = 0; 
      if (fl != null) {
        flags |= U.setReturnFields(fl, rsp);
      }
      
      String sortStr = p.get(SORT);
      if( sortStr == null ) {  
        // TODO? should we disable the ';' syntax with config?
        // legacy mode, where sreq is query;sort
        List<String> commands = StrUtils.splitSmart(qstr,';');
        if( commands.size() == 2 ) {
          // TODO? add a deprication warning to the response header
          qstr = commands.get( 0 );
          sortStr = commands.get( 1 );
        }
        else if( commands.size() == 1 ) {
          // This is need to support the case where someone sends: "q=query;"
          qstr = commands.get( 0 );
        }
        else if( commands.size() > 2 ) {
          throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "If you want to use multiple ';' in the query, use the 'sort' param." );
        }
      }

      Sort sort = null;
      if( sortStr != null ) {
        QueryParsing.SortSpec sortSpec = QueryParsing.parseSort(sortStr, req.getSchema());
        if (sortSpec != null) {
          sort = sortSpec.getSort();
        }
      }

      // parse the query from the 'q' parameter (sort has been striped)
      Query query = QueryParsing.parseQuery(qstr, defaultField, p, req.getSchema());
      
      DocListAndSet results = new DocListAndSet();
      NamedList facetInfo = null;
      List<Query> filters = U.parseFilterQueries(req);
      SolrIndexSearcher s = req.getSearcher();

      if (p.getBool(FACET,false)) {
        results = s.getDocListAndSet(query, filters, sort,
                                     p.getInt(START,0), p.getInt(ROWS,10),
                                     flags);
        facetInfo = getFacetInfo(req, rsp, results.docSet);
      } else {
        results.docList = s.getDocList(query, filters, sort,
                                       p.getInt(START,0), p.getInt(ROWS,10),
                                       flags);
      }

      // pre-fetch returned documents
      U.optimizePreFetchDocs(results.docList, query, req, rsp);
      
      rsp.add("response",results.docList);

      if (null != facetInfo) rsp.add("facet_counts", facetInfo);

      // Include "More Like This" results for *each* result
      if( p.getBool( MoreLikeThisParams.MLT, false ) ) {
        MoreLikeThisHandler.MoreLikeThisHelper mlt 
          = new MoreLikeThisHandler.MoreLikeThisHelper( p, s );
        int mltcount = p.getInt( MoreLikeThisParams.DOC_COUNT, 5 );
        rsp.add( "moreLikeThis", mlt.getMoreLikeThese(results.docList, mltcount, flags));
      }
      
      try {
        NamedList dbg = U.doStandardDebug(req, qstr, query, results.docList);
        if (null != dbg) {
          if (null != filters) {
            dbg.add("filter_queries",req.getParams().getParams(FQ));
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
      
      NamedList sumData = HighlightingUtils.doHighlighting(
        results.docList, query.rewrite(req.getSearcher().getReader()), req, new String[]{defaultField});
      if(sumData != null)
        rsp.add("highlighting", sumData);
  }

  /**
   * Fetches information about Facets for this request.
   *
   * Subclasses may with to override this method to provide more 
   * advanced faceting behavior.
   * @see SimpleFacets#getFacetCounts
   */
  protected NamedList getFacetInfo(SolrQueryRequest req, 
                                   SolrQueryResponse rsp, 
                                   DocSet mainSet) {

    SimpleFacets f = new SimpleFacets(req.getSearcher(), 
                                      mainSet, 
                                      req.getParams());
    return f.getFacetCounts();
  }



  //////////////////////// SolrInfoMBeans methods //////////////////////

  public String getVersion() {
    return "$Revision$";
  }

  public String getDescription() {
    return "The standard Solr request handler";
  }

  public String getSourceId() {
    return "$Id$";
  }

  public String getSource() {
    return "$URL$";
  }

  public URL[] getDocs() {
    try {
      return new URL[] { new URL("http://wiki.apache.org/solr/StandardRequestHandler") };
    }
    catch( MalformedURLException ex ) { return null; }
  }
}







