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

import java.util.ArrayList;
import java.util.List;
import java.net.URL;

import org.apache.solr.util.StrUtils;
import org.apache.solr.util.NamedList;
import org.apache.solr.util.HighlightingUtils;
import org.apache.solr.util.SolrPluginUtils;
import org.apache.solr.search.*;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoMBean;
import org.apache.solr.core.SolrException;
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
public class StandardRequestHandler implements SolrRequestHandler, SolrInfoMBean {

  // statistics
  // TODO: should we bother synchronizing these, or is an off-by-one error
  // acceptable every million requests or so?
  long numRequests;
  long numErrors;
  SolrParams defaults;
  SolrParams appends;
  SolrParams invariants;

  /** shorten the class references for utilities */
  private static class U extends SolrPluginUtils {
    /* :NOOP */
  }

  public void init(NamedList args) {
    Object o = args.get("defaults");
    if (o != null && o instanceof NamedList) {
      defaults = SolrParams.toSolrParams((NamedList)o);
    }
    o = args.get("appends");
    if (o != null && o instanceof NamedList) {
      appends = SolrParams.toSolrParams((NamedList)o);
    }
    o = args.get("invariants");
    if (o != null && o instanceof NamedList) {
      invariants = SolrParams.toSolrParams((NamedList)o);
    }
    
  }

  public void handleRequest(SolrQueryRequest req, SolrQueryResponse rsp) {
    numRequests++;

    try {
      U.setDefaults(req,defaults,appends,invariants);
      SolrParams p = req.getParams();
      String sreq = p.get(Q);

      String defaultField = p.get(DF);

      // find fieldnames to return (fieldlist)
      String fl = p.get(SolrParams.FL);
      int flags = 0; 
      if (fl != null) {
        flags |= U.setReturnFields(fl, rsp);
      }

      if (sreq==null) throw new SolrException(400,"Missing queryString");
      List<String> commands = StrUtils.splitSmart(sreq,';');

      String qs = commands.size() >= 1 ? commands.get(0) : "";
      Query query = QueryParsing.parseQuery(qs, defaultField, p, req.getSchema());

      // If the first non-query, non-filter command is a simple sort on an indexed field, then
      // we can use the Lucene sort ability.
      Sort sort = null;
      if (commands.size() >= 2) {
        QueryParsing.SortSpec sortSpec = QueryParsing.parseSort(commands.get(1), req.getSchema());
        if (sortSpec != null) {
          sort = sortSpec.getSort();
          // ignore the count for now... it's currently only controlled by start & limit on req
          // count = sortSpec.getCount();
        }
      }

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
      
      rsp.add(null,results.docList);

      if (null != facetInfo) rsp.add("facet_counts", facetInfo);

      try {
        NamedList dbg = U.doStandardDebug(req, qs, query, results.docList);
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
        SolrException.logOnce(SolrCore.log, "Exception durring debug", e);
        rsp.add("exception_during_debug", SolrException.toStr(e));
      }

      NamedList sumData = HighlightingUtils.doHighlighting(
        results.docList, query, req, new String[]{defaultField});
      if(sumData != null)
        rsp.add("highlighting", sumData);

    } catch (SolrException e) {
      rsp.setException(e);
      numErrors++;
      return;
    } catch (Exception e) {
      SolrException.log(SolrCore.log,e);
      rsp.setException(e);
      numErrors++;
      return;
    }
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


  public String getName() {
    return StandardRequestHandler.class.getName();
  }

  public String getVersion() {
    return SolrCore.version;
  }

  public String getDescription() {
    return "The standard Solr request handler";
  }

  public Category getCategory() {
    return Category.QUERYHANDLER;
  }

  public String getSourceId() {
    return "$Id$";
  }

  public String getSource() {
    return "$URL$";
  }

  public URL[] getDocs() {
    return null;
  }

  public NamedList getStatistics() {
    NamedList lst = new NamedList();
    lst.add("requests", numRequests);
    lst.add("errors", numErrors);
    return lst;
  }
}

