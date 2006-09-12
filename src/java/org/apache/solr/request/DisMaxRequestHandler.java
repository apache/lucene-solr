/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoMBean;
import org.apache.solr.core.SolrException;

import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.DocList;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.DocListAndSet;
import org.apache.solr.search.SolrQueryParser;
import org.apache.solr.search.QueryParsing;

import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryResponse;
import org.apache.solr.request.SolrRequestHandler;

import org.apache.solr.schema.IndexSchema;

import org.apache.solr.util.NamedList;
import org.apache.solr.util.HighlightingUtils;
import org.apache.solr.util.SolrPluginUtils;
import org.apache.solr.util.DisMaxParams;
import static org.apache.solr.request.SolrParams.*;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.queryParser.QueryParser;

/* this is the standard logging framework for Solr */

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.net.URL;
    
/**
 * <p>
 * A Generic query plugin designed to be given a simple query expression
 * from a user, which it will then query agaisnt a variety of
 * pre-configured fields, in a variety of ways, using BooleanQueries,
 * DisjunctionMaxQueries, and PhraseQueries.
 * </p>
 *
 * <p>
 * All of the following options may be configured for this plugin
 * in the solrconfig as defaults, and may be overriden as request parameters
 * </p>
 *
 * <ul>
 * <li>tie - (Tie breaker) float value to use as tiebreaker in
 *           DisjunctionMaxQueries (should be something much less then 1)
 * </li>
 * <li> qf - (Query Fields) fields and boosts to use when building
 *           DisjunctionMaxQueries from the users query.  Format is:
 *           "<code>fieldA^1.0 fieldB^2.2</code>".
 * </li>
 * <li> mm - (Minimum Match) this supports a wide variety of
 *           complex expressions.
 *           read {@link SolrPluginUtils#setMinShouldMatch SolrPluginUtils.setMinShouldMatch} for full details.
 * </li>
 * <li> pf - (Phrase Fields) fields/boosts to make phrase queries out
 *           of to boost
 *           the users query for exact matches on the specified fields.
 *           Format is: "<code>fieldA^1.0 fieldB^2.2</code>".
 * </li>
 * <li> ps - (Phrase Slop) amount of slop on phrase queries built for pf
 *           fields.
 * </li>
 * <li> bq - (Boost Query) a raw lucene query that will be included in the 
 *           users query to influcene the score.  If this is a BooleanQuery
 *           with a default boost (1.0f) then the individual clauses will be
 *           added directly to the main query.  Otherwise the query will be
 *           included as is.
 * </li>
 * <li> bf - (Boost Functions) functions (with optional boosts) that will be
 *           included in the users query to influcene the score.
 *           Format is: "<code>funcA(arg1,arg2)^1.2
 *           funcB(arg3,arg4)^2.2</code>".  NOTE: Whitespace is not allowed
 *           in the function arguments.
 * </li>
 * <li> fq - (Filter Query) a raw lucene query that can be used
 *           to restrict the super set of products we are interested in - more
 *           efficient then using bq, but doesn't influence score.
 *           This param can be specified multiple times, and the filters
 *           are addative.
 * </li>
 * </ul>
 *
 * <p>
 * The following options are only available as request params...
 * </p>
 *
 * <ul>
 * <li>   q - (Query) the raw unparsed, unescaped, query from the user.
 * </li>
 * <li>sort - (Order By) list of fields and direction to sort on.
 * </li>
 * </ul>
 *
 * <pre>
 * :TODO: document facet param support
 *
 * :TODO: make bf,pf,qf multival params now that SolrParams supports them
 * </pre>
 */
public class DisMaxRequestHandler
  implements SolrRequestHandler, SolrInfoMBean  {


  /**
   * A field we can't ever find in any schema, so we can safely tell
   * DisjunctionMaxQueryParser to use it as our defaultField, and
   * map aliases from it to any field in our schema.
   */
  private static String IMPOSSIBLE_FIELD_NAME = "\uFFFC\uFFFC\uFFFC";
    
  // statistics
  // TODO: should we bother synchronizing these, or is an off-by-one error
  // acceptable every million requests or so?
  long numRequests;
  long numErrors;
  
  SolrParams defaults;
  SolrParams appends;
  SolrParams invariants;
    
  /** shorten the class referneces for utilities */
  private static class U extends SolrPluginUtils {
    /* :NOOP */
  }
  /** shorten the class referneces for utilities */
  private static class DMP extends DisMaxParams {
    /* :NOOP */
  }

  public DisMaxRequestHandler() {
    super();
  }
    
  /* returns URLs to the Wiki pages */
  public URL[] getDocs() {
    /* :TODO: need docs */
    return new URL[0];
  }
  public String getName() {
    return this.getClass().getName();
  }

  public NamedList getStatistics() {
    NamedList lst = new NamedList();
    lst.add("requests", numRequests);
    lst.add("errors", numErrors);
    return lst;
  }

  public String getVersion() {
    return "$Revision:$";
  }
    
  public String getDescription() {
    return "DisjunctionMax Request Handler: Does relevancy based queries "
      + "accross a variety of fields using configured boosts";
  }
    
  public Category getCategory() {
    return Category.QUERYHANDLER;
  }
    
  public String getSourceId() {
    return "$Id:$";
  }
    
  public String getSource() {
    return "$URL:$";
  }

  /** sets the default variables for any usefull info it finds in the config
   * if a config option is not inthe format expected, logs an warning
   * and ignores it..
   */
  public void init(NamedList args) {

    if (-1 == args.indexOf("defaults",0)) {
      // no explict defaults list, use all args implicitly
      // indexOf so "<null name="defaults"/> is valid indicator of no defaults
      defaults = SolrParams.toSolrParams(args);
    } else {
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
  }

  public void handleRequest(SolrQueryRequest req, SolrQueryResponse rsp) {
    numRequests++;
        
    try {
      U.setDefaults(req,defaults,appends,invariants);
      SolrParams params = req.getParams();
      
      int flags = 0;
      
      SolrIndexSearcher s = req.getSearcher();
      IndexSchema schema = req.getSchema();
            
      Map<String,Float> queryFields = U.parseFieldBoosts(params.get(DMP.QF));
      Map<String,Float> phraseFields = U.parseFieldBoosts(params.get(DMP.PF));

      float tiebreaker = params.getFloat(DMP.TIE, 0.0f);
            
      int pslop = params.getInt(DMP.PS, 0);

      /* a generic parser for parsing regular lucene queries */
      QueryParser p = new SolrQueryParser(schema, null);

      /* a parser for dealing with user input, which will convert
       * things to DisjunctionMaxQueries
       */
      U.DisjunctionMaxQueryParser up =
        new U.DisjunctionMaxQueryParser(schema, IMPOSSIBLE_FIELD_NAME);
      up.addAlias(IMPOSSIBLE_FIELD_NAME,
                  tiebreaker, queryFields);

      /* for parsing slopy phrases using DisjunctionMaxQueries */
      U.DisjunctionMaxQueryParser pp =
        new U.DisjunctionMaxQueryParser(schema, IMPOSSIBLE_FIELD_NAME);
      pp.addAlias(IMPOSSIBLE_FIELD_NAME,
                  tiebreaker, phraseFields);
      pp.setPhraseSlop(pslop);
            
            
      /* * * Main User Query * * */

      String userQuery = U.partialEscape
        (U.stripUnbalancedQuotes(params.get(Q))).toString();
            
      /* the main query we will execute.  we disable the coord because
       * this query is an artificial construct
       */
      BooleanQuery query = new BooleanQuery(true);

      String minShouldMatch = params.get(DMP.MM, "100%");
            
      Query dis = up.parse(userQuery);

      if (dis instanceof BooleanQuery) {
        BooleanQuery t = new BooleanQuery();
        U.flattenBooleanQuery(t, (BooleanQuery)dis);

        U.setMinShouldMatch(t, minShouldMatch);
                
        query.add(t, Occur.MUST);
      } else {
        query.add(dis, Occur.MUST);
      }

      /* * * Add on Phrases for the Query * * */
            
      /* build up phrase boosting queries */

      /* if the userQuery already has some quotes, stip them out.
       * we've already done the phrases they asked for in the main
       * part of the query, this is to boost docs that may not have
       * matched those phrases but do match looser phrases.
       */
      String userPhraseQuery = userQuery.replace("\"","");
      Query phrase = pp.parse("\"" + userPhraseQuery + "\"");
      if (null != phrase) {
        query.add(phrase, Occur.SHOULD);
      }
            
      /* * * Boosting Query * * */

      String boostQuery = params.get(DMP.BQ);
      if (null != boostQuery && !boostQuery.equals("")) {
        Query tmp = p.parse(boostQuery);
        /* if the default boost was used, and we've got a BooleanQuery
         * extract the subqueries out and use them directly
         */
        if (1.0f == tmp.getBoost() && tmp instanceof BooleanQuery) {
          for (BooleanClause c : ((BooleanQuery)tmp).getClauses()) {
            query.add(c);
          }
        } else {
          query.add(tmp, BooleanClause.Occur.SHOULD);
        }
      }

      /* * * Boosting Functions * * */

      String boostFunc = params.get(DMP.BF);
      if (null != boostFunc && !boostFunc.equals("")) {
        List<Query> funcs = U.parseFuncs(schema, boostFunc);
        for (Query f : funcs) {
          query.add(f, Occur.SHOULD);
        }
      }
            
      /* * * Restrict Results * * */

      List<Query> restrictions = U.parseFilterQueries(req);
            
      /* * * Generate Main Results * * */

      flags |= U.setReturnFields(req,rsp);
      
      DocListAndSet results = new DocListAndSet();
      NamedList facetInfo = null;
      if (params.getBool(FACET,false)) {
        results = s.getDocListAndSet(query, restrictions,
                                     SolrPluginUtils.getSort(req),
                                     req.getStart(), req.getLimit(),
                                     flags);
        facetInfo = getFacetInfo(req, rsp, results.docSet);
      } else {
        results.docList = s.getDocList(query, restrictions,
                                       SolrPluginUtils.getSort(req),
                                       req.getStart(), req.getLimit(),
                                       flags);
      }
      rsp.add("search-results",results.docList);
      
      if (null != facetInfo) rsp.add("facet_counts", facetInfo);


            
      /* * * Debugging Info * * */

      try {
        NamedList debug = U.doStandardDebug(req, userQuery, query, results.docList);
        if (null != debug) {
          debug.add("boostquery", boostQuery);
          debug.add("boostfunc", boostFunc);
          if (null != restrictions) {
            debug.add("filter_queries", params.getParams(FQ));
            List<String> fqs = new ArrayList<String>(restrictions.size());
            for (Query fq : restrictions) {
              fqs.add(QueryParsing.toString(fq, req.getSchema()));
            }
            debug.add("parsed_filter_queries",fqs);
          }
          rsp.add("debug", debug);
        }

      } catch (Exception e) {
        SolrException.logOnce(SolrCore.log,
                              "Exception durring debug", e);
        rsp.add("exception_during_debug", SolrException.toStr(e));
      }

      /* * * Highlighting/Summarizing  * * */
      if(HighlightingUtils.isHighlightingEnabled(req)) {

        BooleanQuery highlightQuery = new BooleanQuery();
        U.flattenBooleanQuery(highlightQuery, query);
        String[] highFields = queryFields.keySet().toArray(new String[0]);
        NamedList sumData =
          HighlightingUtils.doHighlighting(results.docList, highlightQuery, 
                                           req, highFields);
        if(sumData != null)
          rsp.add("highlighting", sumData);
      }
            
    } catch (Exception e) {
      SolrException.log(SolrCore.log,e);
      rsp.setException(e);
      numErrors++;
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
  
  
}
