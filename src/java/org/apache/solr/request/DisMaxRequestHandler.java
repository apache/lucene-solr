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
import org.apache.solr.search.SolrQueryParser;
import org.apache.solr.search.QueryParsing;

import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryResponse;
import org.apache.solr.request.SolrRequestHandler;

import org.apache.solr.schema.IndexSchema;

import org.apache.solr.util.NamedList;
import org.apache.solr.util.SolrPluginUtils;
import org.apache.solr.util.DisMaxParams;

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
    
  /** shorten the class referneces for utilities */
  private static class U extends SolrPluginUtils {
    /* :NOOP */
  }

  protected final DisMaxParams params = new DisMaxParams();
    
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

    params.setValues(args);
        
  }

  public void handleRequest(SolrQueryRequest req, SolrQueryResponse rsp) {
    numRequests++;
        
    try {

      int flags = 0;
      SolrIndexSearcher s = req.getSearcher();
      IndexSchema schema = req.getSchema();
            
      Map<String,Float> queryFields =
        U.parseFieldBoosts(U.getParam(req, params.QF, params.qf));
      Map<String,Float> phraseFields =
        U.parseFieldBoosts(U.getParam(req, params.PF, params.pf));

      float tiebreaker = U.getNumberParam
        (req, params.TIE, params.tiebreaker).floatValue();
            
      int pslop = U.getNumberParam(req, params.PS, params.pslop).intValue();

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
        (U.stripUnbalancedQuotes(req.getQueryString())).toString();
            
      /* the main query we will execute.  we disable the coord because
       * this query is an artificial construct
       */
      BooleanQuery query = new BooleanQuery(true);

      String minShouldMatch = U.getParam(req, params.MM, params.mm);
            
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

      String boostQuery = U.getParam(req, params.BQ, params.bq);
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

      String boostFunc = U.getParam(req, params.BF, params.bf);
      if (null != boostFunc && !boostFunc.equals("")) {
        List<Query> funcs = U.parseFuncs(schema, boostFunc);
        for (Query f : funcs) {
          query.add(f, Occur.SHOULD);
        }
      }
            
      /* * * Restrict Results * * */

      List<Query> restrictions = new ArrayList<Query>(1);
            
      /* User Restriction */
      String filterQueryString = U.getParam(req, params.FQ, params.fq);
      Query filterQuery = null;
      if (null != filterQueryString && !filterQueryString.equals("")) {
        filterQuery = p.parse(filterQueryString);
        restrictions.add(filterQuery);
      }
            
      /* * * Generate Main Results * * */

      flags |= U.setReturnFields(U.getParam(req, SolrParams.FL, params.fl), rsp);
      DocList results = s.getDocList(query, restrictions,
                                     SolrPluginUtils.getSort(req),
                                     req.getStart(), req.getLimit(),
                                     flags);
      rsp.add("search-results",results);


            
      /* * * Debugging Info * * */

      try {
        NamedList debug = U.doStandardDebug(req, userQuery, query, results, params);
        if (null != debug) {
          debug.add("boostquery", boostQuery);
          debug.add("boostfunc", boostFunc);

          debug.add("filterquery", filterQueryString);
          if (null != filterQuery) {
            debug.add("parsedfilterquery",
                      QueryParsing.toString(filterQuery, schema));
          }
                    
          rsp.add("debug", debug);
        }

      } catch (Exception e) {
        SolrException.logOnce(SolrCore.log,
                              "Exception durring debug", e);
        rsp.add("exception_during_debug", SolrException.toStr(e));
      }

      /* * * Highlighting/Summarizing  * * */
      if(U.getBooleanParam(req, SolrParams.HIGHLIGHT, params.highlight)) {

        BooleanQuery highlightQuery = new BooleanQuery();
        U.flattenBooleanQuery(highlightQuery, query);
        NamedList sumData = U.doStandardHighlighting(results, highlightQuery, 
                                                     req, params, 
                                                     queryFields.keySet().toArray(new String[0]));
        if(sumData != null)
          rsp.add("highlighting", sumData);
      }
            
    } catch (Exception e) {
      SolrException.log(SolrCore.log,e);
      rsp.setException(e);
      numErrors++;
    }
  }

}
