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

import static org.apache.solr.request.SolrParams.FACET;
import static org.apache.solr.request.SolrParams.FQ;
import static org.apache.solr.request.SolrParams.Q;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrException;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.DocListAndSet;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SolrQueryParser;
import org.apache.solr.util.DisMaxParams;
import org.apache.solr.util.HighlightingUtils;
import org.apache.solr.util.NamedList;
import org.apache.solr.util.SolrPluginUtils;
    
/**
 * <p>
 * A Generic query plugin designed to be given a simple query expression
 * from a user, which it will then query against a variety of
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
 * <li>q.alt - An alternate query to be used in cases where the main
 *             query (q) is not specified (or blank).  This query should
 *             be expressed in the Standard SolrQueryParser syntax (you
 *             can use <code>q.alt=*:*</code> to denote that all documents
 *             should be returned when no query is specified)
 * </li>
 * <li>tie - (Tie breaker) float value to use as tiebreaker in
 *           DisjunctionMaxQueries (should be something much less than 1)
 * </li>
 * <li> qf - (Query Fields) fields and boosts to use when building
 *           DisjunctionMaxQueries from the users query.  Format is:
 *           "<code>fieldA^1.0 fieldB^2.2</code>".
 *           This param can be specified multiple times, and the fields
 *           are additive.
 * </li>
 * <li> mm - (Minimum Match) this supports a wide variety of
 *           complex expressions.
 *           read {@link SolrPluginUtils#setMinShouldMatch SolrPluginUtils.setMinShouldMatch} and <a href="http://lucene.apache.org/solr/api/org/apache/solr/util/doc-files/min-should-match.html">mm expression format</a> for details.
 * </li>
 * <li> pf - (Phrase Fields) fields/boosts to make phrase queries out
 *           of, to boost the users query for exact matches on the specified fields.
 *           Format is: "<code>fieldA^1.0 fieldB^2.2</code>".
 *           This param can be specified multiple times, and the fields
 *           are additive.
 * </li>
 * <li> ps - (Phrase Slop) amount of slop on phrase queries built for pf
 *           fields.
 * </li>
 * <li> qs - (Query Slop) amount of slop on phrase queries explicitly
 *           specified in the "q" for qf fields.
 * </li>
 * <li> bq - (Boost Query) a raw lucene query that will be included in the 
 *           users query to influence the score.  If this is a BooleanQuery
 *           with a default boost (1.0f), then the individual clauses will be
 *           added directly to the main query.  Otherwise, the query will be
 *           included as is.
 *           This param can be specified multiple times, and the boosts are 
 *           are additive.  NOTE: the behaviour listed above is only in effect
 *           if a single <code>bq</code> paramter is specified.  Hence you can
 *           disable it by specifying an additional, blank, <code>bq</code> 
 *           parameter.
 * </li>
 * <li> bf - (Boost Functions) functions (with optional boosts) that will be
 *           included in the users query to influence the score.
 *           Format is: "<code>funcA(arg1,arg2)^1.2
 *           funcB(arg3,arg4)^2.2</code>".  NOTE: Whitespace is not allowed
 *           in the function arguments.
 *           This param can be specified multiple times, and the functions
 *           are additive.
 * </li>
 * <li> fq - (Filter Query) a raw lucene query that can be used
 *           to restrict the super set of products we are interested in - more
 *           efficient then using bq, but doesn't influence score.
 *           This param can be specified multiple times, and the filters
 *           are additive.
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
 * </pre>
 */
public class DisMaxRequestHandler extends RequestHandlerBase  {

  /**
   * A field we can't ever find in any schema, so we can safely tell
   * DisjunctionMaxQueryParser to use it as our defaultField, and
   * map aliases from it to any field in our schema.
   */
  private static String IMPOSSIBLE_FIELD_NAME = "\uFFFC\uFFFC\uFFFC";
    
    
  /** shorten the class references for utilities */
  private static class U extends SolrPluginUtils {
    /* :NOOP */
  }
  /** shorten the class references for utilities */
  private static class DMP extends DisMaxParams {
    /* :NOOP */
  }

  public DisMaxRequestHandler() {
    super();
  }
  
  /** Sets the default variables for any useful info it finds in the config.
   * If a config option is not in the format expected, logs a warning
   * and ignores it.
   */
  public void init(NamedList args) {
	// Handle an old format
    if (-1 == args.indexOf("defaults",0)) {
      // no explict defaults list, use all args implicitly
      // indexOf so "<null name="defaults"/> is valid indicator of no defaults
      defaults = SolrParams.toSolrParams(args);
    } else {
      // otherwise use the new one.
      super.init( args );
    }
  }

  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception
  {
      SolrParams params = req.getParams();
      
      int flags = 0;
      
      SolrIndexSearcher s = req.getSearcher();
      IndexSchema schema = req.getSchema();
            
      Map<String,Float> queryFields = U.parseFieldBoosts(params.getParams(DMP.QF));
      Map<String,Float> phraseFields = U.parseFieldBoosts(params.getParams(DMP.PF));

      float tiebreaker = params.getFloat(DMP.TIE, 0.0f);
            
      int pslop = params.getInt(DMP.PS, 0);
      int qslop = params.getInt(DMP.QS, 0);

      /* a generic parser for parsing regular lucene queries */
      QueryParser p = schema.getSolrQueryParser(null);

      /* a parser for dealing with user input, which will convert
       * things to DisjunctionMaxQueries
       */
      U.DisjunctionMaxQueryParser up =
        new U.DisjunctionMaxQueryParser(schema, IMPOSSIBLE_FIELD_NAME);
      up.addAlias(IMPOSSIBLE_FIELD_NAME,
                  tiebreaker, queryFields);
      up.setPhraseSlop(qslop);
      
      /* for parsing slopy phrases using DisjunctionMaxQueries */
      U.DisjunctionMaxQueryParser pp =
        new U.DisjunctionMaxQueryParser(schema, IMPOSSIBLE_FIELD_NAME);
      pp.addAlias(IMPOSSIBLE_FIELD_NAME,
                  tiebreaker, phraseFields);
      pp.setPhraseSlop(pslop);
            
            
      /* the main query we will execute.  we disable the coord because
       * this query is an artificial construct
       */
      BooleanQuery query = new BooleanQuery(true);

      /* * * Main User Query * * */
      Query parsedUserQuery = null;
      String userQuery = params.get( Q );
      Query altUserQuery = null;
      if( userQuery == null || userQuery.trim().length() < 1 ) {
        // If no query is specified, we may have an alternate
        String altQ = params.get( DMP.ALTQ );
        if (altQ != null) {
          altUserQuery = p.parse(altQ);
          query.add( altUserQuery , Occur.MUST );
        } else {
          throw new SolrException( 400, "missing query string" );
        }
      }
      else {
        // There is a valid query string
        userQuery = U.partialEscape(U.stripUnbalancedQuotes(userQuery)).toString();
            
        String minShouldMatch = params.get(DMP.MM, "100%");
        Query dis = up.parse(userQuery);
        parsedUserQuery = dis;
  
        if (dis instanceof BooleanQuery) {
          BooleanQuery t = new BooleanQuery();
          U.flattenBooleanQuery(t, (BooleanQuery)dis);
          U.setMinShouldMatch(t, minShouldMatch);                
          parsedUserQuery = t;
        } 
        query.add(parsedUserQuery, Occur.MUST);
        

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
      }

            
      /* * * Boosting Query * * */
      String[] boostParams = params.getParams(DMP.BQ);
      List<Query> boostQueries = U.parseQueryStrings(req, boostParams);
      if (null != boostQueries) {
        if(1 == boostQueries.size() && 1 == boostParams.length) {
          /* legacy logic */
          Query f = boostQueries.get(0);
          if (1.0f == f.getBoost() && f instanceof BooleanQuery) {
            /* if the default boost was used, and we've got a BooleanQuery
             * extract the subqueries out and use them directly
             */
            for (BooleanClause c : ((BooleanQuery)f).getClauses()) {
              query.add(c);
            }
          } else {
            query.add(f, BooleanClause.Occur.SHOULD);
          }
        } else {
          for(Query f : boostQueries) {
            query.add(f, BooleanClause.Occur.SHOULD);
          }
        }
      }

      /* * * Boosting Functions * * */

      String[] boostFuncs = params.getParams(DMP.BF);
      if (null != boostFuncs && 0 != boostFuncs.length) {
        for (String boostFunc : boostFuncs) {
          if(null == boostFunc || "".equals(boostFunc)) continue;
          List<Query> funcs = U.parseFuncs(schema, boostFunc);
          for (Query f : funcs) {
            query.add(f, Occur.SHOULD);          
          }
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
      rsp.add("response",results.docList);
      // pre-fetch returned documents
      U.optimizePreFetchDocs(results.docList, query, req, rsp);

      
      if (null != facetInfo) rsp.add("facet_counts", facetInfo);


            
      /* * * Debugging Info * * */

      try {
        NamedList debug = U.doStandardDebug(req, userQuery, query, results.docList);
        if (null != debug) {
          debug.add("altquerystring", altUserQuery);
          if (null != boostQueries) {
            debug.add("boost_queries", boostParams);
            debug.add("parsed_boost_queries", 
                      QueryParsing.toString(boostQueries, req.getSchema()));
          }
          debug.add("boostfuncs", params.getParams(DMP.BF));
          if (null != restrictions) {
            debug.add("filter_queries", params.getParams(FQ));
            debug.add("parsed_filter_queries", 
                      QueryParsing.toString(restrictions, req.getSchema()));
          }
          rsp.add("debug", debug);
        }

      } catch (Exception e) {
        SolrException.logOnce(SolrCore.log,
                              "Exception during debug", e);
        rsp.add("exception_during_debug", SolrException.toStr(e));
      }

      /* * * Highlighting/Summarizing  * * */
      if(HighlightingUtils.isHighlightingEnabled(req) && parsedUserQuery != null) {
        String[] highFields = queryFields.keySet().toArray(new String[0]);
        NamedList sumData =
          HighlightingUtils.doHighlighting(
	       results.docList, 
	       parsedUserQuery.rewrite(req.getSearcher().getReader()), 
	       req, 
	       highFields);
        if(sumData != null)
          rsp.add("highlighting", sumData);
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

	@Override
	public String getDescription() {
	    return "DisjunctionMax Request Handler: Does relevancy based queries "
	       + "across a variety of fields using configured boosts";
	}

	@Override
	public String getVersion() {
	    return "$Revision$";
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
    return new URL[] { new URL("http://wiki.apache.org/solr/DisMaxRequestHandler") };
    }
    catch( MalformedURLException ex ) { return null; }
  }
}
