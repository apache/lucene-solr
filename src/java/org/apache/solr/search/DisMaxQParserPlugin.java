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
package org.apache.solr.search;

import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.DefaultSolrParams;
import org.apache.solr.common.params.DisMaxParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.util.SolrPluginUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
/**
 * Create a dismax query from the input value.
 * <br>Other parameters: all main query related parameters from the {@link org.apache.solr.handler.DisMaxRequestHandler} are supported.
 * localParams are checked before global request params.
 * <br>Example: <code>&lt;!dismax qf=myfield,mytitle^2&gt;foo</code> creates a dismax query across
 * across myfield and mytitle, with a higher weight on mytitle.
 */
public class DisMaxQParserPlugin extends QParserPlugin {
  public static String NAME = "dismax";

  public void init(NamedList args) {
  }

  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new DismaxQParser(qstr, localParams, params, req);
  }
}


class DismaxQParser extends QParser {

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
  private static interface DMP extends DisMaxParams {
    /* :NOOP */
  }


  public DismaxQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    super(qstr, localParams, params, req);
  }

  Map<String,Float> queryFields;
  Query parsedUserQuery;


  private String[] boostParams;
  private List<Query> boostQueries;
  private Query altUserQuery;
  private QParser altQParser;
  

  public Query parse() throws ParseException {
    SolrParams solrParams = localParams == null ? params : new DefaultSolrParams(localParams, params);

    IndexSchema schema = req.getSchema();

    queryFields = U.parseFieldBoosts(solrParams.getParams(DMP.QF));
    Map<String,Float> phraseFields = U.parseFieldBoosts(solrParams.getParams(DMP.PF));

    float tiebreaker = solrParams.getFloat(DMP.TIE, 0.0f);

    int pslop = solrParams.getInt(DMP.PS, 0);
    int qslop = solrParams.getInt(DMP.QS, 0);

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

    /* for parsing sloppy phrases using DisjunctionMaxQueries */
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
    parsedUserQuery = null;
    String userQuery = getString();
    altUserQuery = null;
    if( userQuery == null || userQuery.trim().length() < 1 ) {
      // If no query is specified, we may have an alternate
      String altQ = solrParams.get( DMP.ALTQ );
      if (altQ != null) {
        altQParser = subQuery(altQ, null);
        altUserQuery = altQParser.parse();
        query.add( altUserQuery , BooleanClause.Occur.MUST );
      } else {
        throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "missing query string" );
      }
    }
    else {
      // There is a valid query string
      userQuery = U.partialEscape(U.stripUnbalancedQuotes(userQuery)).toString();

      String minShouldMatch = solrParams.get(DMP.MM, "100%");
      Query dis = up.parse(userQuery);
      parsedUserQuery = dis;

      if (dis instanceof BooleanQuery) {
        BooleanQuery t = new BooleanQuery();
        U.flattenBooleanQuery(t, (BooleanQuery)dis);
        U.setMinShouldMatch(t, minShouldMatch);
        parsedUserQuery = t;
      }
      query.add(parsedUserQuery, BooleanClause.Occur.MUST);


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
        query.add(phrase, BooleanClause.Occur.SHOULD);
      }
    }


    /* * * Boosting Query * * */
    boostParams = solrParams.getParams(DMP.BQ);
    //List<Query> boostQueries = U.parseQueryStrings(req, boostParams);
    boostQueries=null;
    if (boostParams!=null && boostParams.length>0) {
      boostQueries = new ArrayList<Query>();
      for (String qs : boostParams) {
        if (qs.trim().length()==0) continue;
        Query q = subQuery(qs, null).parse();
        boostQueries.add(q);
      }
    }
    if (null != boostQueries) {
      if(1 == boostQueries.size() && 1 == boostParams.length) {
        /* legacy logic */
        Query f = boostQueries.get(0);
        if (1.0f == f.getBoost() && f instanceof BooleanQuery) {
          /* if the default boost was used, and we've got a BooleanQuery
           * extract the subqueries out and use them directly
           */
          for (Object c : ((BooleanQuery)f).clauses()) {
            query.add((BooleanClause)c);
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

    String[] boostFuncs = solrParams.getParams(DMP.BF);
    if (null != boostFuncs && 0 != boostFuncs.length) {
      for (String boostFunc : boostFuncs) {
        if(null == boostFunc || "".equals(boostFunc)) continue;
        Map<String,Float> ff = SolrPluginUtils.parseFieldBoosts(boostFunc);
        for (String f : ff.keySet()) {
          Query fq = subQuery(f, FunctionQParserPlugin.NAME).parse();
          Float b = ff.get(f);
          if (null != b) {
            fq.setBoost(b);
          }
          query.add(fq, BooleanClause.Occur.SHOULD);
        }
      }
    }

    return query;
  }

  @Override
  public String[] getDefaultHighlightFields() {
    String[] highFields = queryFields.keySet().toArray(new String[0]);
    return highFields;
  }

  @Override
  public Query getHighlightQuery() throws ParseException {
    return parsedUserQuery;
  }

  public void addDebugInfo(NamedList<Object> debugInfo) {
    super.addDebugInfo(debugInfo);
    debugInfo.add("altquerystring", altUserQuery);
    if (null != boostQueries) {
      debugInfo.add("boost_queries", boostParams);
      debugInfo.add("parsed_boost_queries",
                QueryParsing.toString(boostQueries, req.getSchema()));
    }
    debugInfo.add("boostfuncs", req.getParams().getParams(DisMaxParams.BF));   
  }
}
