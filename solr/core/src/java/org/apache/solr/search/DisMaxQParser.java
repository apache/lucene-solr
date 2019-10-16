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
package org.apache.solr.search;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.parser.QueryParser;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.common.params.DisMaxParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.util.SolrPluginUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Query parser for dismax queries
 * <p>
 * <b>Note: This API is experimental and may change in non backward-compatible ways in the future</b>
 *
 *
 */
public class DisMaxQParser extends QParser {

  /**
   * A field we can't ever find in any schema, so we can safely tell DisjunctionMaxQueryParser to use it as our
   * defaultField, and map aliases from it to any field in our schema.
   */
  private static String IMPOSSIBLE_FIELD_NAME = "\uFFFC\uFFFC\uFFFC";

  /**
   * Applies the appropriate default rules for the "mm" param based on the 
   * effective value of the "q.op" param
   *
   * @see QueryParsing#OP
   * @see DisMaxParams#MM
   */
  public static String parseMinShouldMatch(final IndexSchema schema, 
                                           final SolrParams params) {
    org.apache.solr.parser.QueryParser.Operator op = QueryParsing.parseOP(params.get(QueryParsing.OP));
    
    return params.get(DisMaxParams.MM, 
                      op.equals(QueryParser.Operator.AND) ? "100%" : "0%");
  }

  /**
   * Uses {@link SolrPluginUtils#parseFieldBoosts(String)} with the 'qf' parameter. Falls back to the 'df' parameter
   */
  public static Map<String, Float> parseQueryFields(final IndexSchema indexSchema, final SolrParams solrParams)
      throws SyntaxError {
    Map<String, Float> queryFields = SolrPluginUtils.parseFieldBoosts(solrParams.getParams(DisMaxParams.QF));
    if (queryFields.isEmpty()) {
      String df = solrParams.get(CommonParams.DF);
      if (df == null) {
        throw new SyntaxError("Neither "+DisMaxParams.QF+" nor "+CommonParams.DF +" are present.");
      }
      queryFields.put(df, 1.0f);
    }
    return queryFields;
  }

  public DisMaxQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    super(qstr, localParams, params, req);
  }

  protected Map<String, Float> queryFields;
  protected Query parsedUserQuery;


  protected String[] boostParams;
  protected List<Query> boostQueries;
  protected Query altUserQuery;

  private boolean parsed = false;


  @Override
  public Query parse() throws SyntaxError {
    
    parsed = true;
    SolrParams solrParams = SolrParams.wrapDefaults(localParams, params);

    queryFields = parseQueryFields(req.getSchema(), solrParams);
    
    /* the main query we will execute.  we disable the coord because
     * this query is an artificial construct
     */
    BooleanQuery.Builder query = new BooleanQuery.Builder();

    boolean notBlank = addMainQuery(query, solrParams);
    if (!notBlank)
      return null;
    addBoostQuery(query, solrParams);
    addBoostFunctions(query, solrParams);

    return QueryUtils.build(query, this);
  }

  protected void addBoostFunctions(BooleanQuery.Builder query, SolrParams solrParams) throws SyntaxError {
    String[] boostFuncs = solrParams.getParams(DisMaxParams.BF);
    if (null != boostFuncs && 0 != boostFuncs.length) {
      for (String boostFunc : boostFuncs) {
        if (null == boostFunc || "".equals(boostFunc)) continue;
        Map<String, Float> ff = SolrPluginUtils.parseFieldBoosts(boostFunc);
        for (Map.Entry<String, Float> entry : ff.entrySet()) {
          Query fq = subQuery(entry.getKey(), FunctionQParserPlugin.NAME).getQuery();
          Float b = entry.getValue();
          if (null != b) {
            fq = new BoostQuery(fq, b);
          }
          query.add(fq, BooleanClause.Occur.SHOULD);
        }
      }
    }
  }

  protected void addBoostQuery(BooleanQuery.Builder query, SolrParams solrParams) throws SyntaxError {
    boostParams = solrParams.getParams(DisMaxParams.BQ);
    //List<Query> boostQueries = SolrPluginUtils.parseQueryStrings(req, boostParams);
    boostQueries = null;
    if (boostParams != null && boostParams.length > 0) {
      boostQueries = new ArrayList<>();
      for (String qs : boostParams) {
        if (qs.trim().length() == 0) continue;
        Query q = subQuery(qs, null).getQuery();
        boostQueries.add(q);
      }
    }
    if (null != boostQueries) {
      if (1 == boostQueries.size() && 1 == boostParams.length) {
        /* legacy logic */
        Query f = boostQueries.get(0);
        while (f instanceof BoostQuery) {
          BoostQuery bq = (BoostQuery) f;
          if (bq .getBoost() == 1f) {
            f = bq.getQuery();
          } else {
            break;
          }
        }
        if (f instanceof BooleanQuery) {
          /* if the default boost was used, and we've got a BooleanQuery
           * extract the subqueries out and use them directly
           */
          for (Object c : ((BooleanQuery) f).clauses()) {
            query.add((BooleanClause) c);
          }
        } else {
          query.add(f, BooleanClause.Occur.SHOULD);
        }
      } else {
        for (Query f : boostQueries) {
          query.add(f, BooleanClause.Occur.SHOULD);
        }
      }
    }
  }

  /** Adds the main query to the query argument. If it's blank then false is returned. */
  protected boolean addMainQuery(BooleanQuery.Builder query, SolrParams solrParams) throws SyntaxError {
    Map<String, Float> phraseFields = SolrPluginUtils.parseFieldBoosts(solrParams.getParams(DisMaxParams.PF));
    float tiebreaker = solrParams.getFloat(DisMaxParams.TIE, 0.0f);

    /* a parser for dealing with user input, which will convert
     * things to DisjunctionMaxQueries
     */
    SolrPluginUtils.DisjunctionMaxQueryParser up = getParser(queryFields, DisMaxParams.QS, solrParams, tiebreaker);

    /* for parsing sloppy phrases using DisjunctionMaxQueries */
    SolrPluginUtils.DisjunctionMaxQueryParser pp = getParser(phraseFields, DisMaxParams.PS, solrParams, tiebreaker);

    /* * * Main User Query * * */
    parsedUserQuery = null;
    String userQuery = getString();
    altUserQuery = null;
    if (userQuery == null || userQuery.trim().length() < 1) {
      // If no query is specified, we may have an alternate
      altUserQuery = getAlternateUserQuery(solrParams);
      if (altUserQuery == null)
        return false;
      query.add(altUserQuery, BooleanClause.Occur.MUST);
    } else {
      // There is a valid query string
      userQuery = SolrPluginUtils.partialEscape(SolrPluginUtils.stripUnbalancedQuotes(userQuery)).toString();
      userQuery = SolrPluginUtils.stripIllegalOperators(userQuery).toString();

      parsedUserQuery = getUserQuery(userQuery, up, solrParams);
      query.add(parsedUserQuery, BooleanClause.Occur.MUST);

      Query phrase = getPhraseQuery(userQuery, pp);
      if (null != phrase) {
        query.add(phrase, BooleanClause.Occur.SHOULD);
      }
    }
    return true;
  }

  protected Query getAlternateUserQuery(SolrParams solrParams) throws SyntaxError {
    String altQ = solrParams.get(DisMaxParams.ALTQ);
    if (altQ != null) {
      QParser altQParser = subQuery(altQ, null);
      return altQParser.getQuery();
    } else {
      return null;
    }
  }

  protected Query getPhraseQuery(String userQuery, SolrPluginUtils.DisjunctionMaxQueryParser pp) throws SyntaxError {
    /* * * Add on Phrases for the Query * * */

    /* build up phrase boosting queries */

    /* if the userQuery already has some quotes, strip them out.
     * we've already done the phrases they asked for in the main
     * part of the query, this is to boost docs that may not have
     * matched those phrases but do match looser phrases.
    */
    String userPhraseQuery = userQuery.replace("\"", "");
    return pp.parse("\"" + userPhraseQuery + "\"");
  }

  protected Query getUserQuery(String userQuery, SolrPluginUtils.DisjunctionMaxQueryParser up, SolrParams solrParams)
          throws SyntaxError {


    String minShouldMatch = parseMinShouldMatch(req.getSchema(), solrParams);
    Query dis = up.parse(userQuery);
    Query query = dis;

    if (dis instanceof BooleanQuery) {
      BooleanQuery.Builder t = new BooleanQuery.Builder();
      SolrPluginUtils.flattenBooleanQuery(t, (BooleanQuery) dis);
      boolean mmAutoRelax = params.getBool(DisMaxParams.MM_AUTORELAX, false);
      SolrPluginUtils.setMinShouldMatch(t, minShouldMatch, mmAutoRelax);
      query = QueryUtils.build(t, this);
    }
    return query;
  }

  protected SolrPluginUtils.DisjunctionMaxQueryParser getParser(Map<String, Float> fields, String paramName,
                                                                SolrParams solrParams, float tiebreaker) {
    int slop = solrParams.getInt(paramName, 0);
    SolrPluginUtils.DisjunctionMaxQueryParser parser = new SolrPluginUtils.DisjunctionMaxQueryParser(this,
            IMPOSSIBLE_FIELD_NAME);
    parser.addAlias(IMPOSSIBLE_FIELD_NAME, tiebreaker, fields);
    parser.setPhraseSlop(slop);
    parser.setSplitOnWhitespace(true);
    return parser;
  }

  @Override
  public String[] getDefaultHighlightFields() {
    return queryFields.keySet().toArray(new String[queryFields.keySet().size()]);
  }

  @Override
  public Query getHighlightQuery() throws SyntaxError {
    if (!parsed)
      parse();
    return parsedUserQuery == null ? altUserQuery : parsedUserQuery;
  }

  @Override
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
