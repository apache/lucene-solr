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

import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.join.CrossCollectionJoinQParser;
import org.apache.solr.search.join.ScoreJoinQParserPlugin;
import org.apache.solr.util.RefCounted;

public class JoinQParserPlugin extends QParserPlugin {

  public static final String NAME = "join";
  /** Choose the internal algorithm */
  private static final String METHOD = "method";

  private String routerField;

  private Set<String> allowSolrUrls;

  private static class JoinParams {
    final String fromField;
    final String fromCore;
    final Query fromQuery;
    final long fromCoreOpenTime;
    final String toField;

    public JoinParams(String fromField, String fromCore, Query fromQuery, long fromCoreOpenTime, String toField) {
      this.fromField = fromField;
      this.fromCore = fromCore;
      this.fromQuery = fromQuery;
      this.fromCoreOpenTime = fromCoreOpenTime;
      this.toField = toField;
    }
  }

  private enum Method {
    index {
      @Override
      Query makeFilter(QParser qparser, JoinQParserPlugin plugin) throws SyntaxError {
        final JoinParams jParams = parseJoin(qparser);
        final JoinQuery q = new JoinQuery(jParams.fromField, jParams.toField, jParams.fromCore, jParams.fromQuery);
        q.fromCoreOpenTime = jParams.fromCoreOpenTime;
        return q;
      }

      @Override
      Query makeJoinDirectFromParams(JoinParams jParams) {
        return new JoinQuery(jParams.fromField, jParams.toField, null, jParams.fromQuery);
      }
    },
    dvWithScore {
      @Override
      Query makeFilter(QParser qparser, JoinQParserPlugin plugin) throws SyntaxError {
        return new ScoreJoinQParserPlugin().createParser(qparser.qstr, qparser.localParams, qparser.params, qparser.req).parse();
      }

      @Override
      Query makeJoinDirectFromParams(JoinParams jParams) {
        return ScoreJoinQParserPlugin.createJoinQuery(jParams.fromQuery, jParams.fromField, jParams.toField, ScoreMode.None);
      }
    },
    topLevelDV {
      @Override
      Query makeFilter(QParser qparser, JoinQParserPlugin plugin) throws SyntaxError {
        final JoinParams jParams = parseJoin(qparser);
        final JoinQuery q = createTopLevelJoin(jParams);
        q.fromCoreOpenTime = jParams.fromCoreOpenTime;
        return q;
      }

      @Override
      Query makeJoinDirectFromParams(JoinParams jParams) {
        return new TopLevelJoinQuery(jParams.fromField, jParams.toField, null, jParams.fromQuery);
      }

      private JoinQuery createTopLevelJoin(JoinParams jParams) {
        if (isSelfJoin(jParams)) {
          return new TopLevelJoinQuery.SelfJoin(jParams.fromField, jParams.fromQuery);
        }
        return new TopLevelJoinQuery(jParams.fromField, jParams.toField, jParams.fromCore, jParams.fromQuery);
      }

      private boolean isSelfJoin(JoinParams jparams) {
        return jparams.fromCore == null &&
                (jparams.fromField != null && jparams.fromField.equals(jparams.toField));
      }
    },
    crossCollection {
      @Override
      Query makeFilter(QParser qparser, JoinQParserPlugin plugin) throws SyntaxError {
        return new CrossCollectionJoinQParser(qparser.qstr, qparser.localParams, qparser.params, qparser.req,
                plugin.routerField, plugin.allowSolrUrls).parse();
      }
    };

    abstract Query makeFilter(QParser qparser, JoinQParserPlugin plugin) throws SyntaxError;

    Query makeJoinDirectFromParams(JoinParams jParams) {
      throw new IllegalStateException("Join method [" + name() + "] doesn't support qparser-less creation");
    }

    JoinParams parseJoin(QParser qparser) throws SyntaxError {
      final String fromField = qparser.getParam("from");
      final String fromIndex = qparser.getParam("fromIndex");
      final String toField = qparser.getParam("to");
      final String v = qparser.localParams.get(QueryParsing.V);
      final String coreName;

      Query fromQuery;
      long fromCoreOpenTime = 0;

      if (fromIndex != null && !fromIndex.equals(qparser.req.getCore().getCoreDescriptor().getName()) ) {
        CoreContainer container = qparser.req.getCore().getCoreContainer();

        // if in SolrCloud mode, fromIndex should be the name of a single-sharded collection
        coreName = ScoreJoinQParserPlugin.getCoreName(fromIndex, container);

        final SolrCore fromCore = container.getCore(coreName);
        if (fromCore == null) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              "Cross-core join: no such core " + coreName);
        }

        RefCounted<SolrIndexSearcher> fromHolder = null;
        LocalSolrQueryRequest otherReq = new LocalSolrQueryRequest(fromCore, qparser.params);
        try {
          QParser parser = QParser.getParser(v, otherReq);
          fromQuery = parser.getQuery();
          fromHolder = fromCore.getRegisteredSearcher();
          if (fromHolder != null) fromCoreOpenTime = fromHolder.get().getOpenNanoTime();
        } finally {
          otherReq.close();
          fromCore.close();
          if (fromHolder != null) fromHolder.decref();
        }
      } else {
        coreName = null;
        QParser fromQueryParser = qparser.subQuery(v, null);
        fromQueryParser.setIsFilter(true);
        fromQuery = fromQueryParser.getQuery();
      }

      final String indexToUse = coreName == null ? fromIndex : coreName;
      return new JoinParams(fromField, indexToUse, fromQuery, fromCoreOpenTime, toField);
    }
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public void init(@SuppressWarnings({"rawtypes"})NamedList args) {
    routerField = (String) args.get("routerField");

    if (args.get("allowSolrUrls") != null) {
      allowSolrUrls = new HashSet<>();
      allowSolrUrls.addAll((List<String>) args.get("allowSolrUrls"));
    } else {
      allowSolrUrls = null;
    }
  }

  @Override
  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    final JoinQParserPlugin plugin = this;

    return new QParser(qstr, localParams, params, req) {

      @Override
      public Query parse() throws SyntaxError {
        if (localParams != null && localParams.get(METHOD) != null) {
          // TODO Make sure 'method' is valid value here and give users a nice error
          final Method explicitMethod = Method.valueOf(localParams.get(METHOD));
          return explicitMethod.makeFilter(this, plugin);
        }

        // Legacy join behavior before introduction of SOLR-13892
        if(localParams!=null && localParams.get(ScoreJoinQParserPlugin.SCORE)!=null) {
          return new ScoreJoinQParserPlugin().createParser(qstr, localParams, params, req).parse();
        } else {
          return Method.index.makeFilter(this, plugin);
        }
      }
    };
  }

  private static final EnumSet<Method> JOIN_METHOD_WHITELIST = EnumSet.of(Method.index, Method.topLevelDV, Method.dvWithScore);
  /**
   * A helper method for other plugins to create (non-scoring) JoinQueries wrapped around arbitrary queries against the same core.
   * 
   * @param subQuery the query to define the starting set of documents on the "left side" of the join
   * @param fromField "left side" field name to use in the join
   * @param toField "right side" field name to use in the join
   * @param method indicates which implementation should be used to process the join.  Currently only 'index',
   *               'dvWithScore', and 'topLevelDV' are supported.
   */
  public static Query createJoinQuery(Query subQuery, String fromField, String toField, String method) {
    // no method defaults to 'index' for back compatibility
    if ( method == null ) {
      return new JoinQuery(fromField, toField, null, subQuery);
    }


    final Method joinMethod = parseMethodString(method);
    if (! JOIN_METHOD_WHITELIST.contains(joinMethod)) {
      // TODO Throw something that the callers here (FacetRequest) can catch and produce a more domain-appropriate error message for?
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Join method " + method + " not supported for non-scoring, same-core joins");
    }

    final JoinParams jParams = new JoinParams(fromField, null, subQuery, 0L, toField);
    return joinMethod.makeJoinDirectFromParams(jParams);
  }

  private static Method parseMethodString(String method) {
    try {
      return Method.valueOf(method);
    } catch (IllegalArgumentException iae) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Provided join method '" + method + "' not supported");
    }
  }
  
}