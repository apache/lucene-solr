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
import java.util.ArrayList;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.solr.analytics.AnalyticsDriver;
import org.apache.solr.analytics.AnalyticsRequestManager;
import org.apache.solr.analytics.AnalyticsRequestParser;
import org.apache.solr.analytics.ExpressionFactory;
import org.apache.solr.analytics.stream.AnalyticsShardResponseParser;
import org.apache.solr.client.solrj.io.ModelCache;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.AnalyticsComponent;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.AnalyticsShardResponseWriter;
import org.apache.solr.response.AnalyticsShardResponseWriter.AnalyticsResponse;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.Filter;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.util.plugin.SolrCoreAware;

/**
 * Handler for Analytics shard requests. This handler should only be called by the {@link AnalyticsComponent}
 * since the response is written in a bit-stream, formatted by the {@link AnalyticsShardResponseWriter}
 * that can only be read by the {@link AnalyticsShardResponseParser}.
 */
public class AnalyticsHandler extends RequestHandlerBase implements SolrCoreAware, PermissionNameProvider {
  public static final String NAME = "/analytics";
  private IndexSchema indexSchema;

  static SolrClientCache clientCache = new SolrClientCache();
  static ModelCache modelCache = null;

  @Override
  public PermissionNameProvider.Name getPermissionName(AuthorizationContext request) {
    return PermissionNameProvider.Name.READ_PERM;
  }

  @Override
  public void inform(SolrCore core) {
    core.registerResponseWriter(AnalyticsShardResponseWriter.NAME, new AnalyticsShardResponseWriter());

    indexSchema = core.getLatestSchema();
    AnalyticsRequestParser.init();
  }

  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    try {
      DocSet docs;
      try {
        docs = getDocuments(req);
      } catch (SyntaxError e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
      }
      // The olap-style requests are converted to the current format in the AnalyticsComponent
      // so the AnalyticsHandler only needs to handle current format requests.
      AnalyticsRequestManager manager = AnalyticsRequestParser.parse(req.getParams().get(AnalyticsRequestParser.analyticsParamName),
                                                                new ExpressionFactory(indexSchema),
                                                                false);
      // Collect the reduction data for the request
      SolrIndexSearcher searcher = req.getSearcher();
      Filter filter = docs.getTopFilter();
      AnalyticsDriver.drive(manager, searcher, filter, req);

      // Do not calculate results, instead export the reduction data for this shard.
      rsp.addResponse(new AnalyticsResponse(manager));
    } catch (SolrException e) {
      rsp.addResponse(new AnalyticsResponse(e));
    }
  }

  /**
   * Get the documents returned by the query and filter queries included in the request.
   *
   * @param req the request sent to the handler
   * @return the set of documents matching the query
   * @throws SyntaxError if there is a syntax error in the queries
   * @throws IOException if an error occurs while searching the index
   */
  private DocSet getDocuments(SolrQueryRequest req) throws SyntaxError, IOException {
    SolrParams params = req.getParams();
    ArrayList<Query> queries = new ArrayList<>();

    // Query Param
    String queryString = params.get( CommonParams.Q );

    String defType = params.get(QueryParsing.DEFTYPE, QParserPlugin.DEFAULT_QTYPE);

    QParser parser = QParser.getParser(queryString, defType, req);
    Query query = parser.getQuery();
    if (query == null) {
      // normalize a null query to a query that matches nothing
      query = new MatchNoDocsQuery();
    }
    queries.add(query);

    // Filter Params
    String[] fqs = req.getParams().getParams(CommonParams.FQ);
    if (fqs!=null) {
      for (String fq : fqs) {
        if (fq != null && fq.trim().length()!=0) {
          QParser fqp = QParser.getParser(fq, req);
          queries.add(fqp.getQuery());
        }
      }
    }
    return req.getSearcher().getDocSet(queries);
  }

  @Override
  public String getDescription() {
    return NAME;
  }

  public String getSource() {
    return null;
  }
}
