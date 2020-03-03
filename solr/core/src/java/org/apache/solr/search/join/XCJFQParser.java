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

package org.apache.solr.search.join;

import org.apache.lucene.search.Query;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SyntaxError;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

@SuppressWarnings("WeakerAccess")
public class XCJFQParser extends QParser {

  public static final String ZK_HOST = "zkHost";
  public static final String SOLR_URL = "solrUrl";
  public static final String COLLECTION = "collection";
  public static final String FROM = "from";
  public static final String TO = "to";
  public static final String ROUTED_BY_JOIN_KEY = "routed";
  public static final String TTL = "ttl";

  public static final int TTL_DEFAULT = 60 * 60; // in seconds

  private static final Set<String> OWN_PARAMS = new HashSet<>(Arrays.asList(
          QueryParsing.TYPE, QueryParsing.V, ZK_HOST, SOLR_URL, COLLECTION, FROM, TO, ROUTED_BY_JOIN_KEY, TTL));

  private final String routerField;
  private final Set<String> solrUrlWhitelist;

  public XCJFQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req, String routerField, Set<String> solrUrlWhiteList) {
    super(qstr, localParams, params, req);
    this.routerField = routerField;
    // If specified in the config, this will limit which solr url's the parser can connect to.
    this.solrUrlWhitelist = solrUrlWhiteList;
  }

  @Override
  public Query parse() throws SyntaxError {
    String query = localParams.get(QueryParsing.V);
    String zkHost = localParams.get(ZK_HOST);
    String solrUrl = localParams.get(SOLR_URL);
    // Test if this is a valid solr url.
    if (solrUrl != null) {
      if (solrUrlWhitelist == null) {
        throw new SyntaxError("White list must be configured to use solrUrl parameter.");
      }
      if (!solrUrlWhitelist.contains(solrUrl)) {
        throw new SyntaxError("Solr Url was not in the whitelist.  Please check your configuration.");
      }
    }

    String collection = localParams.get(COLLECTION);
    String fromField = localParams.get(FROM);
    String toField = localParams.get(TO);
    boolean routedByJoinKey = localParams.getBool(ROUTED_BY_JOIN_KEY, toField.equals(routerField));
    int ttl = localParams.getInt(TTL, TTL_DEFAULT);

    ModifiableSolrParams otherParams = new ModifiableSolrParams();
    for (Iterator<String> it = localParams.getParameterNamesIterator(); it.hasNext(); ) {
      String paramName = it.next();
      if (!OWN_PARAMS.contains(paramName)) {
        otherParams.set(paramName, localParams.getParams(paramName));
      }
    }

    return new XCJFQuery(query, zkHost, solrUrl, collection, fromField, toField, routedByJoinKey, ttl, otherParams);
  }
}
