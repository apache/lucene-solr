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

package org.apache.solr.api;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.servlet.CoordinatorHttpSolrCall;
import org.apache.solr.servlet.SolrDispatchFilter;

public class CoordinatorV2HttpSolrCall extends V2HttpCall {
  private String collectionName;
  CoordinatorHttpSolrCall.Factory factory;

  public CoordinatorV2HttpSolrCall(
      CoordinatorHttpSolrCall.Factory factory,
      SolrDispatchFilter solrDispatchFilter,
      CoreContainer cc,
      HttpServletRequest request,
      HttpServletResponse response,
      boolean retry) {
    super(solrDispatchFilter, cc, request, response, retry);
    this.factory = factory;
  }

  @Override
  protected SolrCore getCoreByCollection(String collectionName, boolean isPreferLeader) {
    this.collectionName = collectionName;
    SolrCore core = super.getCoreByCollection(collectionName, isPreferLeader);
    if (core != null) return core;
    if (!path.endsWith("/select")) return null;
    return CoordinatorHttpSolrCall.getCore(factory, this, collectionName, isPreferLeader);
  }

  @Override
  protected void init() throws Exception {
    super.init();
    if (action == SolrDispatchFilter.Action.PROCESS && core != null) {
      solrReq = CoordinatorHttpSolrCall.wrappedReq(solrReq, collectionName, this);
    }
  }
}
