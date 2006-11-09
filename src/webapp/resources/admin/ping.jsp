<%@ page contentType="text/html; charset=utf-8" pageEncoding="UTF-8"%>
<%--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
--%>
<%@ page import="org.apache.solr.core.SolrConfig,
                 org.apache.solr.core.SolrCore,
                 org.apache.solr.core.SolrException"%>
<%@ page import="org.apache.solr.request.LocalSolrQueryRequest"%>
<%@ page import="org.apache.solr.request.SolrQueryResponse"%>
<%@ page import="java.util.StringTokenizer"%>
<%
  SolrCore core = SolrCore.getSolrCore();

  String queryArgs = (request.getQueryString() == null) ?
      SolrConfig.config.get("admin/pingQuery","") : request.getQueryString();
  StringTokenizer qtokens = new StringTokenizer(queryArgs,"&");
  String tok;
  String query = null;
  while (qtokens.hasMoreTokens()) {
    tok = qtokens.nextToken();
    String[] split = tok.split("=");
    if (split[0].startsWith("q")) {
      query = split[1];
    }
  }
  LocalSolrQueryRequest req = new LocalSolrQueryRequest(core, query,null,0,1,LocalSolrQueryRequest.emptyArgs);
  SolrQueryResponse resp = new SolrQueryResponse();
  try {
    core.execute(req,resp);
    if (resp.getException() != null) {
      response.sendError(500, SolrException.toStr(resp.getException()));
    }
  } catch (Throwable t) {
      response.sendError(500, SolrException.toStr(t));
  } finally {
      req.close();
  }
%>
