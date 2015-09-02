package org.apache.solr.security;

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

import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;

import org.apache.http.auth.BasicUserPrincipal;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.Utils;

public class TestRuleBasedAuthorizationPlugin extends SolrTestCaseJ4 {

  public void testBasicPermissions() {
    int STATUS_OK = 200;
    int FORBIDDEN = 403;
    int PROMPT_FOR_CREDENTIALS = 401;

    String jsonRules= "{" +
        "  user-role : {" +
        "    steve: [dev,user]," +
        "    tim: [dev,admin]," +
        "    joe: [user]," +
        "    noble:[dev,user]" +
        "  }," +
        "  permissions : [" +
        "    {name:'schema-edit'," +
        "     role:admin}," +
        "    {name:'collection-admin-read'," +
        "    role:null}," +
        "    {name:collection-admin-edit ," +
        "    role:admin}," +
        "    {name:mycoll_update," +
        "      collection:mycoll," +
        "      path:'/update/*'," +
        "      role:[dev,admin]" +
        "    }]}" ;
    Map initConfig = (Map) Utils.fromJSON(jsonRules.getBytes(StandardCharsets.UTF_8));

    RuleBasedAuthorizationPlugin plugin= new RuleBasedAuthorizationPlugin();
    plugin.init(initConfig);

    Map<String, Object> values = Utils.makeMap(
        "resource", "/update/json/docs",
        "httpMethod", "POST",
        "collectionRequests", Collections.singletonList(new AuthorizationContext.CollectionRequest("mycoll")),
        "userPrincipal", new BasicUserPrincipal("tim"));
    AuthorizationContext context = new MockAuthorizationContext(values);

    AuthorizationResponse authResp = plugin.authorize(context);
    assertEquals(STATUS_OK, authResp.statusCode);

    values.remove("userPrincipal");
    authResp = plugin.authorize(context);
    assertEquals(PROMPT_FOR_CREDENTIALS,authResp.statusCode);

    values.put("userPrincipal", new BasicUserPrincipal("somebody"));
    authResp = plugin.authorize(context);
    assertEquals(FORBIDDEN,authResp.statusCode);

    values.put("httpMethod","GET");
    values.put("resource","/schema");
    authResp = plugin.authorize(context);
    assertEquals(STATUS_OK,authResp.statusCode);

    values.put("resource","/schema/fields");
    authResp = plugin.authorize(context);
    assertEquals(STATUS_OK,authResp.statusCode);

    values.put("resource","/schema");
    values.put("httpMethod","POST");
    authResp = plugin.authorize(context);
    assertEquals(FORBIDDEN,authResp.statusCode);

    values.put("resource","/admin/collections");
    values.put("requestType", AuthorizationContext.RequestType.ADMIN);
    values.put("params", new MapSolrParams(Collections.singletonMap("action", "LIST")));
    values.put("httpMethod","GET");
    authResp = plugin.authorize(context);
    assertEquals(STATUS_OK,authResp.statusCode);

    values.remove("userPrincipal");
    authResp = plugin.authorize(context);
    assertEquals(STATUS_OK,authResp.statusCode);

    values.put("params", new MapSolrParams(Collections.singletonMap("action", "CREATE")));
    authResp = plugin.authorize(context);
    assertEquals(PROMPT_FOR_CREDENTIALS, authResp.statusCode);

    values.put("params", new MapSolrParams(Collections.singletonMap("action", "RELOAD")));
    authResp = plugin.authorize(context);
    assertEquals(PROMPT_FOR_CREDENTIALS, authResp.statusCode);

    values.put("userPrincipal", new BasicUserPrincipal("somebody"));
    authResp = plugin.authorize(context);
    assertEquals(FORBIDDEN,authResp.statusCode);

    values.put("userPrincipal", new BasicUserPrincipal("tim"));
    authResp = plugin.authorize(context);
    assertEquals(STATUS_OK,authResp.statusCode);


  }

  private static class MockAuthorizationContext extends AuthorizationContext {
    private final Map<String,Object> values;

    private MockAuthorizationContext(Map<String, Object> values) {
      this.values = values;
    }

    @Override
    public SolrParams getParams() {
      return (SolrParams) values.get("params");
    }

    @Override
    public Principal getUserPrincipal() {
      return (Principal) values.get("userPrincipal");
    }

    @Override
    public String getHttpHeader(String header) {
      return null;
    }

    @Override
    public Enumeration getHeaderNames() {
      return null;
    }

    @Override
    public String getRemoteAddr() {
      return null;
    }

    @Override
    public String getRemoteHost() {
      return null;
    }

    @Override
    public List<CollectionRequest> getCollectionRequests() {
      return (List<CollectionRequest>) values.get("collectionRequests");
    }

    @Override
    public RequestType getRequestType() {
      return (RequestType) values.get("requestType");
    }

    @Override
    public String getHttpMethod() {
      return (String) values.get("httpMethod");
    }

    @Override
    public String getResource() {
      return (String) values.get("resource");
    }
  }


}
