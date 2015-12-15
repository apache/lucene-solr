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

import java.security.Principal;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.auth.BasicUserPrincipal;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.security.AuthorizationContext.CollectionRequest;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.solr.common.util.Utils.makeMap;

public class TestRuleBasedAuthorizationPlugin extends SolrTestCaseJ4 {
  String permissions = "{" +
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
      "    },{name:read , role:dev }]}";


  public void testBasicPermissions() {
    int STATUS_OK = 200;
    int FORBIDDEN = 403;
    int PROMPT_FOR_CREDENTIALS = 401;

    checkRules(makeMap("resource", "/update/json/docs",
        "httpMethod", "POST",
        "userPrincipal", "tim",
        "collectionRequests", singletonList(new CollectionRequest("mycoll")) )
        , STATUS_OK);


    checkRules(makeMap("resource", "/update/json/docs",
        "httpMethod", "POST",
        "collectionRequests", singletonList(new CollectionRequest("mycoll")) )
        , PROMPT_FOR_CREDENTIALS);

    checkRules(makeMap("resource", "/schema",
        "userPrincipal", "somebody",
        "collectionRequests", singletonList(new CollectionRequest("mycoll")),
        "httpMethod", "POST")
        , FORBIDDEN);

    checkRules(makeMap("resource", "/schema",
        "userPrincipal", "somebody",
        "collectionRequests", singletonList(new CollectionRequest("mycoll")),
        "httpMethod", "GET")
        , STATUS_OK);

    checkRules(makeMap("resource", "/schema/fields",
        "userPrincipal", "somebody",
        "collectionRequests", singletonList(new CollectionRequest("mycoll")),
        "httpMethod", "GET")
        , STATUS_OK);

    checkRules(makeMap("resource", "/schema",
        "userPrincipal", "somebody",
        "collectionRequests", singletonList(new CollectionRequest("mycoll")),
        "httpMethod", "POST" )
        , FORBIDDEN);

    checkRules(makeMap("resource", "/admin/collections",
        "userPrincipal", "tim",
        "requestType", AuthorizationContext.RequestType.ADMIN,
        "collectionRequests", null,
        "httpMethod", "GET",
        "params", new MapSolrParams(singletonMap("action", "LIST")))
        , STATUS_OK);

    checkRules(makeMap("resource", "/admin/collections",
        "userPrincipal", null,
        "requestType", AuthorizationContext.RequestType.ADMIN,
        "collectionRequests", null,
        "httpMethod", "GET",
        "params", new MapSolrParams(singletonMap("action", "LIST")))
        , STATUS_OK);

    checkRules(makeMap("resource", "/admin/collections",
        "userPrincipal", null,
        "requestType", AuthorizationContext.RequestType.ADMIN,
        "collectionRequests", null,
        "params", new MapSolrParams(singletonMap("action", "CREATE")))
        , PROMPT_FOR_CREDENTIALS);

    checkRules(makeMap("resource", "/admin/collections",
        "userPrincipal", null,
        "requestType", AuthorizationContext.RequestType.ADMIN,
        "collectionRequests", null,
        "params", new MapSolrParams(singletonMap("action", "RELOAD")))
        , PROMPT_FOR_CREDENTIALS);


    checkRules(makeMap("resource", "/admin/collections",
        "userPrincipal", "somebody",
        "requestType", AuthorizationContext.RequestType.ADMIN,
        "collectionRequests", null,
        "params", new MapSolrParams(singletonMap("action", "CREATE")))
        , FORBIDDEN);

    checkRules(makeMap("resource", "/admin/collections",
        "userPrincipal", "tim",
        "requestType", AuthorizationContext.RequestType.ADMIN,
        "collectionRequests", null,
        "params", new MapSolrParams(singletonMap("action", "CREATE")))
        , STATUS_OK);

    checkRules(makeMap("resource", "/select",
        "httpMethod", "GET",
        "collectionRequests", singletonList(new CollectionRequest("mycoll")),
        "userPrincipal", "joe")
        , FORBIDDEN);

  }



  private void checkRules(Map<String, Object> values, int expected) {

    AuthorizationContext context = new MockAuthorizationContext(values);
    RuleBasedAuthorizationPlugin plugin = new RuleBasedAuthorizationPlugin();
    plugin.init((Map) Utils.fromJSONString(permissions));
    AuthorizationResponse authResp = plugin.authorize(context);
    assertEquals(expected, authResp.statusCode);
  }

  private static class MockAuthorizationContext extends AuthorizationContext {
    private final Map<String,Object> values;

    private MockAuthorizationContext(Map<String, Object> values) {
      this.values = values;
    }

    @Override
    public SolrParams getParams() {
      SolrParams params = (SolrParams) values.get("params");
      return params == null ?  new MapSolrParams(new HashMap<String, String>()) : params;
    }

    @Override
    public Principal getUserPrincipal() {
      Object userPrincipal = values.get("userPrincipal");
      return userPrincipal == null ? null : new BasicUserPrincipal(String.valueOf(userPrincipal));
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
