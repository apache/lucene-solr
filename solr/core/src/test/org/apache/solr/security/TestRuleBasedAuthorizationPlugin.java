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
package org.apache.solr.security;

import java.io.IOException;
import java.io.StringReader;
import java.security.Principal;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.http.auth.BasicUserPrincipal;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.handler.ReplicationHandler;
import org.apache.solr.handler.SchemaHandler;
import org.apache.solr.handler.UpdateRequestHandler;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.handler.component.SearchHandler;
import org.apache.solr.security.AuthorizationContext.CollectionRequest;
import org.apache.solr.security.AuthorizationContext.RequestType;
import org.apache.solr.util.CommandOperation;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.solr.common.util.Utils.getObjectByPath;
import static org.apache.solr.common.util.Utils.makeMap;
import static org.apache.solr.util.CommandOperation.captureErrors;

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
      "    }," +
      "{name:read , role:dev }," +
      "{name:freeforall, path:'/foo', role:'*'}]}";



  public void testBasicPermissions() {
    int STATUS_OK = 200;
    int FORBIDDEN = 403;
    int PROMPT_FOR_CREDENTIALS = 401;

    checkRules(makeMap("resource", "/update/json/docs",
        "httpMethod", "POST",
        "userPrincipal", "unknownuser",
        "collectionRequests", "freeforall",
        "handler", new UpdateRequestHandler())
        , STATUS_OK);

    checkRules(makeMap("resource", "/update/json/docs",
        "httpMethod", "POST",
        "userPrincipal", "tim",
        "collectionRequests", "mycoll",
        "handler", new UpdateRequestHandler())
        , STATUS_OK);


    checkRules(makeMap("resource", "/update/json/docs",
        "httpMethod", "POST",
        "collectionRequests", "mycoll",
        "handler", new UpdateRequestHandler())
        , PROMPT_FOR_CREDENTIALS);

    checkRules(makeMap("resource", "/schema",
        "userPrincipal", "somebody",
        "collectionRequests", "mycoll",
        "httpMethod", "POST",
        "handler", new SchemaHandler())
        , FORBIDDEN);

    checkRules(makeMap("resource", "/schema",
        "userPrincipal", "somebody",
        "collectionRequests", "mycoll",
        "httpMethod", "GET",
        "handler", new SchemaHandler()
    )
        , STATUS_OK);

    checkRules(makeMap("resource", "/schema/fields",
        "userPrincipal", "somebody",
        "collectionRequests", "mycoll",
        "httpMethod", "GET",
        "handler", new SchemaHandler())
        , STATUS_OK);

    checkRules(makeMap("resource", "/schema",
        "userPrincipal", "somebody",
        "collectionRequests", "mycoll",
        "httpMethod", "POST",
        "handler", new SchemaHandler())
        , FORBIDDEN);

    checkRules(makeMap("resource", "/admin/collections",
        "userPrincipal", "tim",
        "requestType", RequestType.ADMIN,
        "collectionRequests", null,
        "httpMethod", "GET",
        "handler", new CollectionsHandler(),
        "params", new MapSolrParams(singletonMap("action", "LIST")))
        , STATUS_OK);

    checkRules(makeMap("resource", "/admin/collections",
        "userPrincipal", null,
        "requestType", RequestType.ADMIN,
        "collectionRequests", null,
        "httpMethod", "GET",
        "handler", new CollectionsHandler(),
        "params", new MapSolrParams(singletonMap("action", "LIST")))
        , STATUS_OK);

    checkRules(makeMap("resource", "/admin/collections",
        "userPrincipal", null,
        "requestType", RequestType.ADMIN,
        "collectionRequests", null,
        "handler", new CollectionsHandler(),
        "params", new MapSolrParams(singletonMap("action", "CREATE")))
        , PROMPT_FOR_CREDENTIALS);

    checkRules(makeMap("resource", "/admin/collections",
        "userPrincipal", null,
        "requestType", RequestType.ADMIN,
        "collectionRequests", null,
        "handler", new CollectionsHandler(),
        "params", new MapSolrParams(singletonMap("action", "RELOAD")))
        , PROMPT_FOR_CREDENTIALS);


    checkRules(makeMap("resource", "/admin/collections",
        "userPrincipal", "somebody",
        "requestType", RequestType.ADMIN,
        "collectionRequests", null,
        "handler", new CollectionsHandler(),
        "params", new MapSolrParams(singletonMap("action", "CREATE")))
        , FORBIDDEN);

    checkRules(makeMap("resource", "/admin/collections",
        "userPrincipal", "tim",
        "requestType", RequestType.ADMIN,
        "collectionRequests", null,
        "handler", new CollectionsHandler(),
        "params", new MapSolrParams(singletonMap("action", "CREATE")))
        , STATUS_OK);

    checkRules(makeMap("resource", "/select",
        "httpMethod", "GET",
        "handler", new SearchHandler(),
        "collectionRequests", singletonList(new CollectionRequest("mycoll")),
        "userPrincipal", "joe")
        , FORBIDDEN);


    Map rules = (Map) Utils.fromJSONString(permissions);
    ((Map)rules.get("user-role")).put("cio","su");
    ((List)rules.get("permissions")).add( makeMap("name", "all", "role", "su"));

    checkRules(makeMap("resource", "/replication",
        "httpMethod", "POST",
        "userPrincipal", "tim",
        "handler", new ReplicationHandler(),
        "collectionRequests", singletonList(new CollectionRequest("mycoll")) )
        , FORBIDDEN, rules);

    checkRules(makeMap("resource", "/replication",
        "httpMethod", "POST",
        "userPrincipal", "cio",
        "handler", new ReplicationHandler(),
        "collectionRequests", singletonList(new CollectionRequest("mycoll")) )
        , STATUS_OK, rules);

    checkRules(makeMap("resource", "/admin/collections",
        "userPrincipal", "tim",
        "requestType", AuthorizationContext.RequestType.ADMIN,
        "collectionRequests", null,
        "handler", new CollectionsHandler(),
        "params", new MapSolrParams(singletonMap("action", "CREATE")))
        , STATUS_OK, rules);

    rules = (Map) Utils.fromJSONString(permissions);
    ((List)rules.get("permissions")).add( makeMap("name", "core-admin-edit", "role", "su"));
    ((List)rules.get("permissions")).add( makeMap("name", "core-admin-read", "role", "user"));
    ((Map)rules.get("user-role")).put("cio","su");
    ((List)rules.get("permissions")).add( makeMap("name", "all", "role", "su"));
    permissions = Utils.toJSONString(rules);

    checkRules(makeMap("resource", "/admin/cores",
        "userPrincipal", null,
        "requestType", RequestType.ADMIN,
        "collectionRequests", null,
        "handler", new CoreAdminHandler(null),
        "params", new MapSolrParams(singletonMap("action", "CREATE")))
        , PROMPT_FOR_CREDENTIALS);

    checkRules(makeMap("resource", "/admin/cores",
        "userPrincipal", "joe",
        "requestType", RequestType.ADMIN,
        "collectionRequests", null,
        "handler", new CoreAdminHandler(null),
        "params", new MapSolrParams(singletonMap("action", "CREATE")))
        , FORBIDDEN);

  checkRules(makeMap("resource", "/admin/cores",
        "userPrincipal", "joe",
        "requestType", RequestType.ADMIN,
        "collectionRequests", null,
        "handler", new CoreAdminHandler(null),
        "params", new MapSolrParams(singletonMap("action", "STATUS")))
        , STATUS_OK);

    checkRules(makeMap("resource", "/admin/cores",
        "userPrincipal", "cio",
        "requestType", RequestType.ADMIN,
        "collectionRequests", null,
        "handler", new CoreAdminHandler(null),
        "params", new MapSolrParams(singletonMap("action", "CREATE")))
        ,STATUS_OK );

  }

  public void testEditRules() throws IOException {
    Perms perms =  new Perms();
    perms.runCmd("{set-permission : {name: config-edit, role: admin } }", true);
    assertEquals("config-edit",  getObjectByPath(perms.conf, false, "permissions[0]/name"));
    assertEquals(1 , perms.getVal("permissions[0]/index"));
    assertEquals("admin" ,  perms.getVal("permissions[0]/role"));
    perms.runCmd("{set-permission : {name: config-edit, role: [admin, dev], index:2 } }", false);
    perms.runCmd("{set-permission : {name: config-edit, role: [admin, dev], index:1}}", true);
    Collection roles = (Collection) perms.getVal("permissions[0]/role");
    assertEquals(2, roles.size());
    assertTrue(roles.contains("admin"));
    assertTrue(roles.contains("dev"));
    perms.runCmd("{set-permission : {role: [admin, dev], collection: x , path: '/a/b' , method :[GET, POST] }}", true);
    assertNotNull(perms.getVal("permissions[1]"));
    assertEquals("x", perms.getVal("permissions[1]/collection"));
    assertEquals("/a/b", perms.getVal("permissions[1]/path"));
    perms.runCmd("{update-permission : {index : 2, method : POST }}", true);
    assertEquals("POST" , perms.getVal("permissions[1]/method"));
    perms.runCmd("{set-permission : {name : read, collection : y, role:[guest, dev] ,  before :2}}", true);
    assertNotNull(perms.getVal("permissions[2]"));
    assertEquals("y", perms.getVal("permissions[1]/collection"));
    assertEquals("read", perms.getVal("permissions[1]/name"));
    perms.runCmd("{delete-permission : 3}", true);
    assertTrue(captureErrors(perms.parsedCommands).isEmpty());
    assertEquals("y",perms.getVal("permissions[1]/collection"));
  }

  static class  Perms {
    Map conf =  new HashMap<>();
    RuleBasedAuthorizationPlugin plugin = new RuleBasedAuthorizationPlugin();
    List<CommandOperation> parsedCommands;

    public void runCmd(String cmds, boolean failOnError) throws IOException {
      parsedCommands = CommandOperation.parse(new StringReader(cmds));
      LinkedList ll = new LinkedList();
      Map<String, Object> edited = plugin.edit(conf, parsedCommands);
      if(edited!= null) conf = edited;
      List<Map> maps = captureErrors(parsedCommands);
      if(failOnError){
        assertTrue("unexpected error ,"+maps , maps.isEmpty());
      } else {
        assertFalse("expected error", maps.isEmpty());
      }
    }
    public Object getVal(String path){
      return getObjectByPath(conf,false, path);
    }
  }

  private void checkRules(Map<String, Object> values, int expected) {
    checkRules(values,expected,(Map) Utils.fromJSONString(permissions));
  }

  private void checkRules(Map<String, Object> values, int expected, Map<String ,Object> permissions) {
    AuthorizationContext context = new MockAuthorizationContext(values);
    RuleBasedAuthorizationPlugin plugin = new RuleBasedAuthorizationPlugin();
    plugin.init(permissions);
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
      Object collectionRequests = values.get("collectionRequests");
      if (collectionRequests instanceof String) {
        return singletonList(new CollectionRequest((String)collectionRequests));
      }
      return (List<CollectionRequest>) collectionRequests;
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
    public Object getHandler() {
      Object handler = values.get("handler");
      return handler instanceof String ? (PermissionNameProvider) request -> PermissionNameProvider.Name.get((String) handler) : handler;
    }

    @Override
    public String getResource() {
      return (String) values.get("resource");
    }
  }


}
