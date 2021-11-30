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
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.http.auth.BasicUserPrincipal;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.common.util.Utils;
import org.apache.solr.handler.DumpRequestHandler;
import org.apache.solr.handler.ReplicationHandler;
import org.apache.solr.handler.SchemaHandler;
import org.apache.solr.handler.UpdateRequestHandler;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.handler.admin.PropertiesRequestHandler;
import org.apache.solr.handler.component.SearchHandler;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.security.AuthorizationContext.CollectionRequest;
import org.apache.solr.security.AuthorizationContext.RequestType;
import org.hamcrest.core.IsInstanceOf;
import org.hamcrest.core.IsNot;
import org.junit.Test;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.solr.common.util.CommandOperation.captureErrors;
import static org.apache.solr.common.util.Utils.getObjectByPath;
import static org.apache.solr.common.util.Utils.makeMap;

/**
 * Base class for testing RBAC. This will test the {@link RuleBasedAuthorizationPlugin} implementation
 * but also serves as a base class for testing other sub classes
 */
@SuppressWarnings("unchecked")
public class BaseTestRuleBasedAuthorizationPlugin extends SolrTestCaseJ4 {
  @SuppressWarnings({"rawtypes"})
  protected Map rules;

  final int STATUS_OK = 200;
  final int FORBIDDEN = 403;
  final int PROMPT_FOR_CREDENTIALS = 401;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    resetPermissionsAndRoles();
  }

  protected void resetPermissionsAndRoles() {
    String permissions = "{" +
        "  user-role : {" +
        "    steve: [dev,user]," +
        "    tim: [dev,admin]," +
        "    joe: [user]," +
        "    noble:[dev,user]" +
        "  }," +
        "  permissions : [" +
        "    {name:'schema-edit'," +
        "      role:admin}," +
        "    {name:'collection-admin-read'," +
        "      role:null}," +
        "    {name:collection-admin-edit ," +
        "      role:admin}," +
        "    {name:mycoll_update," +
        "      collection:mycoll," +
        "      path:'/update/*'," +
        "      role:[dev,admin]" +
        "    }," +
        "{name:read, role:dev }," +
        "{name:freeforall, path:'/foo', role:'*'}]}";
    rules = (Map) Utils.fromJSONString(permissions);
  }

  @Test
  public void testBasicPermissions() {
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
        "handler", new SchemaHandler())
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

    setUserRole("cio", "su");
    addPermission("all", "su");

    checkRules(makeMap("resource", ReplicationHandler.PATH,
        "httpMethod", "POST",
        "userPrincipal", "tim",
        "handler", new ReplicationHandler(),
        "collectionRequests", singletonList(new CollectionRequest("mycoll")) )
        , FORBIDDEN);

    checkRules(makeMap("resource", ReplicationHandler.PATH,
        "httpMethod", "POST",
        "userPrincipal", "cio",
        "handler", new ReplicationHandler(),
        "collectionRequests", singletonList(new CollectionRequest("mycoll")) )
        , STATUS_OK);

    checkRules(makeMap("resource", "/admin/collections",
        "userPrincipal", "tim",
        "requestType", AuthorizationContext.RequestType.ADMIN,
        "collectionRequests", null,
        "handler", new CollectionsHandler(),
        "params", new MapSolrParams(singletonMap("action", "CREATE")))
        , STATUS_OK);

    resetPermissionsAndRoles();
    addPermission("core-admin-edit", "su");
    addPermission("core-admin-read", "user");
    setUserRole("cio", "su");
    addPermission("all", "su");

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
        ,STATUS_OK);

    resetPermissionsAndRoles();
    addPermission("test-params", "admin", "/x", makeMap("key", Arrays.asList("REGEX:(?i)val1", "VAL2")));

    checkRules(makeMap("resource", "/x",
        "userPrincipal", null,
        "requestType", RequestType.UNKNOWN,
        "collectionRequests", "go",
        "handler", new DumpRequestHandler(),
        "params", new MapSolrParams(singletonMap("key", "VAL1")))
        , PROMPT_FOR_CREDENTIALS);

    checkRules(makeMap("resource", "/x",
        "userPrincipal", null,
        "requestType", RequestType.UNKNOWN,
        "collectionRequests", "go",
        "handler", new DumpRequestHandler(),
        "params", new MapSolrParams(singletonMap("key", "Val1")))
        , PROMPT_FOR_CREDENTIALS);

    checkRules(makeMap("resource", "/x",
        "userPrincipal", null,
        "requestType", RequestType.UNKNOWN,
        "collectionRequests", "go",
        "handler", new DumpRequestHandler(),
        "params", new MapSolrParams(singletonMap("key", "Val1")))
        , PROMPT_FOR_CREDENTIALS);

    checkRules(makeMap("resource", "/x",
        "userPrincipal", "joe",
        "requestType", RequestType.UNKNOWN,
        "collectionRequests", "go",
        "handler", new DumpRequestHandler(),
        "params", new MapSolrParams(singletonMap("key", "Val1")))
        , FORBIDDEN);

    checkRules(makeMap("resource", "/x",
        "userPrincipal", "joe",
        "requestType", RequestType.UNKNOWN,
        "collectionRequests", "go",
        "handler", new DumpRequestHandler(),
        "params", new MapSolrParams(singletonMap("key", "Val2")))
        , STATUS_OK);

    checkRules(makeMap("resource", "/x",
        "userPrincipal", "joe",
        "requestType", RequestType.UNKNOWN,
        "collectionRequests", "go",
        "handler", new DumpRequestHandler(),
        "params", new MapSolrParams(singletonMap("key", "VAL2")))
        , FORBIDDEN);

    Map<String, Object> customRules = (Map<String, Object>) Utils.fromJSONString(
        "{permissions:[" +
        "      {name:update, role:[admin_role,update_role]}," +
        "      {name:read, role:[admin_role,update_role,read_role]}" +
        "]}");

    clearUserRoles();
    setUserRole("admin", "admin_role");
    setUserRole("update", "update_role");
    setUserRole("solr", "read_role");

    checkRules(makeMap("resource", "/update",
        "userPrincipal", "solr",
        "requestType", RequestType.UNKNOWN,
        "collectionRequests", "go",
        "handler", new UpdateRequestHandler(),
        "params", new MapSolrParams(singletonMap("key", "VAL2")))
        , FORBIDDEN, customRules);
  }

  /*
   * RuleBasedAuthorizationPlugin handles requests differently based on whether the underlying handler implements
   * PermissionNameProvider or not.  If this test fails because UpdateRequestHandler stops implementing
   * PermissionNameProvider, or PropertiesRequestHandler starts to, then just change the handlers used here.
   */
  @Test
  public void testAllPermissionAllowsActionsWhenUserHasCorrectRole() {
    SolrRequestHandler handler = new UpdateRequestHandler();
    assertThat(handler, new IsInstanceOf(PermissionNameProvider.class));
    setUserRole("dev", "dev");
    setUserRole("admin", "admin");
    addPermission("all", "dev", "admin");
    checkRules(makeMap("resource", "/update",
        "userPrincipal", "dev",
        "requestType", RequestType.UNKNOWN,
        "collectionRequests", "go",
        "handler", handler,
        "params", new MapSolrParams(singletonMap("key", "VAL2")))
        , STATUS_OK);

    handler = new PropertiesRequestHandler();
    assertThat(handler, new IsNot<>(new IsInstanceOf(PermissionNameProvider.class)));
    checkRules(makeMap("resource", "/admin/info/properties",
        "userPrincipal", "dev",
        "requestType", RequestType.UNKNOWN,
        "collectionRequests", "go",
        "handler", handler,
        "params", new MapSolrParams(emptyMap()))
        , STATUS_OK);
  }


  /*
   * RuleBasedAuthorizationPlugin handles requests differently based on whether the underlying handler implements
   * PermissionNameProvider or not.  If this test fails because UpdateRequestHandler stops implementing
   * PermissionNameProvider, or PropertiesRequestHandler starts to, then just change the handlers used here.
   */
  @Test
  public void testAllPermissionAllowsActionsWhenAssociatedRoleIsWildcard() {
    SolrRequestHandler handler = new UpdateRequestHandler();
    assertThat(handler, new IsInstanceOf(PermissionNameProvider.class));
    setUserRole("dev", "dev");
    setUserRole("admin", "admin");
    addPermission("all", "*");
    checkRules(makeMap("resource", "/update",
        "userPrincipal", "dev",
        "requestType", RequestType.UNKNOWN,
        "collectionRequests", "go",
        "handler", new UpdateRequestHandler(),
        "params", new MapSolrParams(singletonMap("key", "VAL2")))
        , STATUS_OK);

    handler = new PropertiesRequestHandler();
    assertThat(handler, new IsNot<>(new IsInstanceOf(PermissionNameProvider.class)));
    checkRules(makeMap("resource", "/admin/info/properties",
        "userPrincipal", "dev",
        "requestType", RequestType.UNKNOWN,
        "collectionRequests", "go",
        "handler", handler,
        "params", new MapSolrParams(emptyMap()))
        , STATUS_OK);
  }

  /*
   * RuleBasedAuthorizationPlugin handles requests differently based on whether the underlying handler implements
   * PermissionNameProvider or not.  If this test fails because UpdateRequestHandler stops implementing
   * PermissionNameProvider, or PropertiesRequestHandler starts to, then just change the handlers used here.
   */
  @Test
  public void testAllPermissionDeniesActionsWhenUserIsNotCorrectRole() {
    SolrRequestHandler handler = new UpdateRequestHandler();
    assertThat(handler, new IsInstanceOf(PermissionNameProvider.class));
    setUserRole("dev", "dev");
    setUserRole("admin", "admin");
    addPermission("all", "admin");
    checkRules(makeMap("resource", "/update",
        "userPrincipal", "dev",
        "requestType", RequestType.UNKNOWN,
        "collectionRequests", "go",
        "handler", new UpdateRequestHandler(),
        "params", new MapSolrParams(singletonMap("key", "VAL2")))
        , FORBIDDEN);

    handler = new PropertiesRequestHandler();
    assertThat(handler, new IsNot<>(new IsInstanceOf(PermissionNameProvider.class)));
    checkRules(makeMap("resource", "/admin/info/properties",
        "userPrincipal", "dev",
        "requestType", RequestType.UNKNOWN,
        "collectionRequests", "go",
        "handler", handler,
        "params", new MapSolrParams(emptyMap()))
        , FORBIDDEN);
  }

  void addPermission(String permissionName, String role, String path, Map<String, Object> params) {
    ((List)rules.get("permissions")).add( makeMap("name", permissionName, "role", role, "path", path, "params", params));
  }

  void removePermission(String name) {
    List<Map<String,Object>> oldPerm = ((List) rules.get("permissions"));
    List<Map<String, Object>> newPerm = oldPerm.stream().filter(p -> !p.get("name").equals(name)).collect(Collectors.toList());
    rules.put("permissions", newPerm);
  }

  protected void addPermission(String permissionName, String... roles) {
    ((List)rules.get("permissions")).add( makeMap("name", permissionName, "role", Arrays.asList(roles)));
  }

  void clearUserRoles() {
    rules.put("user-role", new HashMap<String,Object>());
  }

  protected void setUserRole(String user, String role) {
    ((Map)rules.get("user-role")).put(user, role);
  }

  public void testEditRules() throws IOException {
    Perms perms =  new Perms();
    perms.runCmd("{set-permission : {name: config-edit, role: admin } }", true);
    assertEquals("config-edit",  getObjectByPath(perms.conf, false, "permissions[0]/name"));
    assertEquals(1 , perms.getVal("permissions[0]/index"));
    assertEquals("admin", perms.getVal("permissions[0]/role"));
    perms.runCmd("{set-permission : {name: config-edit, role: [admin, dev], index:2 } }", false);
    perms.runCmd("{set-permission : {name: config-edit, role: [admin, dev], index:1}}", true);
    @SuppressWarnings({"rawtypes"})
    Collection roles = (Collection) perms.getVal("permissions[0]/role");
    assertEquals(2, roles.size());
    assertTrue(roles.contains("admin"));
    assertTrue(roles.contains("dev"));
    perms.runCmd("{set-permission : {role: [admin, dev], collection: x , path: '/a/b' , method :[GET, POST] }}", true);
    assertNotNull(perms.getVal("permissions[1]"));
    assertEquals("x", perms.getVal("permissions[1]/collection"));
    assertEquals("/a/b", perms.getVal("permissions[1]/path"));
    perms.runCmd("{update-permission : {index : 2, method : POST }}", true);
    assertEquals("POST", perms.getVal("permissions[1]/method"));
    assertEquals("/a/b", perms.getVal("permissions[1]/path"));

    perms.runCmd("{set-permission : {name : read, collection : y, role:[guest, dev] ,  before :2}}", true);
    assertNotNull(perms.getVal("permissions[2]"));
    assertEquals("y", perms.getVal("permissions[1]/collection"));
    assertEquals("read", perms.getVal("permissions[1]/name"));
    assertEquals("POST", perms.getVal("permissions[2]/method"));
    assertEquals("/a/b", perms.getVal("permissions[2]/path"));

    perms.runCmd("{delete-permission : 3}", true);
    assertTrue(captureErrors(perms.parsedCommands).isEmpty());
    assertEquals("y", perms.getVal("permissions[1]/collection"));

    List<Map<String,Object>> permList = (List<Map<String,Object>>)perms.getVal("permissions");
    assertEquals(2, permList.size());
    assertEquals("config-edit", perms.getVal("permissions[0]/name"));
    assertEquals(1, perms.getVal("permissions[0]/index"));
    assertEquals("read", perms.getVal("permissions[1]/name"));
    assertEquals(2, perms.getVal("permissions[1]/index"));

    // delete a non-existent permission
    perms.runCmd("{delete-permission : 3}", false);
    assertEquals(1, perms.parsedCommands.size());
    assertEquals(1, perms.parsedCommands.get(0).getErrors().size());
    assertEquals("No such index: 3", perms.parsedCommands.get(0).getErrors().get(0));

    perms.runCmd("{delete-permission : 1}", true);
    assertTrue(captureErrors(perms.parsedCommands).isEmpty());
    permList = (List<Map<String,Object>>)perms.getVal("permissions");
    assertEquals(1, permList.size());
    // indexes should have been re-ordered after the delete, so now "read" has index==1
    assertEquals("read", perms.getVal("permissions[0]/name"));
    assertEquals(1, perms.getVal("permissions[0]/index"));
    // delete last remaining
    perms.runCmd("{delete-permission : 1}", true);
    assertTrue(captureErrors(perms.parsedCommands).isEmpty());
    permList = (List<Map<String,Object>>)perms.getVal("permissions");
    assertEquals(0, permList.size());
  }

  static class  Perms {
    @SuppressWarnings({"rawtypes"})
    Map conf =  new HashMap<>();
    ConfigEditablePlugin plugin = new RuleBasedAuthorizationPlugin();
    List<CommandOperation> parsedCommands;

    public void runCmd(String cmds, boolean failOnError) throws IOException {
      parsedCommands = CommandOperation.parse(new StringReader(cmds));
      @SuppressWarnings({"rawtypes"})
      LinkedList ll = new LinkedList();
      Map<String, Object> edited = plugin.edit(conf, parsedCommands);
      if(edited!= null) conf = edited;
      @SuppressWarnings({"rawtypes"})
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

  void checkRules(Map<String, Object> values, int expected) {
    checkRules(values, expected, rules);
  }

  void checkRules(Map<String, Object> values, int expected, Map<String, Object> permissions) {
    AuthorizationContext context = getMockContext(values);
    try (RuleBasedAuthorizationPluginBase plugin = createPlugin()) {
      plugin.init(permissions);
      AuthorizationResponse authResp = plugin.authorize(context);
      assertEquals(expected, authResp.statusCode);
    } catch (IOException e) {
      ; // swallow error, otherwise a you have to add a _lot_ of exceptions to methods.
    }
  }

  protected RuleBasedAuthorizationPluginBase createPlugin() {
    return new RuleBasedAuthorizationPlugin();
  }

  AuthorizationContext getMockContext(Map<String, Object> values) {
    return new MockAuthorizationContext(values) {
      @Override
      public Principal getUserPrincipal() {
        Object userPrincipal = values.get("userPrincipal");
        return userPrincipal == null ? null : new BasicUserPrincipal(String.valueOf(userPrincipal));
      }
    };
  }

  protected abstract class MockAuthorizationContext extends AuthorizationContext {
    private final Map<String,Object> values;

    public MockAuthorizationContext(Map<String, Object> values) {
      this.values = values;
    }

    @Override
    public SolrParams getParams() {
      SolrParams params = (SolrParams) values.get("params");
      return params == null ?  new MapSolrParams(new HashMap<>()) : params;
    }

    @Override
    public String getHttpHeader(String header) {
      return null;
    }

    @Override
    @SuppressWarnings({"rawtypes"})
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
