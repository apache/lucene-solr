package org.apache.solr.handler.admin;

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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.ZkStateReader.ConfigData;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.Utils;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.BasicAuthPlugin;
import org.apache.solr.security.RuleBasedAuthorizationPlugin;
import org.apache.solr.util.CommandOperation;

import static org.apache.solr.common.util.Utils.makeMap;

public class SecurityConfHandlerTest extends SolrTestCaseJ4 {

  public void testEdit() throws Exception {
    MockSecurityHandler handler = new MockSecurityHandler();
    String command = "{\n" +
        "'set-user': {'tom':'TomIsCool'},\n" +
        "'set-user':{ 'tom':'TomIsUberCool'}\n" +
        "}";
    LocalSolrQueryRequest req = new LocalSolrQueryRequest(null, new ModifiableSolrParams());
    req.getContext().put("httpMethod", "POST");
    req.getContext().put("path", "/admin/authentication");
    ContentStreamBase.ByteArrayStream o = new ContentStreamBase.ByteArrayStream(command.getBytes(StandardCharsets.UTF_8), "");
    req.setContentStreams(Collections.<ContentStream>singletonList(o));
    handler.handleRequestBody(req, new SolrQueryResponse());

    BasicAuthPlugin basicAuth = new BasicAuthPlugin();
    ConfigData securityCfg = (ConfigData) handler.m.get("/security.json");
    basicAuth.init((Map<String, Object>) securityCfg.data.get("authentication"));
    assertTrue(basicAuth.authenticate("tom", "TomIsUberCool"));

    command = "{\n" +
        "'set-user': {'harry':'HarryIsCool'},\n" +
        "'delete-user': ['tom','harry']\n" +
        "}";
    o = new ContentStreamBase.ByteArrayStream(command.getBytes(StandardCharsets.UTF_8), "");
    req.setContentStreams(Collections.<ContentStream>singletonList(o));
    handler.handleRequestBody(req, new SolrQueryResponse());
    securityCfg = (ConfigData) handler.m.get("/security.json");
    assertEquals(3, securityCfg.version);
    Map result = (Map) securityCfg.data.get("authentication");
    result = (Map) result.get("credentials");
    assertTrue(result.isEmpty());


    command = "{'set-user-role': { 'tom': ['admin','dev']},\n" +
        "'set-permission':{'name': 'security-edit',\n" +
        "                  'role': 'admin'\n" +
        "                  },\n" +
        "'set-permission':{'name':'some-permission',\n" +
        "                      'collection':'acoll',\n" +
        "                      'path':'/nonexistentpath',\n" +
        "                      'role':'guest',\n" +
        "                      'before':'security-edit'\n" +
        "                      }\n" +
        "}";

    req = new LocalSolrQueryRequest(null, new ModifiableSolrParams());
    req.getContext().put("httpMethod", "POST");
    req.getContext().put("path", "/admin/authorization");
    o = new ContentStreamBase.ByteArrayStream(command.getBytes(StandardCharsets.UTF_8), "");
    req.setContentStreams(Collections.<ContentStream>singletonList(o));
    SolrQueryResponse rsp = new SolrQueryResponse();
    handler.handleRequestBody(req, rsp);
    assertNull(rsp.getValues().get(CommandOperation.ERR_MSGS));
    Map authzconf = (Map) ((ConfigData) handler.m.get("/security.json")).data.get("authorization");
    Map userRoles = (Map) authzconf.get("user-role");
    List tomRoles = (List) userRoles.get("tom");
    assertTrue(tomRoles.contains("admin"));
    assertTrue(tomRoles.contains("dev"));
    List<Map> permissions = (List<Map>) authzconf.get("permissions");
    assertEquals(2, permissions.size());
    for (Map p : permissions) {
      assertEquals("some-permission", p.get("name"));
      break;
    }


    command = "{\n" +
        "'set-permission':{'name': 'security-edit',\n" +
        "                  'role': ['admin','dev']\n" +
        "                  }}";
    req = new LocalSolrQueryRequest(null, new ModifiableSolrParams());
    req.getContext().put("httpMethod","POST");
    req.getContext().put("path","/admin/authorization");
    o = new ContentStreamBase.ByteArrayStream(command.getBytes(StandardCharsets.UTF_8),"");
    req.setContentStreams(Collections.<ContentStream>singletonList(o));
    rsp = new SolrQueryResponse();
    handler.handleRequestBody(req, rsp);
    authzconf = (Map) ((ConfigData) handler.m.get("/security.json")).data.get("authorization");
    permissions = (List<Map>) authzconf.get("permissions");

    Map p = permissions.get(1);
    assertEquals("security-edit", p.get("name"));
    List rol = (List) p.get("role");
    assertEquals( "admin", rol.get(0));
    assertEquals( "dev", rol.get(1));

    command = "{\n" +
        "'update-permission':{'name': 'some-permission',\n" +
        "                  'role': ['guest','admin']\n" +
        "                  }}";
    req = new LocalSolrQueryRequest(null, new ModifiableSolrParams());
    req.getContext().put("httpMethod","POST");
    req.getContext().put("path","/admin/authorization");
    o = new ContentStreamBase.ByteArrayStream(command.getBytes(StandardCharsets.UTF_8),"");
    req.setContentStreams(Collections.<ContentStream>singletonList(o));
    rsp = new SolrQueryResponse();
    handler.handleRequestBody(req, rsp);
    authzconf = (Map) ((ConfigData) handler.m.get("/security.json")).data.get("authorization");
    permissions = (List<Map>) authzconf.get("permissions");

    p = permissions.get(0);
    assertEquals("some-permission", p.get("name"));
    rol = (List) p.get("role");
    assertEquals( "guest", rol.get(0));
    assertEquals( "admin", rol.get(1));



    command = "{\n" +
        "'delete-permission': 'some-permission',\n" +
        "'set-user-role':{'tom':null}\n" +
        "}";
    req = new LocalSolrQueryRequest(null, new ModifiableSolrParams());
    req.getContext().put("httpMethod", "POST");
    req.getContext().put("path", "/admin/authorization");
    o = new ContentStreamBase.ByteArrayStream(command.getBytes(StandardCharsets.UTF_8), "");
    req.setContentStreams(Collections.<ContentStream>singletonList(o));
    rsp = new SolrQueryResponse();
    handler.handleRequestBody(req, rsp);
    assertNull(rsp.getValues().get(CommandOperation.ERR_MSGS));
    authzconf = (Map) ((ConfigData) handler.m.get("/security.json")).data.get("authorization");
    userRoles = (Map) authzconf.get("user-role");
    assertEquals(0, userRoles.size());
    permissions = (List<Map>) authzconf.get("permissions");
    assertEquals(1, permissions.size());

    for (Map permission : permissions) {
      assertFalse("some-permission".equals(permission.get("name")));
    }
    command = "{\n" +
        "'set-permission':{'name': 'security-edit',\n" +
        "                  'method':'POST',"+ // -ve test security edit is a well-known permission , only role attribute should be provided
        "                  'role': 'admin'\n" +
        "                  }}";
    req = new LocalSolrQueryRequest(null, new ModifiableSolrParams());
    req.getContext().put("httpMethod", "POST");
    req.getContext().put("path", "/admin/authorization");
    o = new ContentStreamBase.ByteArrayStream(command.getBytes(StandardCharsets.UTF_8), "");
    req.setContentStreams(Collections.<ContentStream>singletonList(o));
    rsp = new SolrQueryResponse();
    handler.handleRequestBody(req, rsp);
    List l = (List) ((Map) ((List) rsp.getValues().get("errorMessages")).get(0)).get("errorMessages");
    assertEquals(1, l.size());
  }


  public static class MockSecurityHandler extends SecurityConfHandler {
    private Map<String, Object> m;
    final BasicAuthPlugin basicAuthPlugin = new BasicAuthPlugin();
    final RuleBasedAuthorizationPlugin rulesBasedAuthorizationPlugin = new RuleBasedAuthorizationPlugin();


    public MockSecurityHandler() {
      super(null);
      m = new HashMap<>();
      ConfigData data = new ConfigData(makeMap("authentication", makeMap("class", "solr." + BasicAuthPlugin.class.getSimpleName())), 1);
      data.data.put("authorization", makeMap("class", "solr." + RuleBasedAuthorizationPlugin.class.getSimpleName()));
      m.put("/security.json", data);


      basicAuthPlugin.init(new HashMap<String, Object>());

      rulesBasedAuthorizationPlugin.init(new HashMap<String, Object>());
    }

    public Map<String, Object> getM() {
      return m;
    }

    @Override
    Object getPlugin(String key) {
      if (key.equals("authentication")) {
        return basicAuthPlugin;
      }
      if (key.equals("authorization")) {
        return rulesBasedAuthorizationPlugin;
      }
      return null;
    }

    @Override
    ConfigData getSecurityProps(boolean getFresh) {
      return (ConfigData) m.get("/security.json");
    }

    @Override
    boolean persistConf(String key, byte[] buf, int version) {
      Object data = m.get(key);
      if (data instanceof ConfigData) {
        ConfigData configData = (ConfigData) data;
        if (configData.version == version) {
          ConfigData result = new ConfigData((Map<String, Object>) Utils.fromJSON(buf), version + 1);
          m.put(key, result);
          return true;
        } else {
          return false;
        }
      }
      throw new RuntimeException();
    }


    public String getStandardJson() throws Exception {
      String command = "{\n" +
          "'set-user': {'solr':'SolrRocks'}\n" +
          "}";
      LocalSolrQueryRequest req = new LocalSolrQueryRequest(null, new ModifiableSolrParams());
      req.getContext().put("httpMethod", "POST");
      req.getContext().put("path", "/admin/authentication");
      ContentStreamBase.ByteArrayStream o = new ContentStreamBase.ByteArrayStream(command.getBytes(StandardCharsets.UTF_8), "");
      req.setContentStreams(Collections.<ContentStream>singletonList(o));
      handleRequestBody(req, new SolrQueryResponse());

      command = "{'set-user-role': { 'solr': 'admin'},\n" +
          "'set-permission':{'name': 'security-edit', 'role': 'admin'}" +
          "}";
      req = new LocalSolrQueryRequest(null, new ModifiableSolrParams());
      req.getContext().put("httpMethod", "POST");
      req.getContext().put("path", "/admin/authorization");
      o = new ContentStreamBase.ByteArrayStream(command.getBytes(StandardCharsets.UTF_8), "");
      req.setContentStreams(Collections.<ContentStream>singletonList(o));
      SolrQueryResponse rsp = new SolrQueryResponse();
      handleRequestBody(req, rsp);
      Map<String, Object> data = ((ConfigData) m.get("/security.json")).data;
      ((Map) data.get("authentication")).remove("");
      ((Map) data.get("authorization")).remove("");
      return Utils.toJSONString(data);
    }
  }


  public static void main(String[] args) throws Exception {
    System.out.println(new MockSecurityHandler().getStandardJson());
  }


}


