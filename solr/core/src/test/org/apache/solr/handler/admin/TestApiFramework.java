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

package org.apache.solr.handler.admin;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.api.ApiBag;
import org.apache.solr.api.V2HttpCall.CompositeApi;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.common.util.ValidatingJsonMap;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.PluginBag;
import org.apache.solr.handler.PingRequestHandler;
import org.apache.solr.handler.SchemaHandler;
import org.apache.solr.handler.SolrConfigHandler;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.api.Api;
import org.apache.solr.api.V2HttpCall;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.common.util.PathTrie;

import static org.apache.solr.api.ApiBag.EMPTY_SPEC;
import static org.apache.solr.client.solrj.SolrRequest.METHOD.GET;
import static org.apache.solr.common.params.CommonParams.COLLECTIONS_HANDLER_PATH;
import static org.apache.solr.common.params.CommonParams.CONFIGSETS_HANDLER_PATH;
import static org.apache.solr.common.params.CommonParams.CORES_HANDLER_PATH;
import static org.apache.solr.common.util.ValidatingJsonMap.NOT_NULL;

public class TestApiFramework extends SolrTestCaseJ4 {

  public void testFramework() {
    Map<String, Object[]> calls = new HashMap<>();
    Map<String, Object> out = new HashMap<>();
    CoreContainer mockCC = TestCoreAdminApis.getCoreContainerMock(calls, out);
    PluginBag<SolrRequestHandler> containerHandlers = new PluginBag<>(SolrRequestHandler.class, null, false);
    containerHandlers.put(COLLECTIONS_HANDLER_PATH, new TestCollectionAPIs.MockCollectionsHandler());
    containerHandlers.put(CORES_HANDLER_PATH, new CoreAdminHandler(mockCC));
    containerHandlers.put(CONFIGSETS_HANDLER_PATH, new ConfigSetsHandler(mockCC));
    out.put("getRequestHandlers", containerHandlers);

    PluginBag<SolrRequestHandler> coreHandlers = new PluginBag<>(SolrRequestHandler.class, null, false);
    coreHandlers.put("/schema", new SchemaHandler());
    coreHandlers.put("/config", new SolrConfigHandler());
    coreHandlers.put("/admin/ping", new PingRequestHandler());

    Map<String, String> parts = new HashMap<>();
    String fullPath = "/collections/hello/shards";
    Api api = V2HttpCall.getApiInfo(containerHandlers, fullPath, "POST",
       fullPath, parts);
    assertNotNull(api);
    assertConditions(api.getSpec(), Utils.makeMap(
        "/methods[0]", "POST",
        "/commands/create", NOT_NULL));
    assertEquals("hello", parts.get("collection"));


    parts = new HashMap<>();
    api = V2HttpCall.getApiInfo(containerHandlers, "/collections/hello/shards", "POST",
      null, parts);
    assertConditions(api.getSpec(), Utils.makeMap(
        "/methods[0]", "POST",
        "/commands/split", NOT_NULL,
        "/commands/add-replica", NOT_NULL
    ));


    parts = new HashMap<>();
    api = V2HttpCall.getApiInfo(containerHandlers, "/collections/hello/shards/shard1", "POST",
        null, parts);
    assertConditions(api.getSpec(), Utils.makeMap(
        "/methods[0]", "POST",
        "/commands/force-leader", NOT_NULL
    ));
    assertEquals("hello", parts.get("collection"));
    assertEquals("shard1", parts.get("shard"));


    parts = new HashMap<>();
    api = V2HttpCall.getApiInfo(containerHandlers, "/collections/hello", "POST",
       null, parts);
    assertConditions(api.getSpec(), Utils.makeMap(
        "/methods[0]", "POST",
        "/commands/add-replica-property", NOT_NULL,
        "/commands/delete-replica-property", NOT_NULL
    ));
    assertEquals("hello", parts.get("collection"));

    api = V2HttpCall.getApiInfo(containerHandlers, "/collections/hello/shards/shard1/replica1", "DELETE",
       null, parts);
    assertConditions(api.getSpec(), Utils.makeMap(
        "/methods[0]", "DELETE",
        "/url/params/onlyIfDown/type", "boolean"
    ));
    assertEquals("hello", parts.get("collection"));
    assertEquals("shard1", parts.get("shard"));
    assertEquals("replica1", parts.get("replica"));

    SolrQueryResponse rsp = invoke(containerHandlers, null, "/collections/_introspect", GET, mockCC);

    assertConditions(rsp.getValues().asMap(2), Utils.makeMap(
        "/spec[0]/methods[0]", "DELETE",
        "/spec[1]/methods[0]", "POST",
        "/spec[2]/methods[0]", "GET"

    ));

    rsp = invoke(coreHandlers, "/schema/_introspect", "/collections/hello/schema/_introspect", GET, mockCC);
    assertConditions(rsp.getValues().asMap(2), Utils.makeMap(
        "/spec[0]/methods[0]", "POST",
        "/spec[0]/commands", NOT_NULL,
        "/spec[1]/methods[0]", "GET"));

    rsp = invoke(coreHandlers, "/", "/collections/hello/_introspect", GET, mockCC);
    assertConditions(rsp.getValues().asMap(2), Utils.makeMap(
        "/availableSubPaths", NOT_NULL,
        "availableSubPaths /collections/hello/config/jmx", NOT_NULL,
        "availableSubPaths /collections/hello/schema", NOT_NULL,
        "availableSubPaths /collections/hello/shards", NOT_NULL,
        "availableSubPaths /collections/hello/shards/{shard}", NOT_NULL,
        "availableSubPaths /collections/hello/shards/{shard}/{replica}", NOT_NULL
    ));

  }
  public void testTrailingTemplatePaths(){
    PathTrie<Api> registry =  new PathTrie<>();
    Api api = new Api(EMPTY_SPEC) {
      @Override
      public void call(SolrQueryRequest req, SolrQueryResponse rsp) {

      }
    };
    Api intropsect = new ApiBag.IntrospectApi(api,false);
    ApiBag.registerIntrospect(Collections.emptyMap(),registry,"/c/.system/blob/{name}",intropsect);
    ApiBag.registerIntrospect(Collections.emptyMap(), registry, "/c/.system/{x}/{name}", intropsect);
    assertEquals(intropsect, registry.lookup("/c/.system/blob/random_string/_introspect", new HashMap<>()));
    assertEquals(intropsect, registry.lookup("/c/.system/blob/_introspect", new HashMap<>()));
    assertEquals(intropsect, registry.lookup("/c/.system/_introspect", new HashMap<>()));
    assertEquals(intropsect, registry.lookup("/c/.system/v1/_introspect", new HashMap<>()));
    assertEquals(intropsect, registry.lookup("/c/.system/v1/v2/_introspect", new HashMap<>()));
  }
  private SolrQueryResponse invoke(PluginBag<SolrRequestHandler> reqHandlers, String path,
                                   String fullPath, SolrRequest.METHOD method,
                                   CoreContainer mockCC) {
    HashMap<String, String> parts = new HashMap<>();
    boolean containerHandlerLookup = mockCC.getRequestHandlers() == reqHandlers;
    path = path == null ? fullPath : path;
    Api api = null;
    if (containerHandlerLookup) {
      api = V2HttpCall.getApiInfo(reqHandlers, path, "GET", fullPath, parts);
    } else {
      api = V2HttpCall.getApiInfo(mockCC.getRequestHandlers(), fullPath, "GET", fullPath, parts);
      if (api == null) api = new CompositeApi(null);
      if (api instanceof CompositeApi) {
        CompositeApi compositeApi = (CompositeApi) api;
        api = V2HttpCall.getApiInfo(reqHandlers, path, "GET", fullPath, parts);
        compositeApi.add(api);
        api = compositeApi;
      }
    }

    SolrQueryResponse rsp = new SolrQueryResponse();
    LocalSolrQueryRequest req = new LocalSolrQueryRequest(null, new MapSolrParams(new HashMap<>())){
      @Override
      public List<CommandOperation> getCommands(boolean validateInput) {
        return Collections.emptyList();
      }
    };

    api.call(req,rsp);
    return rsp;

  }


  private void assertConditions(Map root, Map conditions) {
    for (Object o : conditions.entrySet()) {
      Map.Entry e = (Map.Entry) o;
      String path = (String) e.getKey();
      List<String> parts = StrUtils.splitSmart(path, path.charAt(0) == '/' ?  '/':' ');
      if (parts.get(0).isEmpty()) parts.remove(0);
      Object val = Utils.getObjectByPath(root, false, parts);
      if (e.getValue() instanceof ValidatingJsonMap.PredicateWithErrMsg) {
        ValidatingJsonMap.PredicateWithErrMsg value = (ValidatingJsonMap.PredicateWithErrMsg) e.getValue();
        String err = value.test(val);
        if(err != null){
          assertEquals(err + " for " + e.getKey() + " in :" + Utils.toJSONString(root), e.getValue(), val);
        }

      } else {
        assertEquals("incorrect value for path " + e.getKey() + " in :" + Utils.toJSONString(root), e.getValue(), val);
      }
    }
  }
}
