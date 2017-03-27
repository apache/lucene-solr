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
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.ValidatingJsonMap;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.PluginBag;
import org.apache.solr.core.SolrCore;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.servlet.HttpSolrCall;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.solr.servlet.SolrRequestParsers;
import org.apache.solr.util.JsonSchemaValidator;
import org.apache.solr.util.PathTrie;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.JSON;
import static org.apache.solr.common.params.CommonParams.WT;
import static org.apache.solr.servlet.SolrDispatchFilter.Action.ADMIN;
import static org.apache.solr.servlet.SolrDispatchFilter.Action.PASSTHROUGH;
import static org.apache.solr.servlet.SolrDispatchFilter.Action.PROCESS;
import static org.apache.solr.util.PathTrie.getPathSegments;

// class that handle the '/v2' path
public class V2HttpCall extends HttpSolrCall {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private Api api;
  List<String> pieces;
  private String prefix;
  HashMap<String, String> parts = new HashMap<>();
  static final Set<String> knownPrefixes = ImmutableSet.of("cluster", "node", "collections", "cores", "c");

  public V2HttpCall(SolrDispatchFilter solrDispatchFilter, CoreContainer cc,
                    HttpServletRequest request, HttpServletResponse response, boolean retry) {
    super(solrDispatchFilter, cc, request, response, retry);
  }

  protected void init() throws Exception {
    String path = this.path;
    String fullPath = path = path.substring(7);//strip off '/____v2'
    try {
      pieces = getPathSegments(path);
      if (pieces.size() == 0) {
        prefix = "c";
        path = "/c";
      } else {
        prefix = pieces.get(0);
      }

      boolean isCompositeApi = false;
      if (knownPrefixes.contains(prefix)) {
        api = getApiInfo(cores.getRequestHandlers(), path, req.getMethod(), fullPath, parts);
        if (api != null) {
          isCompositeApi = api instanceof CompositeApi;
          if (!isCompositeApi) {
            initAdminRequest(path);
            return;
          }
        }
      }

      if ("c".equals(prefix) || "collections".equals(prefix)) {
        String collectionName = origCorename = corename = pieces.get(1);
        DocCollection collection = getDocCollection(collectionName);
        if (collection == null) {
           if ( ! path.endsWith(ApiBag.INTROSPECT)) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "no such collection or alias");
          }
        } else {
          boolean isPreferLeader = false;
          if (path.endsWith("/update") || path.contains("/update/")) {
            isPreferLeader = true;
          }
          core = getCoreByCollection(collection.getName(), isPreferLeader);
          if (core == null) {
            //this collection exists , but this node does not have a replica for that collection
            //todo find a better way to compute remote
            extractRemotePath(corename, origCorename, 0);
            return;
          }
        }
      } else if ("cores".equals(prefix)) {
        origCorename = corename = pieces.get(1);
        core = cores.getCore(corename);
      }
      if (core == null) {
        log.error(">> path: '" + path + "'");
        if (path.endsWith(ApiBag.INTROSPECT)) {
          initAdminRequest(path);
          return;
        } else {
          throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "no core retrieved for " + corename);
        }
      }

      this.path = path = path.substring(prefix.length() + pieces.get(1).length() + 2);
      Api apiInfo = getApiInfo(core.getRequestHandlers(), path, req.getMethod(), fullPath, parts);
      if (isCompositeApi && apiInfo instanceof CompositeApi) {
        ((CompositeApi) this.api).add(apiInfo);
      } else {
        api = apiInfo == null ? api : apiInfo;
      }
      MDCLoggingContext.setCore(core);
      parseRequest();

      if (usingAliases) {
        processAliases(aliases, collectionsList);
      }

      action = PROCESS;
      // we are done with a valid handler
    } catch (RuntimeException rte) {
      log.error("Error in init()", rte);
      throw rte;
    } finally {
      if (api == null) action = PASSTHROUGH;
      if (solrReq != null) solrReq.getContext().put(CommonParams.PATH, path);
    }
  }

  private void initAdminRequest(String path) throws Exception {
    solrReq = SolrRequestParsers.DEFAULT.parse(null, path, req);
    solrReq.getContext().put(CoreContainer.class.getName(), cores);
    requestType = AuthorizationContext.RequestType.ADMIN;
    action = ADMIN;
  }

  protected void parseRequest() throws Exception {
    config = core.getSolrConfig();
    // get or create/cache the parser for the core
    SolrRequestParsers parser = config.getRequestParsers();

    // With a valid handler and a valid core...

    if (solrReq == null) solrReq = parser.parse(core, path, req);
  }

  protected DocCollection getDocCollection(String collectionName) {
    if (!cores.isZooKeeperAware()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Solr not running in cloud mode ");
    }
    ZkStateReader zkStateReader = cores.getZkController().getZkStateReader();
    DocCollection collection = zkStateReader.getClusterState().getCollectionOrNull(collectionName);
    if (collection == null) {
      collectionName = corename = lookupAliases(collectionName);
      collection = zkStateReader.getClusterState().getCollectionOrNull(collectionName);
    }
    return collection;
  }

  public static Api getApiInfo(PluginBag<SolrRequestHandler> requestHandlers,
                               String path, String method,
                               String fullPath,
                               Map<String, String> parts) {
    fullPath = fullPath == null ? path : fullPath;
    Api api = requestHandlers.v2lookup(path, method, parts);
    if (api == null && path.endsWith(ApiBag.INTROSPECT)) {
      // the particular http method does not have any ,
      // just try if any other method has this path
      api = requestHandlers.v2lookup(path, null, parts);
    }

    if (api == null) {
      return getSubPathApi(requestHandlers, path, fullPath, new CompositeApi(null));
    }

    if (api instanceof ApiBag.IntrospectApi) {
      final Map<String, Api> apis = new LinkedHashMap<>();
      for (String m : SolrRequest.SUPPORTED_METHODS) {
        Api x = requestHandlers.v2lookup(path, m, parts);
        if (x != null) apis.put(m, x);
      }
      api = new CompositeApi(new Api(ApiBag.EMPTY_SPEC) {
        @Override
        public void call(SolrQueryRequest req, SolrQueryResponse rsp) {
          String method = req.getParams().get("method");
          Set<Api> added = new HashSet<>();
          for (Map.Entry<String, Api> e : apis.entrySet()) {
            if (method == null || e.getKey().equals(method)) {
              if (!added.contains(e.getValue())) {
                e.getValue().call(req, rsp);
                added.add(e.getValue());
              }
            }
          }
        }
      });
      getSubPathApi(requestHandlers,path, fullPath, (CompositeApi) api);
    }


    return api;
  }

  private static CompositeApi getSubPathApi(PluginBag<SolrRequestHandler> requestHandlers, String path, String fullPath, CompositeApi compositeApi) {

    String newPath = path.endsWith(ApiBag.INTROSPECT) ? path.substring(0, path.length() - ApiBag.INTROSPECT.length()) : path;
    Map<String, Set<String>> subpaths = new LinkedHashMap<>();

    getSubPaths(newPath, requestHandlers.getApiBag(), subpaths);
    final Map<String, Set<String>> subPaths = subpaths;
    if (subPaths.isEmpty()) return null;
    return compositeApi.add(new Api(() -> ValidatingJsonMap.EMPTY) {
      @Override
      public void call(SolrQueryRequest req1, SolrQueryResponse rsp) {
        String prefix = null;
        prefix = fullPath.endsWith(ApiBag.INTROSPECT) ?
            fullPath.substring(0, fullPath.length() - ApiBag.INTROSPECT.length()) :
            fullPath;
        LinkedHashMap<String, Set<String>> result = new LinkedHashMap<>(subPaths.size());
        for (Map.Entry<String, Set<String>> e : subPaths.entrySet()) {
          if (e.getKey().endsWith(ApiBag.INTROSPECT)) continue;
          result.put(prefix + e.getKey(), e.getValue());
        }

        Map m = (Map) rsp.getValues().get("availableSubPaths");
        if(m != null){
          m.putAll(result);
        } else {
          rsp.add("availableSubPaths", result);
        }
      }
    });
  }

  private static void getSubPaths(String path, ApiBag bag, Map<String, Set<String>> pathsVsMethod) {
    for (SolrRequest.METHOD m : SolrRequest.METHOD.values()) {
      PathTrie<Api> registry = bag.getRegistry(m.toString());
      if (registry != null) {
        HashSet<String> subPaths = new HashSet<>();
        registry.lookup(path, new HashMap<>(), subPaths);
        for (String subPath : subPaths) {
          Set<String> supportedMethods = pathsVsMethod.get(subPath);
          if (supportedMethods == null) pathsVsMethod.put(subPath, supportedMethods = new HashSet<>());
          supportedMethods.add(m.toString());
        }
      }
    }
  }

  public static class CompositeApi extends Api {
    private LinkedList<Api> apis = new LinkedList<>();

    public CompositeApi(Api api) {
      super(ApiBag.EMPTY_SPEC);
      if (api != null) apis.add(api);
    }

    @Override
    public void call(SolrQueryRequest req, SolrQueryResponse rsp) {
      for (Api api : apis) {
        api.call(req, rsp);
      }

    }

    public CompositeApi add(Api api) {
      apis.add(api);
      return this;
    }
  }

  @Override
  protected void handleAdmin(SolrQueryResponse solrResp) {
    api.call(this.solrReq, solrResp);
  }

  @Override
  protected void execute(SolrQueryResponse rsp) {
    try {
      api.call(solrReq, rsp);
    } catch (RuntimeException e) {
      throw e;
    }
  }

  @Override
  protected Object _getHandler() {
    return api;
  }

  public Map<String,String> getUrlParts(){
    return parts;
  }

  @Override
  protected QueryResponseWriter getResponseWriter() {
    String wt = solrReq.getParams().get(WT, JSON);
    if (core != null) return core.getResponseWriters().get(wt);
    return SolrCore.DEFAULT_RESPONSE_WRITERS.get(wt);
  }

  @Override
  protected ValidatingJsonMap getSpec() {
    return api == null ? null : api.getSpec();
  }

  @Override
  protected Map<String, JsonSchemaValidator> getValidators() {
    return api == null ? null : api.getCommandSchema();
  }
}
