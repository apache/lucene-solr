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

import com.google.common.collect.ImmutableSet;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.annotation.SolrThreadSafe;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.JsonSchemaValidator;
import org.apache.solr.common.util.PathTrie;
import org.apache.solr.common.util.ValidatingJsonMap;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.PluginBag;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.servlet.HttpSolrCall;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.solr.servlet.SolrRequestParsers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.function.Supplier;

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.util.PathTrie.getPathSegments;
import static org.apache.solr.servlet.SolrDispatchFilter.Action.*;

// class that handle the '/v2' path
@SolrThreadSafe
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
    queryParams = SolrRequestParsers.parseQueryString(req.getQueryString());
    String path = this.path;
    final String fullPath = path = path.substring(7);//strip off '/____v2'
    try {
      pieces = getPathSegments(path);
      if (pieces.size() == 0 || (pieces.size() == 1 && path.endsWith(CommonParams.INTROSPECT))) {
        api = new Api(null) {
          @Override
          public void call(SolrQueryRequest req, SolrQueryResponse rsp) {
            rsp.add("documentation", "https://lucene.apache.org/solr/guide/v2-api.html");
            rsp.add("description", "V2 API root path");
          }
        };
        initAdminRequest(path);
        return;
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
        origCorename = pieces.get(1);

        DocCollection collection = resolveDocCollection(queryParams.get(COLLECTION_PROP, origCorename));

        if (collection == null) {
          if ( ! path.endsWith(CommonParams.INTROSPECT)) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "no such collection or alias");
          }
        } else {
          boolean isPreferLeader = (path.endsWith("/update") || path.contains("/update/"));
          core = getCoreByCollection(collection.getName(), isPreferLeader);
          if (core == null) {
            //this collection exists , but this node does not have a replica for that collection
            extractRemotePath(collection.getName(), collection.getName());
            if (action == REMOTEQUERY) {
              coreUrl = coreUrl.replace("/solr/", "/solr/____v2/c/");
              this.path = path = path.substring(prefix.length() + collection.getName().length() + 2);
              return;
            }
          }
        }
      } else if ("cores".equals(prefix)) {
        origCorename = pieces.get(1);
        core = cores.getCore(origCorename);
      } else {
        api = getApiInfo(cores.getRequestHandlers(), path, req.getMethod(), fullPath, parts);
        if(api != null) {
          //custom plugin
          initAdminRequest(path);
          return;
        }
      }
      if (core == null) {
        log.error(">> path: '{}'", path);
        if (path.endsWith(CommonParams.INTROSPECT)) {
          initAdminRequest(path);
          return;
        } else {
          throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "no core retrieved for " + origCorename);
        }
      }

      this.path = path = path.substring(prefix.length() + pieces.get(1).length() + 2);
      Api apiInfo = getApiInfo(core.getRequestHandlers(), path, req.getMethod(), fullPath, parts);
      if (isCompositeApi && apiInfo instanceof CompositeApi) {
        ((CompositeApi) this.api).add(apiInfo);
      } else {
        api = apiInfo == null ? api : apiInfo;
      }
      parseRequest();

      addCollectionParamIfNeeded(getCollectionsList());

      action = PROCESS;
      // we are done with a valid handler
    } catch (RuntimeException rte) {
      log.error("Error in init()", rte);
      throw rte;
    } finally {
      if (action == null && api == null) action = PROCESS;
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

  /**
   * Lookup the collection from the collection string (maybe comma delimited).
   * Also sets {@link #collectionsList} by side-effect.
   * if {@code secondTry} is false then we'll potentially recursively try this all one more time while ensuring
   * the alias and collection info is sync'ed from ZK.
   */
  protected DocCollection resolveDocCollection(String collectionStr) {
    if (!cores.isZooKeeperAware()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Solr not running in cloud mode ");
    }
    ZkStateReader zkStateReader = cores.getZkController().getZkStateReader();

    Supplier<DocCollection> logic = () -> {
      this.collectionsList = resolveCollectionListOrAlias(collectionStr); // side-effect
      if (collectionsList.size() > 1) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Request must be sent to a single collection " +
            "or an alias that points to a single collection," +
            " but '" + collectionStr + "' resolves to " + this.collectionsList);
      }
      String collectionName = collectionsList.get(0); // first
      //TODO an option to choose another collection in the list if can't find a local replica of the first?

      return zkStateReader.getClusterState().getCollectionOrNull(collectionName);
    };

    DocCollection docCollection = logic.get();
    if (docCollection != null) {
      return docCollection;
    }
    // ensure our view is up to date before trying again
    try {
      zkStateReader.aliasesManager.update();
      zkStateReader.forceUpdateCollection(collectionsList.get(0));
    } catch (Exception e) {
      log.error("Error trying to update state while resolving collection.", e);
      //don't propagate exception on purpose
    }
    return logic.get();
  }

  public static Api getApiInfo(PluginBag<SolrRequestHandler> requestHandlers,
                               String path, String method,
                               String fullPath,
                               Map<String, String> parts) {
    fullPath = fullPath == null ? path : fullPath;
    Api api = requestHandlers.v2lookup(path, method, parts);
    if (api == null && path.endsWith(CommonParams.INTROSPECT)) {
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
          RequestHandlerUtils.addExperimentalFormatWarning(rsp);
        }
      });
      getSubPathApi(requestHandlers,path, fullPath, (CompositeApi) api);
    }


    return api;
  }

  @SuppressWarnings({"unchecked"})
  private static CompositeApi getSubPathApi(PluginBag<SolrRequestHandler> requestHandlers, String path, String fullPath, CompositeApi compositeApi) {

    String newPath = path.endsWith(CommonParams.INTROSPECT) ? path.substring(0, path.length() - CommonParams.INTROSPECT.length()) : path;
    Map<String, Set<String>> subpaths = new LinkedHashMap<>();

    getSubPaths(newPath, requestHandlers.getApiBag(), subpaths);
    final Map<String, Set<String>> subPaths = subpaths;
    if (subPaths.isEmpty()) return null;
    return compositeApi.add(new Api(() -> ValidatingJsonMap.EMPTY) {
      @Override
      public void call(SolrQueryRequest req1, SolrQueryResponse rsp) {
        String prefix = null;
        prefix = fullPath.endsWith(CommonParams.INTROSPECT) ?
            fullPath.substring(0, fullPath.length() - CommonParams.INTROSPECT.length()) :
            fullPath;
        LinkedHashMap<String, Set<String>> result = new LinkedHashMap<>(subPaths.size());
        for (Map.Entry<String, Set<String>> e : subPaths.entrySet()) {
          if (e.getKey().endsWith(CommonParams.INTROSPECT)) continue;
          result.put(prefix + e.getKey(), e.getValue());
        }

        @SuppressWarnings({"rawtypes"})
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
    try {
      api.call(this.solrReq, solrResp);
    } catch (Exception e) {
      solrResp.setException(e);
    }
  }

  @Override
  protected void execute(SolrQueryResponse rsp) {
    SolrCore.preDecorateResponse(solrReq, rsp);
    if (api == null) {
      rsp.setException(new SolrException(SolrException.ErrorCode.NOT_FOUND,
          "Cannot find correspond api for the path : " + solrReq.getContext().get(CommonParams.PATH)));
    } else {
      try {
        api.call(solrReq, rsp);
      } catch (Exception e) {
        rsp.setException(e);
      }
    }

    SolrCore.postDecorateResponse(handler, solrReq, rsp);
  }

  @Override
  protected Object _getHandler() {
    return api;
  }

  public Map<String,String> getUrlParts(){
    return parts;
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
