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

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.api.ApiBag;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.CollectionApiMapping;
import org.apache.solr.client.solrj.request.CollectionApiMapping.CommandMeta;
import org.apache.solr.client.solrj.request.CollectionApiMapping.Meta;
import org.apache.solr.client.solrj.request.CollectionApiMapping.V2EndPoint;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.RuntimeLib;
import org.apache.solr.handler.admin.CollectionsHandler.CollectionOperation;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.solr.common.util.CommandOperation.captureErrors;

public class CollectionHandlerApi extends BaseHandlerApiSupport {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  final CollectionsHandler handler;
  static Collection<ApiCommand> apiCommands = createCollMapping();

  public CollectionHandlerApi(CollectionsHandler handler) {
    this.handler = handler;
  }

  private static Collection<ApiCommand> createCollMapping() {
    Map<Meta, ApiCommand> apiMapping = new EnumMap<>(Meta.class);

    for (Meta meta : Meta.values()) {
      for (CollectionOperation op : CollectionOperation.values()) {
        if (op.action == meta.action) {
          apiMapping.put(meta, new ApiCommand() {
            @Override
            public CommandMeta meta() {
              return meta;
            }

            @Override
            public void invoke(SolrQueryRequest req, SolrQueryResponse rsp, BaseHandlerApiSupport apiHandler) throws Exception {
              ((CollectionHandlerApi) apiHandler).handler.invokeAction(req, rsp, ((CollectionHandlerApi) apiHandler).handler.coreContainer, op.action, op);
            }
          });
        }
      }
    }
    //The following APIs have only V2 implementations
    addApi(apiMapping, Meta.GET_NODES, CollectionHandlerApi::getNodes);
    addApi(apiMapping, Meta.SET_CLUSTER_PROPERTY_OBJ, CollectionHandlerApi::setClusterObj);
    addApi(apiMapping, Meta.ADD_RUNTIME_LIB, CollectionHandlerApi::addUpdateRuntimeLib);
    addApi(apiMapping, Meta.UPDATE_RUNTIME_LIB, CollectionHandlerApi::addUpdateRuntimeLib);
    addApi(apiMapping, Meta.ADD_REQ_HANDLER, CollectionHandlerApi::addRequestHandler);
    addApi(apiMapping, Meta.DELETE_REQ_HANDLER, CollectionHandlerApi::deleteReqHandler);

    for (Meta meta : Meta.values()) {
      if (apiMapping.get(meta) == null) {
        log.error("ERROR_INIT. No corresponding API implementation for : " + meta.commandName);
      }
    }

    return apiMapping.values();
  }

  private static void getNodes(ApiInfo params) {
    params.rsp.add("nodes", ((CollectionHandlerApi) params.apiHandler).handler.coreContainer.getZkController().getClusterState().getLiveNodes());
  }

  private static void deleteReqHandler(ApiInfo params) throws Exception {
    String name = params.op.getStr("");
    ClusterProperties clusterProperties = new ClusterProperties(((CollectionHandlerApi) params.apiHandler).handler.coreContainer.getZkController().getZkClient());
    Map<String, Object> map = clusterProperties.getClusterProperties();
    if (Utils.getObjectByPath(map, false, asList(SolrRequestHandler.TYPE, name)) == null) {
      params.op.addError("NO such requestHandler with name :");
      return;
    }
    Map m = new LinkedHashMap();
    Utils.setObjectByPath(m, asList(SolrRequestHandler.TYPE, name), null, true);
    clusterProperties.setClusterProperties(m);
  }

  private static void addRequestHandler(ApiInfo params) throws Exception {
    Map data = params.op.getDataMap();
    String name = (String) data.get("name");
    ClusterProperties clusterProperties = new ClusterProperties(((CollectionHandlerApi) params.apiHandler).handler.coreContainer.getZkController().getZkClient());
    Map<String, Object> map = clusterProperties.getClusterProperties();
    if (Utils.getObjectByPath(map, false, asList(SolrRequestHandler.TYPE, name)) != null) {
      params.op.addError("A requestHandler already exists with the said name");
      return;
    }
    Map m = new LinkedHashMap();
    Utils.setObjectByPath(m, asList(SolrRequestHandler.TYPE, name), data, true);
    clusterProperties.setClusterProperties(m);
  }

  private static CommandOperation getFirstCommand(ApiInfo params) {
    List<CommandOperation> commands = params.req.getCommands(true);
    if (commands == null || commands.size() != 1)
      throw new SolrException(ErrorCode.BAD_REQUEST, "should have exactly one command");
    return commands.get(0);
  }

  private static void addUpdateRuntimeLib(ApiInfo params) throws Exception {
    CollectionHandlerApi handler = (CollectionHandlerApi) params.apiHandler;
    RuntimeLib lib = new RuntimeLib(handler.handler.coreContainer);
    CommandOperation op = params.op;
    String name = op.getStr("name");
    ClusterProperties clusterProperties = new ClusterProperties(((CollectionHandlerApi) params.apiHandler).handler.coreContainer.getZkController().getZkClient());
    Map<String, Object> props = clusterProperties.getClusterProperties();
    List<String> pathToLib = asList("runtimeLib", name);
    Map existing = (Map) Utils.getObjectByPath(props, false, pathToLib);
    if( Meta.ADD_RUNTIME_LIB.commandName.equals(op.name)){
      if(existing != null){
        op.addError(StrUtils.formatString("The jar with a name ''{0}'' already exists",name));
        return;
      }
    } else {
      if(existing == null){
        op.addError(StrUtils.formatString("The jar with a name ''{0}'' doesn not exist",name));
        return;
      }
    }
    try {
      lib.init(new PluginInfo(SolrRequestHandler.TYPE, op.getDataMap()));
    } catch (SolrException e) {
      log.error("Error loading runtimelib ", e);
      op.addError(e.getMessage());
      return;
    }

    Map delta = new LinkedHashMap();
    Utils.setObjectByPath(delta, pathToLib, op.getDataMap(), true);
    clusterProperties.setClusterProperties(delta);

  }

  private static void setClusterObj(ApiInfo params) {
    ClusterProperties clusterProperties = new ClusterProperties(((CollectionHandlerApi) params.apiHandler).handler.coreContainer.getZkController().getZkClient());
    try {
      clusterProperties.setClusterProperties(params.op.getDataMap());
    } catch (Exception e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Error in API", e);
    }
  }

  private static void addApi(Map<Meta, ApiCommand> mapping, Meta metaInfo, Command fun) {
    mapping.put(metaInfo, new ApiCommand() {

      @Override
      public CommandMeta meta() {
        return metaInfo;
      }

      @Override
      public void invoke(SolrQueryRequest req, SolrQueryResponse rsp, BaseHandlerApiSupport apiHandler) throws Exception {
        CommandOperation op = null;
        if(metaInfo.method == SolrRequest.METHOD.POST) {
          List<CommandOperation> commands = req.getCommands(true);
          if (commands == null || commands.size() != 1)
            throw new SolrException(ErrorCode.BAD_REQUEST, "should have exactly one command");
          op = commands.get(0);
        }

        fun.call(new ApiInfo(req, rsp, apiHandler, op));
        if(op!= null && op.hasError()){
         throw new ApiBag.ExceptionWithErrObject(ErrorCode.BAD_REQUEST,"error processing commands", captureErrors(singletonList(op)));
        }
      }
    });
  }

  @Override
  protected List<V2EndPoint> getEndPoints() {
    return asList(CollectionApiMapping.EndPoint.values());
  }

  @Override
  protected Collection<ApiCommand> getCommands() {
    return apiCommands;
  }

  interface Command {

    void call(ApiInfo info) throws Exception;

  }

  static class ApiInfo {
    final SolrQueryRequest req;
    final SolrQueryResponse rsp;
    final BaseHandlerApiSupport apiHandler;
    final CommandOperation op;

    ApiInfo(SolrQueryRequest req, SolrQueryResponse rsp, BaseHandlerApiSupport apiHandler, CommandOperation op) {
      this.req = req;
      this.rsp = rsp;
      this.apiHandler = apiHandler;
      this.op = op;
    }
  }

}
