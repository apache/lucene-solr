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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.solr.api.ApiBag;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionApiMapping;
import org.apache.solr.client.solrj.request.CollectionApiMapping.CommandMeta;
import org.apache.solr.client.solrj.request.CollectionApiMapping.Meta;
import org.apache.solr.client.solrj.request.CollectionApiMapping.V2EndPoint;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.ConfigOverlay;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.RuntimeLib;
import org.apache.solr.handler.SolrConfigHandler;
import org.apache.solr.handler.admin.CollectionsHandler.CollectionOperation;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.RTimer;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.solr.common.util.CommandOperation.captureErrors;
import static org.apache.solr.common.util.StrUtils.formatString;
import static org.apache.solr.core.RuntimeLib.SHA256;

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
    addApi(apiMapping, Meta.ADD_PACKAGE, wrap(CollectionHandlerApi::addUpdatePackage));
    addApi(apiMapping, Meta.UPDATE_PACKAGE, wrap(CollectionHandlerApi::addUpdatePackage));
    addApi(apiMapping, Meta.DELETE_RUNTIME_LIB, wrap(CollectionHandlerApi::deletePackage));
    addApi(apiMapping, Meta.ADD_REQ_HANDLER, wrap(CollectionHandlerApi::addRequestHandler));
    addApi(apiMapping, Meta.DELETE_REQ_HANDLER, wrap(CollectionHandlerApi::deleteReqHandler));

    for (Meta meta : Meta.values()) {
      if (apiMapping.get(meta) == null) {
        log.error("ERROR_INIT. No corresponding API implementation for : " + meta.commandName);
      }
    }

    return apiMapping.values();
  }

  static Command wrap(Command cmd) {
    return info -> {
      CoreContainer cc = ((CollectionHandlerApi) info.apiHandler).handler.coreContainer;
      boolean modified = cmd.call(info);
      if (modified) {
        Stat stat = new Stat();
        Map<String, Object> clusterProperties = new ClusterProperties(cc.getZkController().getZkClient()).getClusterProperties(stat);
        try {
          cc.getPackageManager().onChange(clusterProperties);
        } catch (SolrException e) {
          log.error("error executing command : " + info.op.jsonStr(), e);
          throw e;
        } catch (Exception e) {
          log.error("error executing command : " + info.op.jsonStr(), e);
          throw new SolrException(ErrorCode.SERVER_ERROR, "error executing command : ", e);
        }
        log.info("current version of clusterprops.json is {} , trying to get every node to update ", stat.getVersion());
        log.debug("The current clusterprops.json:  {}", clusterProperties);
        ((CollectionHandlerApi) info.apiHandler).waitForStateSync(stat.getVersion(), cc);

      }
      if (info.op != null && info.op.hasError()) {
        log.error("Error in running command {} , current clusterprops.json : {}", Utils.toJSONString(info.op), Utils.toJSONString(new ClusterProperties(cc.getZkController().getZkClient()).getClusterProperties()));
      }
      return modified;

    };
  }

  private static boolean getNodes(ApiInfo params) {
    params.rsp.add("nodes", ((CollectionHandlerApi) params.apiHandler).handler.coreContainer.getZkController().getClusterState().getLiveNodes());
    return false;
  }

  private static boolean deleteReqHandler(ApiInfo params) throws Exception {
    String name = params.op.getStr("");
    ClusterProperties clusterProperties = new ClusterProperties(((CollectionHandlerApi) params.apiHandler).handler.coreContainer.getZkController().getZkClient());
    Map<String, Object> map = clusterProperties.getClusterProperties();
    if (Utils.getObjectByPath(map, false, asList(SolrRequestHandler.TYPE, name)) == null) {
      params.op.addError("NO such requestHandler with name :");
      return false;
    }
    Map m = new LinkedHashMap();
    Utils.setObjectByPath(m, asList(SolrRequestHandler.TYPE, name), null, true);
    clusterProperties.setClusterProperties(m);
    return true;
  }

  private static boolean addRequestHandler(ApiInfo params) throws Exception {
    Map data = params.op.getDataMap();
    String name = (String) data.get("name");
    CoreContainer coreContainer = ((CollectionHandlerApi) params.apiHandler).handler.coreContainer;
    ClusterProperties clusterProperties = new ClusterProperties(coreContainer.getZkController().getZkClient());
    Map<String, Object> map = clusterProperties.getClusterProperties();
    if (Utils.getObjectByPath(map, false, asList(SolrRequestHandler.TYPE, name)) != null) {
      params.op.addError("A requestHandler already exists with the said name");
      return false;
    }
    Map m = new LinkedHashMap();
    Utils.setObjectByPath(m, asList(SolrRequestHandler.TYPE, name), data, true);
    clusterProperties.setClusterProperties(m);
    return true;
  }

  private static boolean deletePackage(ApiInfo params) throws Exception {
    if (!RuntimeLib.isEnabled()) {
      params.op.addError("node not started with enable.runtime.lib=true");
      return false;
    }
    String name = params.op.getStr(CommandOperation.ROOT_OBJ);
    ClusterProperties clusterProperties = new ClusterProperties(((CollectionHandlerApi) params.apiHandler).handler.coreContainer.getZkController().getZkClient());
    Map<String, Object> props = clusterProperties.getClusterProperties();
    List<String> pathToLib = asList(CommonParams.PACKAGE, name);
    Map existing = (Map) Utils.getObjectByPath(props, false, pathToLib);
    if (existing == null) {
      params.op.addError("No such runtimeLib : " + name);
      return false;
    }
    Map delta = new LinkedHashMap();
    Utils.setObjectByPath(delta, pathToLib, null, true);
    clusterProperties.setClusterProperties(delta);
    return true;
  }

  private static boolean addUpdatePackage(ApiInfo params) throws Exception {
    if (!RuntimeLib.isEnabled()) {
      params.op.addError("node not started with enable.runtime.lib=true");
      return false;
    }

    CollectionHandlerApi handler = (CollectionHandlerApi) params.apiHandler;
    RuntimeLib lib = new RuntimeLib(handler.handler.coreContainer);
    CommandOperation op = params.op;
    String name = op.getStr("name");
    ClusterProperties clusterProperties = new ClusterProperties(((CollectionHandlerApi) params.apiHandler).handler.coreContainer.getZkController().getZkClient());
    Map<String, Object> props = clusterProperties.getClusterProperties();
    List<String> pathToLib = asList(CommonParams.PACKAGE, name);
    Map existing = (Map) Utils.getObjectByPath(props, false, pathToLib);
    if (Meta.ADD_PACKAGE.commandName.equals(op.name)) {
      if (existing != null) {
        op.addError(StrUtils.formatString("The jar with a name ''{0}'' already exists ", name));
        return false;
      }
    } else {
      if (existing == null) {
        op.addError(StrUtils.formatString("The jar with a name ''{0}'' does not exist", name));
        return false;
      }
      if (Objects.equals(existing.get(SHA256), op.getDataMap().get(SHA256))) {
        op.addError("Trying to update a jar with the same sha256");
        return false;
      }
    }
    try {
      lib.init(new PluginInfo(SolrRequestHandler.TYPE, op.getDataMap()));
    } catch (SolrException e) {
      log.error("Error loading runtimelib ", e);
      op.addError(e.getMessage());
      return false;
    }

    Map delta = new LinkedHashMap();
    Utils.setObjectByPath(delta, pathToLib, op.getDataMap(), true);
    clusterProperties.setClusterProperties(delta);
    return true;

  }

  private static boolean setClusterObj(ApiInfo params) {
    ClusterProperties clusterProperties = new ClusterProperties(((CollectionHandlerApi) params.apiHandler).handler.coreContainer.getZkController().getZkClient());
    try {
      clusterProperties.setClusterProperties(params.op.getDataMap());
    } catch (Exception e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Error in API", e);
    }
    return false;
  }

  private void waitForStateSync(int expectedVersion, CoreContainer coreContainer) {
    final RTimer timer = new RTimer();
    int waitTimeSecs = 30;
    // get a list of active replica cores to query for the schema zk version (skipping this core of course)
    List<PerNodeCallable> concurrentTasks = new ArrayList<>();

    ZkStateReader zkStateReader = coreContainer.getZkController().getZkStateReader();
    for (String nodeName : zkStateReader.getClusterState().getLiveNodes()) {
      PerNodeCallable e = new PerNodeCallable(zkStateReader.getBaseUrlForNodeName(nodeName), expectedVersion, waitTimeSecs);
      concurrentTasks.add(e);
    }
    if (concurrentTasks.isEmpty()) return; // nothing to wait for ...

    log.info("Waiting up to {} secs for {} nodes to update clusterprops to be of version {} ",
        waitTimeSecs, concurrentTasks.size(), expectedVersion);
    SolrConfigHandler.execInparallel(concurrentTasks, parallelExecutor -> {
      try {
        List<String> failedList = SolrConfigHandler.executeAll(expectedVersion, waitTimeSecs, concurrentTasks, parallelExecutor);

        // if any tasks haven't completed within the specified timeout, it's an error
        if (failedList != null)
          throw new SolrException(ErrorCode.SERVER_ERROR,
              formatString("{0} out of {1} the property {2} to be of version {3} within {4} seconds! Failed cores: {5}",
                  failedList.size(), concurrentTasks.size() + 1, expectedVersion, 30, failedList));
      } catch (InterruptedException e) {
        log.warn(formatString(
            "Request was interrupted . trying to set the clusterprops to version {0} to propagate to {1} nodes ",
            expectedVersion, concurrentTasks.size()));
        Thread.currentThread().interrupt();

      }
    });

    log.info("Took {}ms to update the clusterprops to be of version {}  on {} nodes",
        timer.getTime(), expectedVersion, concurrentTasks.size());

  }

  interface Command {


    boolean call(ApiInfo info) throws Exception;

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
        if (metaInfo.method == SolrRequest.METHOD.POST) {
          List<CommandOperation> commands = req.getCommands(true);
          if (commands == null || commands.size() != 1)
            throw new SolrException(ErrorCode.BAD_REQUEST, "should have exactly one command");
          op = commands.get(0);
        }

        fun.call(new ApiInfo(req, rsp, apiHandler, op));
        if (op != null && op.hasError()) {
          throw new ApiBag.ExceptionWithErrObject(ErrorCode.BAD_REQUEST, "error processing commands", captureErrors(singletonList(op)));
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

  public static class PerNodeCallable extends SolrConfigHandler.PerReplicaCallable {

    static final List<String> path = Arrays.asList("metadata", CommonParams.VERSION);

    PerNodeCallable(String baseUrl, int expectedversion, int waitTime) {
      super(baseUrl, ConfigOverlay.ZNODEVER, expectedversion, waitTime);
    }

    @Override
    protected boolean verifyResponse(MapWriter mw, int attempts) {
      remoteVersion = (Number) mw._get(path, -1);
      if (remoteVersion.intValue() >= expectedZkVersion) return true;
      log.info(formatString("Could not get expectedVersion {0} from {1} , remote val= {2}   after {3} attempts", expectedZkVersion, coreUrl, remoteVersion, attempts));

      return false;
    }

    public String getPath() {
      return "/____v2/node/ext";
    }
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

  public static void postBlob(String baseUrl, ByteBuffer buf) throws IOException {
    try(HttpSolrClient client = new HttpSolrClient.Builder(baseUrl+"/____v2/node/blob" ).build()){

    }
  }

}
