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

import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.api.Api;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.cloud.OverseerSolrResponse;
import org.apache.solr.cloud.OverseerSolrResponseSerializer;
import org.apache.solr.cloud.OverseerTaskQueue.QueueEvent;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.solr.common.cloud.ZkMaintenanceUtils;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.ConfigSetParams;
import org.apache.solr.common.params.ConfigSetParams.ConfigSetAction;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthenticationPlugin;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.Overseer.QUEUE_OPERATION;
import static org.apache.solr.cloud.OverseerConfigSetMessageHandler.BASE_CONFIGSET;
import static org.apache.solr.cloud.OverseerConfigSetMessageHandler.CONFIGSETS_ACTION_PREFIX;
import static org.apache.solr.cloud.OverseerConfigSetMessageHandler.PROPERTY_PREFIX;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.params.ConfigSetParams.ConfigSetAction.CREATE;
import static org.apache.solr.common.params.ConfigSetParams.ConfigSetAction.DELETE;
import static org.apache.solr.common.params.ConfigSetParams.ConfigSetAction.LIST;
import static org.apache.solr.handler.admin.ConfigSetsHandlerApi.DEFAULT_CONFIGSET_NAME;

/**
 * A {@link org.apache.solr.request.SolrRequestHandler} for ConfigSets API requests.
 */
public class ConfigSetsHandler extends RequestHandlerBase implements PermissionNameProvider {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  protected final CoreContainer coreContainer;
  public static long DEFAULT_ZK_TIMEOUT = 300 * 1000;
  private final ConfigSetsHandlerApi configSetsHandlerApi = new ConfigSetsHandlerApi(this);

  /**
   * Overloaded ctor to inject CoreContainer into the handler.
   *
   * @param coreContainer Core Container of the solr webapp installed.
   */
  public ConfigSetsHandler(final CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
  }


  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    if (coreContainer == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Core container instance missing");
    }

    // Make sure that the core is ZKAware
    if (!coreContainer.isZooKeeperAware()) {
      throw new SolrException(ErrorCode.BAD_REQUEST,
          "Solr instance is not running in SolrCloud mode.");
    }

    // Pick the action
    SolrParams params = req.getParams();
    String a = params.get(ConfigSetParams.ACTION);
    if (a != null) {
      ConfigSetAction action = ConfigSetAction.get(a);
      if (action == null)
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unknown action: " + a);
      if (action == ConfigSetAction.UPLOAD) {
        handleConfigUploadRequest(req, rsp);
        return;
      }
      invokeAction(req, rsp, action);
    } else {
      throw new SolrException(ErrorCode.BAD_REQUEST, "action is a required param");
    }

    rsp.setHttpCaching(false);
  }

  void invokeAction(SolrQueryRequest req, SolrQueryResponse rsp, ConfigSetAction action) throws Exception {
    ConfigSetOperation operation = ConfigSetOperation.get(action);
    if (log.isInfoEnabled()) {
      log.info("Invoked ConfigSet Action :{} with params {} ", action.toLower(), req.getParamString());
    }
    Map<String, Object> result = operation.call(req, rsp, this);
    sendToZk(rsp, operation, result);
  }

  protected void sendToZk(SolrQueryResponse rsp, ConfigSetOperation operation, Map<String, Object> result)
      throws KeeperException, InterruptedException {
    if (result != null) {
      // We need to differentiate between collection and configsets actions since they currently
      // use the same underlying queue.
      result.put(QUEUE_OPERATION, CONFIGSETS_ACTION_PREFIX + operation.action.toLower());
      ZkNodeProps props = new ZkNodeProps(result);
      handleResponse(operation.action.toLower(), props, rsp, DEFAULT_ZK_TIMEOUT);
    }
  }

  private void handleConfigUploadRequest(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    if (!"true".equals(System.getProperty("configset.upload.enabled", "true"))) {
      throw new SolrException(ErrorCode.BAD_REQUEST,
          "Configset upload feature is disabled. To enable this, start Solr with '-Dconfigset.upload.enabled=true'.");
    }

    String configSetName = req.getParams().get(NAME);
    if (StringUtils.isBlank(configSetName)) {
      throw new SolrException(ErrorCode.BAD_REQUEST,
          "The configuration name should be provided in the \"name\" parameter");
    }

    SolrZkClient zkClient = coreContainer.getZkController().getZkClient();
    String configPathInZk = ZkConfigManager.CONFIGS_ZKNODE + "/" + configSetName;

    boolean overwritesExisting = zkClient.exists(configPathInZk, true);

    if (overwritesExisting && !req.getParams().getBool(ConfigSetParams.OVERWRITE, false)) {
      throw new SolrException(ErrorCode.BAD_REQUEST,
          "The configuration " + configSetName + " already exists in zookeeper");
    }

    Iterator<ContentStream> contentStreamsIterator = req.getContentStreams().iterator();

    if (!contentStreamsIterator.hasNext()) {
      throw new SolrException(ErrorCode.BAD_REQUEST,
          "No stream found for the config data to be uploaded");
    }

    InputStream inputStream = contentStreamsIterator.next().getStream();

    // Create a node for the configuration in zookeeper
    boolean cleanup = req.getParams().getBool(ConfigSetParams.CLEANUP, false);
    
    Set<String> filesToDelete;
    if (overwritesExisting && cleanup) {
      filesToDelete = getAllConfigsetFiles(zkClient, configPathInZk);
    } else {
      filesToDelete = Collections.emptySet();
    }
    createBaseZnode(zkClient, overwritesExisting, isTrusted(req), cleanup, configPathInZk);

    ZipInputStream zis = new ZipInputStream(inputStream, StandardCharsets.UTF_8);
    ZipEntry zipEntry = null;
    while ((zipEntry = zis.getNextEntry()) != null) {
      String filePathInZk = configPathInZk + "/" + zipEntry.getName();
      if (filePathInZk.endsWith("/")) {
        filesToDelete.remove(filePathInZk.substring(0, filePathInZk.length() -1));
      } else {
        filesToDelete.remove(filePathInZk);
      }
      if (zipEntry.isDirectory()) {
        zkClient.makePath(filePathInZk, false,  true);
      } else {
        createZkNodeIfNotExistsAndSetData(zkClient, filePathInZk,
            IOUtils.toByteArray(zis));
      }
    }
    zis.close();
    deleteUnusedFiles(zkClient, filesToDelete);
  }

  private void createBaseZnode(SolrZkClient zkClient, boolean overwritesExisting, boolean requestIsTrusted, boolean cleanup, String configPathInZk) throws KeeperException, InterruptedException {
    byte[] baseZnodeData =  ("{\"trusted\": " + Boolean.toString(requestIsTrusted) + "}").getBytes(StandardCharsets.UTF_8);

    if (overwritesExisting) {
      if (cleanup && requestIsTrusted) {
        zkClient.setData(configPathInZk, baseZnodeData, true);
      } else if (!requestIsTrusted) {
        ensureOverwritingUntrustedConfigSet(zkClient, configPathInZk);
      }
    } else {
      zkClient.makePath(configPathInZk, baseZnodeData, true);
    }
  }

  private void deleteUnusedFiles(SolrZkClient zkClient, Set<String> filesToDelete) throws InterruptedException, KeeperException {
    if (!filesToDelete.isEmpty()) {
      if (log.isInfoEnabled()) {
        log.info("Cleaning up {} unused files", filesToDelete.size());
      }
      if (log.isDebugEnabled()) {
        log.debug("Cleaning up unused files: {}", filesToDelete);
      }
      for (String f:filesToDelete) {
        try {
          zkClient.delete(f, -1, true);
        } catch (KeeperException.NoNodeException nne) {
        }
      }
    }
  }

  private Set<String> getAllConfigsetFiles(SolrZkClient zkClient, String configPathInZk) throws KeeperException, InterruptedException {
    final Set<String> files = new HashSet<>();
    if (!configPathInZk.startsWith(ZkConfigManager.CONFIGS_ZKNODE + "/")) {
      throw new IllegalArgumentException("\"" + configPathInZk + "\" not recognized as a configset path");
    }
    ZkMaintenanceUtils.traverseZkTree(zkClient, configPathInZk, ZkMaintenanceUtils.VISIT_ORDER.VISIT_POST, files::add);
    files.remove(configPathInZk);
    return files;
  }

  /*
   * Fail if an untrusted request tries to update a trusted ConfigSet
   */
  private void ensureOverwritingUntrustedConfigSet(SolrZkClient zkClient, String configSetZkPath) {
    byte[] configSetNodeContent;
    try {
      configSetNodeContent = zkClient.getData(configSetZkPath, null, null, true);
    } catch (KeeperException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Exception while fetching current configSet at " + configSetZkPath, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SolrException(ErrorCode.SERVER_ERROR, "Interrupted while fetching current configSet at " + configSetZkPath, e);
    }
    @SuppressWarnings("unchecked")
    Map<Object, Object> contentMap = (Map<Object, Object>) Utils.fromJSON(configSetNodeContent);
    boolean isCurrentlyTrusted = (boolean) contentMap.getOrDefault("trusted", true);
    if (isCurrentlyTrusted) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Trying to make an unstrusted ConfigSet update on a trusted configSet");
    }
  }

  boolean isTrusted(SolrQueryRequest req) {
    AuthenticationPlugin authcPlugin = coreContainer.getAuthenticationPlugin();
    if (log.isInfoEnabled()) {
      log.info("Trying to upload a configset. authcPlugin: {}, user principal: {}",
          authcPlugin, req.getUserPrincipal());
    }
    if (authcPlugin != null && req.getUserPrincipal() != null) {
      return true;
    }
    return false;
  }

  private void createZkNodeIfNotExistsAndSetData(SolrZkClient zkClient,
                                                 String filePathInZk, byte[] data) throws Exception {
    if (!zkClient.exists(filePathInZk, true)) {
      zkClient.create(filePathInZk, data, CreateMode.PERSISTENT, true);
    } else {
      zkClient.setData(filePathInZk, data, true);
    }
  }

  @SuppressWarnings({"unchecked"})
  private void handleResponse(String operation, ZkNodeProps m,
                              SolrQueryResponse rsp, long timeout) throws KeeperException, InterruptedException {
    long time = System.nanoTime();

    QueueEvent event = coreContainer.getZkController()
        .getOverseerConfigSetQueue()
        .offer(Utils.toJSON(m), timeout);
    if (event.getBytes() != null) {
      SolrResponse response = OverseerSolrResponseSerializer.deserialize(event.getBytes());
      rsp.getValues().addAll(response.getResponse());
      @SuppressWarnings({"rawtypes"})
      SimpleOrderedMap exp = (SimpleOrderedMap) response.getResponse().get("exception");
      if (exp != null) {
        Integer code = (Integer) exp.get("rspCode");
        rsp.setException(new SolrException(code != null && code != -1 ? ErrorCode.getErrorCode(code) : ErrorCode.SERVER_ERROR, (String) exp.get("msg")));
      }
    } else {
      if (System.nanoTime() - time >= TimeUnit.NANOSECONDS.convert(timeout, TimeUnit.MILLISECONDS)) {
        throw new SolrException(ErrorCode.SERVER_ERROR, operation
            + " the configset time out:" + timeout / 1000 + "s");
      } else if (event.getWatchedEvent() != null) {
        throw new SolrException(ErrorCode.SERVER_ERROR, operation
            + " the configset error [Watcher fired on path: "
            + event.getWatchedEvent().getPath() + " state: "
            + event.getWatchedEvent().getState() + " type "
            + event.getWatchedEvent().getType() + "]");
      } else {
        throw new SolrException(ErrorCode.SERVER_ERROR, operation
            + " the configset unknown case");
      }
    }
  }

  private static Map<String, Object> copyPropertiesWithPrefix(SolrParams params, Map<String, Object> props, String prefix) {
    Iterator<String> iter = params.getParameterNamesIterator();
    while (iter.hasNext()) {
      String param = iter.next();
      if (param.startsWith(prefix)) {
        props.put(param, params.get(param));
      }
    }

    // The configset created via an API should be mutable.
    props.put("immutable", "false");

    return props;
  }

  @Override
  public String getDescription() {
    return "Manage SolrCloud ConfigSets";
  }

  @Override
  public Category getCategory() {
    return Category.ADMIN;
  }

  enum ConfigSetOperation {
    CREATE_OP(CREATE) {
      @Override
      Map<String, Object> call(SolrQueryRequest req, SolrQueryResponse rsp, ConfigSetsHandler h) throws Exception {
        String baseConfigSetName = req.getParams().get(BASE_CONFIGSET, DEFAULT_CONFIGSET_NAME);
        Map<String, Object> props = CollectionsHandler.copy(req.getParams().required(), null, NAME);
        props.put(BASE_CONFIGSET, baseConfigSetName);
        return copyPropertiesWithPrefix(req.getParams(), props, PROPERTY_PREFIX + ".");
      }
    },
    DELETE_OP(DELETE) {
      @Override
      Map<String, Object> call(SolrQueryRequest req, SolrQueryResponse rsp, ConfigSetsHandler h) throws Exception {
        return CollectionsHandler.copy(req.getParams().required(), null, NAME);
      }
    },
    @SuppressWarnings({"unchecked"})
    LIST_OP(LIST) {
      @Override
      Map<String, Object> call(SolrQueryRequest req, SolrQueryResponse rsp, ConfigSetsHandler h) throws Exception {
        NamedList<Object> results = new NamedList<>();
        SolrZkClient zk = h.coreContainer.getZkController().getZkStateReader().getZkClient();
        ZkConfigManager zkConfigManager = new ZkConfigManager(zk);
        List<String> configSetsList = zkConfigManager.listConfigs();
        results.add("configSets", configSetsList);
        SolrResponse response = new OverseerSolrResponse(results);
        rsp.getValues().addAll(response.getResponse());
        return null;
      }
    };

    ConfigSetAction action;

    ConfigSetOperation(ConfigSetAction action) {
      this.action = action;
    }

    abstract Map<String, Object> call(SolrQueryRequest req, SolrQueryResponse rsp, ConfigSetsHandler h) throws Exception;

    public static ConfigSetOperation get(ConfigSetAction action) {
      for (ConfigSetOperation op : values()) {
        if (op.action == action) return op;
      }
      throw new SolrException(ErrorCode.SERVER_ERROR, "No such action" + action);
    }
  }

  @Override
  public Collection<Api> getApis() {
    return configSetsHandlerApi.getApis();
  }

  @Override
  public Boolean registerV2() {
    return Boolean.TRUE;
  }

  @Override
  public Name getPermissionName(AuthorizationContext ctx) {
    String a = ctx.getParams().get(ConfigSetParams.ACTION);
    if (a != null) {
      ConfigSetAction action = ConfigSetAction.get(a);
      if (action == ConfigSetAction.CREATE || action == ConfigSetAction.DELETE || action == ConfigSetAction.UPLOAD) {
        return Name.CONFIG_EDIT_PERM;
      } else if (action == ConfigSetAction.LIST) {
        return Name.CONFIG_READ_PERM;
      }
    }
    return null;
  }
}
