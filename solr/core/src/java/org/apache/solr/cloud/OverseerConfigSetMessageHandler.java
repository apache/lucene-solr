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
package org.apache.solr.cloud;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ConfigSetParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.ConfigSetProperties;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.params.ConfigSetParams.ConfigSetAction.CREATE;
import static org.apache.solr.common.util.Utils.toJSONString;
import static org.apache.solr.handler.admin.ConfigSetsHandler.DEFAULT_CONFIGSET_NAME;

/**
 * A {@link OverseerMessageHandler} that handles ConfigSets API related
 * overseer messages.
 */
public class OverseerConfigSetMessageHandler implements OverseerMessageHandler {

  /**
   * Prefix to specify an action should be handled by this handler.
   */
  public static final String CONFIGSETS_ACTION_PREFIX = "configsets:";

  /**
   * Name of the ConfigSet to copy from for CREATE
   */
  public static final String BASE_CONFIGSET = "baseConfigSet";

  /**
   * Prefix for properties that should be applied to the ConfigSet for CREATE
   */
  public static final String PROPERTY_PREFIX = "configSetProp";

  private ZkStateReader zkStateReader;

  // we essentially implement a read/write lock for the ConfigSet exclusivity as follows:
  // WRITE: CREATE/DELETE on the ConfigSet under operation
  // READ: for the Base ConfigSet being copied in CREATE.
  // in this way, we prevent a Base ConfigSet from being deleted while it is being copied
  // but don't prevent different ConfigSets from being created with the same Base ConfigSet
  // at the same time.
  @SuppressWarnings({"rawtypes"})
  final private Set configSetWriteWip;
  @SuppressWarnings({"rawtypes"})
  final private Set configSetReadWip;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public OverseerConfigSetMessageHandler(ZkStateReader zkStateReader) {
    this.zkStateReader = zkStateReader;
    this.configSetWriteWip = new HashSet<>();
    this.configSetReadWip = new HashSet<>();
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public OverseerSolrResponse processMessage(ZkNodeProps message, String operation) {
    @SuppressWarnings({"rawtypes"})
    NamedList results = new NamedList();
    try {
      if (!operation.startsWith(CONFIGSETS_ACTION_PREFIX)) {
        throw new SolrException(ErrorCode.BAD_REQUEST,
            "Operation does not contain proper prefix: " + operation
                + " expected: " + CONFIGSETS_ACTION_PREFIX);
      }
      operation = operation.substring(CONFIGSETS_ACTION_PREFIX.length());
      log.info("OverseerConfigSetMessageHandler.processMessage : {}, {}", operation, message);

      ConfigSetParams.ConfigSetAction action = ConfigSetParams.ConfigSetAction.get(operation);
      if (action == null) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Unknown operation:" + operation);
      }
      switch (action) {
        case CREATE:
          createConfigSet(message);
          break;
        case DELETE:
          deleteConfigSet(message);
          break;
        default:
          throw new SolrException(ErrorCode.BAD_REQUEST, "Unknown operation:"
              + operation);
      }
    } catch (Exception e) {
      String configSetName = message.getStr(NAME);

      if (configSetName == null) {
        SolrException.log(log, "Operation " + operation + " failed", e);
      } else {
        SolrException.log(log, "ConfigSet: " + configSetName + " operation: " + operation
            + " failed", e);
      }

      results.add("Operation " + operation + " caused exception:", e);
      @SuppressWarnings({"rawtypes"})
      SimpleOrderedMap nl = new SimpleOrderedMap();
      nl.add("msg", e.getMessage());
      nl.add("rspCode", e instanceof SolrException ? ((SolrException) e).code() : -1);
      results.add("exception", nl);
    }
    return new OverseerSolrResponse(results);
  }

  @Override
  public String getName() {
    return "Overseer ConfigSet Message Handler";
  }

  @Override
  public String getTimerName(String operation) {
    return "configset_" + operation;
  }

  @Override
  public Lock lockTask(ZkNodeProps message, OverseerTaskProcessor.TaskBatch taskBatch) {
    String configSetName = getTaskKey(message);
    if (canExecute(configSetName, message)) {
      markExclusiveTask(configSetName, message);
      return () -> unmarkExclusiveTask(configSetName, message);
    }
    return null;
  }

  @Override
  public String getTaskKey(ZkNodeProps message) {
    return message.getStr(NAME);
  }


  private void markExclusiveTask(String configSetName, ZkNodeProps message) {
    String baseConfigSet = getBaseConfigSetIfCreate(message);
    markExclusive(configSetName, baseConfigSet);
  }

  @SuppressWarnings({"unchecked"})
  private void markExclusive(String configSetName, String baseConfigSetName) {
    synchronized (configSetWriteWip) {
      configSetWriteWip.add(configSetName);
      if (baseConfigSetName != null) configSetReadWip.add(baseConfigSetName);
    }
  }

  private void unmarkExclusiveTask(String configSetName, ZkNodeProps message) {
    String baseConfigSet = getBaseConfigSetIfCreate(message);
    unmarkExclusiveConfigSet(configSetName, baseConfigSet);
  }

  private void unmarkExclusiveConfigSet(String configSetName, String baseConfigSetName) {
    synchronized (configSetWriteWip) {
      configSetWriteWip.remove(configSetName);
      if (baseConfigSetName != null) configSetReadWip.remove(baseConfigSetName);
    }
  }


  private boolean canExecute(String configSetName, ZkNodeProps message) {
    String baseConfigSetName = getBaseConfigSetIfCreate(message);

    synchronized (configSetWriteWip) {
      // need to acquire:
      // 1) write lock on ConfigSet
      // 2) read lock on Base ConfigSet
      if (configSetWriteWip.contains(configSetName) || configSetReadWip.contains(configSetName)) {
        return false;
      }
      if (baseConfigSetName != null && configSetWriteWip.contains(baseConfigSetName)) {
        return false;
      }
    }

    return true;
  }


  private String getBaseConfigSetIfCreate(ZkNodeProps message) {
    String operation = message.getStr(Overseer.QUEUE_OPERATION);
    if (operation != null) {
      operation = operation.substring(CONFIGSETS_ACTION_PREFIX.length());
      ConfigSetParams.ConfigSetAction action = ConfigSetParams.ConfigSetAction.get(operation);
      if (action == CREATE) {
        String baseConfigSetName = message.getStr(BASE_CONFIGSET);
        if (baseConfigSetName == null || baseConfigSetName.length() == 0) {
          baseConfigSetName = DEFAULT_CONFIGSET_NAME;
        }
        return baseConfigSetName;
      }
    }
    return null;
  }

  @SuppressWarnings({"rawtypes"})
  private NamedList getConfigSetProperties(String path) throws IOException {
    byte[] oldPropsData = null;
    try {
      oldPropsData = zkStateReader.getZkClient().getData(path, null, null, true);
    } catch (KeeperException.NoNodeException e) {
      log.info("no existing ConfigSet properties found");
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Error reading old properties",
          SolrZkClient.checkInterrupted(e));
    }

    if (oldPropsData != null) {
      InputStreamReader reader = new InputStreamReader(new ByteArrayInputStream(oldPropsData), StandardCharsets.UTF_8);
      try {
        return ConfigSetProperties.readFromInputStream(reader);
      } finally {
        reader.close();
      }
    }
    return null;
  }

  private Map<String, Object> getNewProperties(ZkNodeProps message) {
    Map<String, Object> properties = null;
    for (Map.Entry<String, Object> entry : message.getProperties().entrySet()) {
      if (entry.getKey().startsWith(PROPERTY_PREFIX + ".")) {
        if (properties == null) {
          properties = new HashMap<String, Object>();
        }
        properties.put(entry.getKey().substring((PROPERTY_PREFIX + ".").length()),
            entry.getValue());
      }
    }
    return properties;
  }

  private void mergeOldProperties(Map<String, Object> newProps, @SuppressWarnings({"rawtypes"})NamedList oldProps) {
    @SuppressWarnings({"unchecked"})
    Iterator<Map.Entry<String, Object>> it = oldProps.iterator();
    while (it.hasNext()) {
      Map.Entry<String, Object> oldEntry = it.next();
      if (!newProps.containsKey(oldEntry.getKey())) {
        newProps.put(oldEntry.getKey(), oldEntry.getValue());
      }
    }
  }

  private byte[] getPropertyData(Map<String, Object> newProps) {
    if (newProps != null) {
      String propertyDataStr = toJSONString(newProps);
      if (propertyDataStr == null) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Invalid property specification");
      }
      return propertyDataStr.getBytes(StandardCharsets.UTF_8);
    }
    return null;
  }

  private String getPropertyPath(String configName, String propertyPath) {
    return ZkConfigManager.CONFIGS_ZKNODE + "/" + configName + "/" + propertyPath;
  }

  private void createConfigSet(ZkNodeProps message) throws IOException {
    String configSetName = getTaskKey(message);
    if (configSetName == null || configSetName.length() == 0) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "ConfigSet name not specified");
    }

    String baseConfigSetName = message.getStr(BASE_CONFIGSET, DEFAULT_CONFIGSET_NAME);

    ZkConfigManager configManager = new ZkConfigManager(zkStateReader.getZkClient());
    if (configManager.configExists(configSetName)) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "ConfigSet already exists: " + configSetName);
    }

    // is there a base config that already exists
    if (!configManager.configExists(baseConfigSetName)) {
      throw new SolrException(ErrorCode.BAD_REQUEST,
          "Base ConfigSet does not exist: " + baseConfigSetName);
    }

    String propertyPath = ConfigSetProperties.DEFAULT_FILENAME;
    Map<String, Object> props = getNewProperties(message);
    if (props != null) {
      // read the old config properties and do a merge, if necessary
      @SuppressWarnings({"rawtypes"})
      NamedList oldProps = getConfigSetProperties(getPropertyPath(baseConfigSetName, propertyPath));
      if (oldProps != null) {
        mergeOldProperties(props, oldProps);
      }
    }
    byte[] propertyData = getPropertyData(props);

    Set<String> copiedToZkPaths = new HashSet<String>();
    try {
      configManager.copyConfigDir(baseConfigSetName, configSetName, copiedToZkPaths);
      if (propertyData != null) {
        try {
          zkStateReader.getZkClient().makePath(
              getPropertyPath(configSetName, propertyPath),
              propertyData, CreateMode.PERSISTENT, null, false, true);
        } catch (KeeperException | InterruptedException e) {
          throw new IOException("Error writing new properties",
              SolrZkClient.checkInterrupted(e));
        }
      }
    } catch (Exception e) {
      // copying the config dir or writing the properties file may have failed.
      // we should delete the ConfigSet because it may be invalid,
      // assuming we actually wrote something.  E.g. could be
      // the entire baseConfig set with the old properties, including immutable,
      // that would make it impossible for the user to delete.
      try {
        if (configManager.configExists(configSetName) && copiedToZkPaths.size() > 0) {
          deleteConfigSet(configSetName, true);
        }
      } catch (IOException ioe) {
        log.error("Error while trying to delete partially created ConfigSet", ioe);
      }
      throw e;
    }
  }

  private void deleteConfigSet(ZkNodeProps message) throws IOException {
    String configSetName = getTaskKey(message);
    if (configSetName == null || configSetName.length() == 0) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "ConfigSet name not specified");
    }

    deleteConfigSet(configSetName, false);
  }

  private void deleteConfigSet(String configSetName, boolean force) throws IOException {
    ZkConfigManager configManager = new ZkConfigManager(zkStateReader.getZkClient());
    if (!configManager.configExists(configSetName)) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "ConfigSet does not exist to delete: " + configSetName);
    }

    for (Map.Entry<String, DocCollection> entry : zkStateReader.getClusterState().getCollectionsMap().entrySet()) {
      String configName = null;
      try {
        configName = zkStateReader.readConfigName(entry.getKey());
      } catch (KeeperException ex) {
        throw new SolrException(ErrorCode.BAD_REQUEST,
            "Can not delete ConfigSet as it is currently being used by collection [" + entry.getKey() + "]");
      }
      if (configSetName.equals(configName))
        throw new SolrException(ErrorCode.BAD_REQUEST,
            "Can not delete ConfigSet as it is currently being used by collection [" + entry.getKey() + "]");
    }

    String propertyPath = ConfigSetProperties.DEFAULT_FILENAME;
    @SuppressWarnings({"rawtypes"})
    NamedList properties = getConfigSetProperties(getPropertyPath(configSetName, propertyPath));
    if (properties != null) {
      Object immutable = properties.get(ConfigSetProperties.IMMUTABLE_CONFIGSET_ARG);
      boolean isImmutableConfigSet = immutable != null ? Boolean.parseBoolean(immutable.toString()) : false;
      if (!force && isImmutableConfigSet) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Requested delete of immutable ConfigSet: " + configSetName);
      }
    }
    configManager.deleteConfigDir(configSetName);
  }
}
