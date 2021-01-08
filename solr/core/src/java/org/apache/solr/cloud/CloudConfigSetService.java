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

import java.lang.invoke.MethodHandles;
import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.cloud.api.collections.CreateCollectionCmd;
import org.apache.solr.common.ConfigNode;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.ConfigSetProperties;
import org.apache.solr.core.ConfigSetService;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SolrCloud ConfigSetService impl.
 */
public class CloudConfigSetService extends ConfigSetService {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private Map<String, ConfigCacheEntry> cache = new ConcurrentHashMap<>();
  private final ZkController zkController;

  public CloudConfigSetService(SolrResourceLoader loader, boolean shareSchema, ZkController zkController) {
    super(loader, shareSchema);
    this.zkController = zkController;
  }

  public void storeConfig(String resource, ConfigNode config, int znodeVersion) {
    cache.put(resource, new ConfigCacheEntry(config, znodeVersion));
  }

  public ConfigNode getConfig(String resource, int znodeVersion) {
    ConfigCacheEntry e = cache.get(resource);
    if (e == null) return null;
    ConfigNode configNode = e.configNode.get();
    if (configNode == null) cache.remove(resource);
    if (e.znodeVersion == znodeVersion) return configNode;
    if (e.znodeVersion < znodeVersion) cache.remove(resource);
    return null;
  }

  @Override
  public SolrResourceLoader createCoreResourceLoader(CoreDescriptor cd) {
    final String colName = cd.getCollectionName();

    // For back compat with cores that can create collections without the collections API
    try {
      if (!zkController.getZkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + colName, true)) {
        // TODO remove this functionality or maybe move to a CLI mechanism
        log.warn("Auto-creating collection (in ZK) from core descriptor (on disk).  This feature may go away!");
        CreateCollectionCmd.createCollectionZkNode(zkController.getSolrCloudManager().getDistribStateManager(), colName, cd.getCloudDescriptor().getParams());
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "Interrupted auto-creating collection", e);
    } catch (KeeperException e) {
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "Failure auto-creating collection", e);
    }

    // The configSet is read from ZK and populated.  Ignore CD's pre-existing configSet; only populated in standalone
    final String configSetName;
    try {
      configSetName = zkController.getZkStateReader().readConfigName(colName);
      cd.setConfigSet(configSetName);
    } catch (KeeperException ex) {
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "Trouble resolving configSet for collection " + colName + ": " + ex.getMessage());
    }

    return new ZkSolrResourceLoader(cd.getInstanceDir(), configSetName, parentLoader.getClassLoader(),
        cd.getSubstitutableProperties(), zkController);
  }

  @Override
  @SuppressWarnings({"rawtypes"})
  protected NamedList loadConfigSetFlags(CoreDescriptor cd, SolrResourceLoader loader) {
    try {
      return ConfigSetProperties.readFromResourceLoader(loader, ".");
    } catch (Exception ex) {
      log.debug("No configSet flags", ex);
      return null;
    }
  }

  @Override
  protected Long getCurrentSchemaModificationVersion(String configSet, SolrConfig solrConfig, String schemaFile) {
    String zkPath = ZkConfigManager.CONFIGS_ZKNODE + "/" + configSet + "/" + schemaFile;
    Stat stat;
    try {
      stat = zkController.getZkClient().exists(zkPath, null, true);
    } catch (KeeperException e) {
      log.warn("Unexpected exception when getting modification time of {}", zkPath, e);
      return null; // debatable; we'll see an error soon if there's a real problem
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
    if (stat == null) { // not found
      return null;
    }
    return (long) stat.getVersion();
  }

  @Override
  public String configSetName(CoreDescriptor cd) {
    return "configset " + cd.getConfigSet();
  }

  private static class ConfigCacheEntry {
    final WeakReference<ConfigNode> configNode;
    final int znodeVersion;

    private ConfigCacheEntry(ConfigNode configNode, int znodeVersion) {
      this.configNode = new WeakReference<>(configNode);
      this.znodeVersion = znodeVersion;
    }
  }
  public SolrCloudManager getSolrCloudManager() {
    return zkController.getSolrCloudManager();
  }
}
