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
package org.apache.solr.core;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.lucene.search.IndexSearcher;
import org.apache.solr.common.SolrException;
import org.apache.solr.logging.LogWatcherConfig;
import org.apache.solr.update.UpdateShardHandlerConfig;


public class NodeConfig {

  private final String nodeName;

  private final Path coreRootDirectory;

  private final Path solrDataHome;

  private final Integer booleanQueryMaxClauseCount;
  
  private final Path configSetBaseDirectory;

  private final String sharedLibDirectory;

  private final PluginInfo shardHandlerFactoryConfig;

  private final UpdateShardHandlerConfig updateShardHandlerConfig;

  private final String coreAdminHandlerClass;

  private final String collectionsAdminHandlerClass;

  private final String healthCheckHandlerClass;

  private final String infoHandlerClass;

  private final String configSetsHandlerClass;

  private final LogWatcherConfig logWatcherConfig;

  private final CloudConfig cloudConfig;

  private final Integer coreLoadThreads;

  private final int replayUpdatesThreads;

  @Deprecated
  // This should be part of the transientCacheConfig, remove in 7.0
  private final int transientCacheSize;

  private final boolean useSchemaCache;

  private final String managementPath;

  private final PluginInfo[] backupRepositoryPlugins;

  private final MetricsConfig metricsConfig;

  private final PluginInfo transientCacheConfig;

  private final PluginInfo tracerConfig;

  private NodeConfig(String nodeName, Path coreRootDirectory, Path solrDataHome, Integer booleanQueryMaxClauseCount,
                     Path configSetBaseDirectory, String sharedLibDirectory,
                     PluginInfo shardHandlerFactoryConfig, UpdateShardHandlerConfig updateShardHandlerConfig,
                     String coreAdminHandlerClass, String collectionsAdminHandlerClass,
                     String healthCheckHandlerClass, String infoHandlerClass, String configSetsHandlerClass,
                     LogWatcherConfig logWatcherConfig, CloudConfig cloudConfig, Integer coreLoadThreads, int replayUpdatesThreads,
                     int transientCacheSize, boolean useSchemaCache, String managementPath, SolrResourceLoader loader,
                     Properties solrProperties, PluginInfo[] backupRepositoryPlugins,
                     MetricsConfig metricsConfig, PluginInfo transientCacheConfig, PluginInfo tracerConfig) {
    this.nodeName = nodeName;
    this.coreRootDirectory = coreRootDirectory;
    this.solrDataHome = solrDataHome;
    this.booleanQueryMaxClauseCount = booleanQueryMaxClauseCount;
    this.configSetBaseDirectory = configSetBaseDirectory;
    this.sharedLibDirectory = sharedLibDirectory;
    this.shardHandlerFactoryConfig = shardHandlerFactoryConfig;
    this.updateShardHandlerConfig = updateShardHandlerConfig;
    this.coreAdminHandlerClass = coreAdminHandlerClass;
    this.collectionsAdminHandlerClass = collectionsAdminHandlerClass;
    this.healthCheckHandlerClass = healthCheckHandlerClass;
    this.infoHandlerClass = infoHandlerClass;
    this.configSetsHandlerClass = configSetsHandlerClass;
    this.logWatcherConfig = logWatcherConfig;
    this.cloudConfig = cloudConfig;
    this.coreLoadThreads = coreLoadThreads;
    this.replayUpdatesThreads = replayUpdatesThreads;
    this.transientCacheSize = transientCacheSize;
    this.useSchemaCache = useSchemaCache;
    this.managementPath = managementPath;
    this.loader = loader;
    this.solrProperties = solrProperties;
    this.backupRepositoryPlugins = backupRepositoryPlugins;
    this.metricsConfig = metricsConfig;
    this.transientCacheConfig = transientCacheConfig;
    this.tracerConfig = tracerConfig;

    if (this.cloudConfig != null && this.getCoreLoadThreadCount(false) < 2) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "SolrCloud requires a value of at least 2 for coreLoadThreads (configured value = " + this.coreLoadThreads + ")");
    }
  }

  public String getNodeName() {
    return nodeName;
  }

  public Path getCoreRootDirectory() {
    return coreRootDirectory;
  }

  public Path getSolrDataHome() {
    return solrDataHome;
  }

  /** 
   * If null, the lucene default will not be overridden
   *
   * @see IndexSearcher#setMaxClauseCount
   */
  public Integer getBooleanQueryMaxClauseCount() {
    return booleanQueryMaxClauseCount;
  }
  
  public PluginInfo getShardHandlerFactoryPluginInfo() {
    return shardHandlerFactoryConfig;
  }

  public UpdateShardHandlerConfig getUpdateShardHandlerConfig() {
    return updateShardHandlerConfig;
  }

  public int getCoreLoadThreadCount(boolean zkAware) {
    return coreLoadThreads == null ?
        (zkAware ? NodeConfigBuilder.DEFAULT_CORE_LOAD_THREADS_IN_CLOUD : NodeConfigBuilder.DEFAULT_CORE_LOAD_THREADS)
        : coreLoadThreads;
  }

  public int getReplayUpdatesThreads() {
    return replayUpdatesThreads;
  }

  public String getSharedLibDirectory() {
    return sharedLibDirectory;
  }

  public String getCoreAdminHandlerClass() {
    return coreAdminHandlerClass;
  }
  
  public String getCollectionsHandlerClass() {
    return collectionsAdminHandlerClass;
  }

  public String getHealthCheckHandlerClass() {
    return healthCheckHandlerClass;
  }

  public String getInfoHandlerClass() {
    return infoHandlerClass;
  }

  public String getConfigSetsHandlerClass() {
    return configSetsHandlerClass;
  }

  public boolean hasSchemaCache() {
    return useSchemaCache;
  }

  public String getManagementPath() {
    return managementPath;
  }

  public Path getConfigSetBaseDirectory() {
    return configSetBaseDirectory;
  }

  public LogWatcherConfig getLogWatcherConfig() {
    return logWatcherConfig;
  }

  public CloudConfig getCloudConfig() {
    return cloudConfig;
  }

  public int getTransientCacheSize() {
    return transientCacheSize;
  }

  protected final SolrResourceLoader loader;
  protected final Properties solrProperties;

  public Properties getSolrProperties() {
    return solrProperties;
  }

  public SolrResourceLoader getSolrResourceLoader() {
    return loader;
  }

  public PluginInfo[] getBackupRepositoryPlugins() {
    return backupRepositoryPlugins;
  }

  public MetricsConfig getMetricsConfig() {
    return metricsConfig;
  }

  public PluginInfo getTransientCachePluginInfo() { return transientCacheConfig; }

  public PluginInfo getTracerConfiguratorPluginInfo() {
    return tracerConfig;
  }

  public static class NodeConfigBuilder {

    private Path coreRootDirectory;
    private Path solrDataHome;
    private Integer booleanQueryMaxClauseCount;
    private Path configSetBaseDirectory;
    private String sharedLibDirectory = "lib";
    private PluginInfo shardHandlerFactoryConfig;
    private UpdateShardHandlerConfig updateShardHandlerConfig = UpdateShardHandlerConfig.DEFAULT;
    private String coreAdminHandlerClass = DEFAULT_ADMINHANDLERCLASS;
    private String collectionsAdminHandlerClass = DEFAULT_COLLECTIONSHANDLERCLASS;
    private String healthCheckHandlerClass = DEFAULT_HEALTHCHECKHANDLERCLASS;
    private String infoHandlerClass = DEFAULT_INFOHANDLERCLASS;
    private String configSetsHandlerClass = DEFAULT_CONFIGSETSHANDLERCLASS;
    private LogWatcherConfig logWatcherConfig = new LogWatcherConfig(true, null, null, 50);
    private CloudConfig cloudConfig;
    private int coreLoadThreads = DEFAULT_CORE_LOAD_THREADS;
    private int replayUpdatesThreads = Runtime.getRuntime().availableProcessors();
    @Deprecated
    //Remove in 7.0 and put it all in the transientCache element in solrconfig.xml
    private int transientCacheSize = DEFAULT_TRANSIENT_CACHE_SIZE;
    private boolean useSchemaCache = false;
    private String managementPath;
    private Properties solrProperties = new Properties();
    private PluginInfo[] backupRepositoryPlugins;
    private MetricsConfig metricsConfig;
    private PluginInfo transientCacheConfig;
    private PluginInfo tracerConfig;

    private final SolrResourceLoader loader;
    private final String nodeName;

    public static final int DEFAULT_CORE_LOAD_THREADS = 3;
    //No:of core load threads in cloud mode is set to a default of 8
    public static final int DEFAULT_CORE_LOAD_THREADS_IN_CLOUD = 8;

    public static final int DEFAULT_TRANSIENT_CACHE_SIZE = Integer.MAX_VALUE;

    private static final String DEFAULT_ADMINHANDLERCLASS = "org.apache.solr.handler.admin.CoreAdminHandler";
    private static final String DEFAULT_INFOHANDLERCLASS = "org.apache.solr.handler.admin.InfoHandler";
    private static final String DEFAULT_COLLECTIONSHANDLERCLASS = "org.apache.solr.handler.admin.CollectionsHandler";
    private static final String DEFAULT_HEALTHCHECKHANDLERCLASS = "org.apache.solr.handler.admin.HealthCheckHandler";
    private static final String DEFAULT_CONFIGSETSHANDLERCLASS = "org.apache.solr.handler.admin.ConfigSetsHandler";

    public static final Set<String> DEFAULT_HIDDEN_SYS_PROPS = new HashSet<>(Arrays.asList(
        "javax.net.ssl.keyStorePassword",
        "javax.net.ssl.trustStorePassword",
        "basicauth",
        "zkDigestPassword",
        "zkDigestReadonlyPassword"
    ));

    public NodeConfigBuilder(String nodeName, SolrResourceLoader loader) {
      this.nodeName = nodeName;
      this.loader = loader;
      this.coreRootDirectory = loader.getInstancePath();
      // always init from sysprop because <solrDataHome> config element may be missing
      String dataHomeProperty = System.getProperty(SolrXmlConfig.SOLR_DATA_HOME);
      if (dataHomeProperty != null && !dataHomeProperty.isEmpty()) {
        solrDataHome = loader.getInstancePath().resolve(dataHomeProperty);
      }
      this.configSetBaseDirectory = loader.getInstancePath().resolve("configsets");
      this.metricsConfig = new MetricsConfig.MetricsConfigBuilder().build();
    }

    public NodeConfigBuilder setCoreRootDirectory(String coreRootDirectory) {
      this.coreRootDirectory = loader.getInstancePath().resolve(coreRootDirectory);
      return this;
    }

    public NodeConfigBuilder setSolrDataHome(String solrDataHomeString) {
      // keep it null unless explicitly set to non-empty value
      if (solrDataHomeString != null && !solrDataHomeString.isEmpty()) {
        this.solrDataHome = loader.getInstancePath().resolve(solrDataHomeString);
      }
      return this;
    }
    
    public NodeConfigBuilder setBooleanQueryMaxClauseCount(Integer booleanQueryMaxClauseCount) {
      this.booleanQueryMaxClauseCount = booleanQueryMaxClauseCount;
      return this;
    }

    public NodeConfigBuilder setConfigSetBaseDirectory(String configSetBaseDirectory) {
      this.configSetBaseDirectory = loader.getInstancePath().resolve(configSetBaseDirectory);
      return this;
    }

    public NodeConfigBuilder setSharedLibDirectory(String sharedLibDirectory) {
      this.sharedLibDirectory = sharedLibDirectory;
      return this;
    }

    public NodeConfigBuilder setShardHandlerFactoryConfig(PluginInfo shardHandlerFactoryConfig) {
      this.shardHandlerFactoryConfig = shardHandlerFactoryConfig;
      return this;
    }

    public NodeConfigBuilder setUpdateShardHandlerConfig(UpdateShardHandlerConfig updateShardHandlerConfig) {
      this.updateShardHandlerConfig = updateShardHandlerConfig;
      return this;
    }

    public NodeConfigBuilder setCoreAdminHandlerClass(String coreAdminHandlerClass) {
      this.coreAdminHandlerClass = coreAdminHandlerClass;
      return this;
    }

    public NodeConfigBuilder setCollectionsAdminHandlerClass(String collectionsAdminHandlerClass) {
      this.collectionsAdminHandlerClass = collectionsAdminHandlerClass;
      return this;
    }

    public NodeConfigBuilder setHealthCheckHandlerClass(String healthCheckHandlerClass) {
      this.healthCheckHandlerClass = healthCheckHandlerClass;
      return this;
    }

    public NodeConfigBuilder setInfoHandlerClass(String infoHandlerClass) {
      this.infoHandlerClass = infoHandlerClass;
      return this;
    }

    public NodeConfigBuilder setConfigSetsHandlerClass(String configSetsHandlerClass) {
      this.configSetsHandlerClass = configSetsHandlerClass;
      return this;
    }

    public NodeConfigBuilder setLogWatcherConfig(LogWatcherConfig logWatcherConfig) {
      this.logWatcherConfig = logWatcherConfig;
      return this;
    }

    public NodeConfigBuilder setCloudConfig(CloudConfig cloudConfig) {
      this.cloudConfig = cloudConfig;
      return this;
    }

    public NodeConfigBuilder setCoreLoadThreads(int coreLoadThreads) {
      this.coreLoadThreads = coreLoadThreads;
      return this;
    }

    public NodeConfigBuilder setReplayUpdatesThreads(int replayUpdatesThreads) {
      this.replayUpdatesThreads = replayUpdatesThreads;
      return this;
    }

    // Remove in Solr 7.0
    @Deprecated
    public NodeConfigBuilder setTransientCacheSize(int transientCacheSize) {
      this.transientCacheSize = transientCacheSize;
      return this;
    }

    public NodeConfigBuilder setUseSchemaCache(boolean useSchemaCache) {
      this.useSchemaCache = useSchemaCache;
      return this;
    }

    public NodeConfigBuilder setManagementPath(String managementPath) {
      this.managementPath = managementPath;
      return this;
    }

    public NodeConfigBuilder setSolrProperties(Properties solrProperties) {
      this.solrProperties = solrProperties;
      return this;
    }

    public NodeConfigBuilder setBackupRepositoryPlugins(PluginInfo[] backupRepositoryPlugins) {
      this.backupRepositoryPlugins = backupRepositoryPlugins;
      return this;
    }

    public NodeConfigBuilder setMetricsConfig(MetricsConfig metricsConfig) {
      this.metricsConfig = metricsConfig;
      return this;
    }
    
    public NodeConfigBuilder setSolrCoreCacheFactoryConfig(PluginInfo transientCacheConfig) {
      this.transientCacheConfig = transientCacheConfig;
      return this;
    }

    public NodeConfigBuilder setTracerConfig(PluginInfo tracerConfig) {
      this.tracerConfig = tracerConfig;
      return this;
    }

    public NodeConfig build() {
      return new NodeConfig(nodeName, coreRootDirectory, solrDataHome, booleanQueryMaxClauseCount,
                            configSetBaseDirectory, sharedLibDirectory, shardHandlerFactoryConfig,
                            updateShardHandlerConfig, coreAdminHandlerClass, collectionsAdminHandlerClass, healthCheckHandlerClass, infoHandlerClass, configSetsHandlerClass,
                            logWatcherConfig, cloudConfig, coreLoadThreads, replayUpdatesThreads, transientCacheSize, useSchemaCache, managementPath, loader, solrProperties,
                            backupRepositoryPlugins, metricsConfig, transientCacheConfig, tracerConfig);
    }
  }
}

