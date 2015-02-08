package org.apache.solr.core;

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

import org.apache.commons.io.IOUtils;
import org.apache.solr.cloud.CloudConfigSetService;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.logging.LogWatcherConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;


public abstract class ConfigSolr {
  protected static Logger log = LoggerFactory.getLogger(ConfigSolr.class);
  
  public final static String SOLR_XML_FILE = "solr.xml";

  public static ConfigSolr fromFile(SolrResourceLoader loader, File configFile) {
    log.info("Loading container configuration from {}", configFile.getAbsolutePath());

    if (!configFile.exists()) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "solr.xml does not exist in " + configFile.getAbsolutePath() + " cannot start Solr");
    }

    try (InputStream inputStream = new FileInputStream(configFile)) {
      return fromInputStream(loader, inputStream);
    } catch (SolrException exc) {
      throw exc;
    } catch (Exception exc) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Could not load SOLR configuration", exc);
    }
  }

  public static ConfigSolr fromString(SolrResourceLoader loader, String xml) {
    return fromInputStream(loader, new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
  }

  public static ConfigSolr fromInputStream(SolrResourceLoader loader, InputStream is) {
    try {
      byte[] buf = IOUtils.toByteArray(is);
      try (ByteArrayInputStream dup = new ByteArrayInputStream(buf)) {
        Config config = new Config(loader, null, new InputSource(dup), null, false);
        return new ConfigSolrXml(config);
      }
    } catch (SolrException exc) {
      throw exc;
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  public static ConfigSolr fromSolrHome(SolrResourceLoader loader, String solrHome) {
    return fromFile(loader, new File(solrHome, SOLR_XML_FILE));
  }
  
  public abstract CoresLocator getCoresLocator();


  /**
   * The directory against which relative core instance dirs are resolved.  If none is
   * specified in the config, uses solr home.
   *
   * @return core root directory
   */
  public String getCoreRootDirectory() {
    String relativeDir = getString(CfgProp.SOLR_COREROOTDIRECTORY, null);
    if (relativeDir != null)
      return loader.resolve(relativeDir);
    return loader.getInstanceDir();
  }

  public abstract PluginInfo getShardHandlerFactoryPluginInfo();

  public String getZkHost() {
    String sysZkHost = System.getProperty("zkHost");
    if (sysZkHost != null)
      return sysZkHost;
    return getString(CfgProp.SOLR_ZKHOST, null);
  }

  public int getZkClientTimeout() {
    String sysProp = System.getProperty("zkClientTimeout");
    if (sysProp != null)
      return Integer.parseInt(sysProp);
    return getInt(CfgProp.SOLR_ZKCLIENTTIMEOUT, DEFAULT_ZK_CLIENT_TIMEOUT);
  }

  private static final int DEFAULT_ZK_CLIENT_TIMEOUT = 15000;
  private static final int DEFAULT_LEADER_VOTE_WAIT = 180000;  // 3 minutes
  private static final int DEFAULT_LEADER_CONFLICT_RESOLVE_WAIT = 180000;
  private static final int DEFAULT_CORE_LOAD_THREADS = 3;
  
  // TODO: tune defaults
  private static final int DEFAULT_AUTO_REPLICA_FAILOVER_WAIT_AFTER_EXPIRATION = 30000;
  private static final int DEFAULT_AUTO_REPLICA_FAILOVER_WORKLOOP_DELAY = 10000;
  private static final int DEFAULT_AUTO_REPLICA_FAILOVER_BAD_NODE_EXPIRATION = 60000;
  
  public static final int DEFAULT_DISTRIBUPDATECONNTIMEOUT = 60000;
  public static final int DEFAULT_DISTRIBUPDATESOTIMEOUT = 600000;

  protected static final String DEFAULT_CORE_ADMIN_PATH = "/admin/cores";

  public String getSolrHostPort() {
    return getString(CfgProp.SOLR_HOSTPORT, null);
  }

  public String getZkHostContext() {
    return getString(CfgProp.SOLR_HOSTCONTEXT, null);
  }

  public String getHost() {
    return getString(CfgProp.SOLR_HOST, null);
  }

  public int getLeaderVoteWait() {
    return getInt(CfgProp.SOLR_LEADERVOTEWAIT, DEFAULT_LEADER_VOTE_WAIT);
  }
  
  public int getLeaderConflictResolveWait() {
    return getInt(CfgProp.SOLR_LEADERCONFLICTRESOLVEWAIT, DEFAULT_LEADER_CONFLICT_RESOLVE_WAIT);
  }
  
  public int getAutoReplicaFailoverWaitAfterExpiration() {
    return getInt(CfgProp.SOLR_AUTOREPLICAFAILOVERWAITAFTEREXPIRATION, DEFAULT_AUTO_REPLICA_FAILOVER_WAIT_AFTER_EXPIRATION);
  }
  
  public int getAutoReplicaFailoverWorkLoopDelay() {
    return getInt(CfgProp.SOLR_AUTOREPLICAFAILOVERWORKLOOPDELAY, DEFAULT_AUTO_REPLICA_FAILOVER_WORKLOOP_DELAY);
  }
  
  public int getAutoReplicaFailoverBadNodeExpiration() {
    return getInt(CfgProp.SOLR_AUTOREPLICAFAILOVERBADNODEEXPIRATION, DEFAULT_AUTO_REPLICA_FAILOVER_BAD_NODE_EXPIRATION);
  }

  public boolean getGenericCoreNodeNames() {
    return getBoolean(CfgProp.SOLR_GENERICCORENODENAMES, false);
  }

  public int getDistributedConnectionTimeout() {
    return getInt(CfgProp.SOLR_DISTRIBUPDATECONNTIMEOUT, DEFAULT_DISTRIBUPDATECONNTIMEOUT);
  }

  public int getDistributedSocketTimeout() {
    return getInt(CfgProp.SOLR_DISTRIBUPDATESOTIMEOUT, DEFAULT_DISTRIBUPDATESOTIMEOUT);
  }
  
  public int getMaxUpdateConnections() {
    return getInt(CfgProp.SOLR_MAXUPDATECONNECTIONS, 10000);
  }

  public int getMaxUpdateConnectionsPerHost() {
    return getInt(CfgProp.SOLR_MAXUPDATECONNECTIONSPERHOST, 100);
  }

  public int getCoreLoadThreadCount() {
    return getInt(ConfigSolr.CfgProp.SOLR_CORELOADTHREADS, DEFAULT_CORE_LOAD_THREADS);
  }

  public String getSharedLibDirectory() {
    return getString(ConfigSolr.CfgProp.SOLR_SHAREDLIB, null);
  }

  public String getCoreAdminHandlerClass() {
    return getString(CfgProp.SOLR_ADMINHANDLER, "org.apache.solr.handler.admin.CoreAdminHandler");
  }
  
  public String getZkCredentialsProviderClass() {
    return getString(CfgProp.SOLR_ZKCREDENTIALSPROVIDER, null);
  }

  public String getZkACLProviderClass() {
    return getString(CfgProp.SOLR_ZKACLPROVIDER, null);
  }
  
  public String getCollectionsHandlerClass() {
    return getString(CfgProp.SOLR_COLLECTIONSHANDLER, "org.apache.solr.handler.admin.CollectionsHandler");
  }

  public String getInfoHandlerClass() {
    return getString(CfgProp.SOLR_INFOHANDLER, "org.apache.solr.handler.admin.InfoHandler");
  }

  public boolean hasSchemaCache() {
    return getBoolean(ConfigSolr.CfgProp.SOLR_SHARESCHEMA, false);
  }

  public String getManagementPath() {
    return getString(CfgProp.SOLR_MANAGEMENTPATH, null);
  }

  public String getConfigSetBaseDirectory() {
    return getString(CfgProp.SOLR_CONFIGSETBASEDIR, "configsets");
  }

  public LogWatcherConfig getLogWatcherConfig() {
    String loggingClass = getString(CfgProp.SOLR_LOGGING_CLASS, null);
    String loggingWatcherThreshold = getString(CfgProp.SOLR_LOGGING_WATCHER_THRESHOLD, null);
    return new LogWatcherConfig(
        getBoolean(CfgProp.SOLR_LOGGING_ENABLED, true),
        loggingClass,
        loggingWatcherThreshold,
        getInt(CfgProp.SOLR_LOGGING_WATCHER_SIZE, 50)
    );
  }

  public int getTransientCacheSize() {
    return getInt(CfgProp.SOLR_TRANSIENTCACHESIZE, Integer.MAX_VALUE);
  }

  public ConfigSetService createCoreConfigService(SolrResourceLoader loader, ZkController zkController) {
    if (getZkHost() != null || System.getProperty("zkRun") != null)
      return new CloudConfigSetService(loader, zkController);
    if (hasSchemaCache())
      return new ConfigSetService.SchemaCaching(loader, getConfigSetBaseDirectory());
    return new ConfigSetService.Default(loader, getConfigSetBaseDirectory());
  }

  // Ugly for now, but we'll at least be able to centralize all of the differences between 4x and 5x.
  public static enum CfgProp {
    SOLR_ADMINHANDLER,
    SOLR_COLLECTIONSHANDLER,
    SOLR_CORELOADTHREADS,
    SOLR_COREROOTDIRECTORY,
    SOLR_DISTRIBUPDATECONNTIMEOUT,
    SOLR_DISTRIBUPDATESOTIMEOUT,
    SOLR_MAXUPDATECONNECTIONS,
    SOLR_MAXUPDATECONNECTIONSPERHOST,
    SOLR_HOST,
    SOLR_HOSTCONTEXT,
    SOLR_HOSTPORT,
    SOLR_INFOHANDLER,
    SOLR_LEADERVOTEWAIT,
    SOLR_LOGGING_CLASS,
    SOLR_LOGGING_ENABLED,
    SOLR_LOGGING_WATCHER_SIZE,
    SOLR_LOGGING_WATCHER_THRESHOLD,
    SOLR_MANAGEMENTPATH,
    SOLR_SHAREDLIB,
    SOLR_SHARESCHEMA,
    SOLR_TRANSIENTCACHESIZE,
    SOLR_GENERICCORENODENAMES,
    SOLR_ZKCLIENTTIMEOUT,
    SOLR_ZKHOST,
    SOLR_LEADERCONFLICTRESOLVEWAIT,
    SOLR_CONFIGSETBASEDIR,

    SOLR_AUTOREPLICAFAILOVERWAITAFTEREXPIRATION,
    SOLR_AUTOREPLICAFAILOVERWORKLOOPDELAY,
    SOLR_AUTOREPLICAFAILOVERBADNODEEXPIRATION,
    
    SOLR_ZKCREDENTIALSPROVIDER,
    SOLR_ZKACLPROVIDER,

  }

  protected final SolrResourceLoader loader;
  protected final Properties solrProperties;

  public ConfigSolr(SolrResourceLoader loader, Properties solrProperties) {
    this.loader = loader;
    this.solrProperties = solrProperties;
  }

  public ConfigSolr(SolrResourceLoader loader) {
    this(loader, new Properties());
  }

  protected abstract String getProperty(CfgProp key);

  private String getString(CfgProp key, String defaultValue) {
    String v = getProperty(key);
    return v == null ? defaultValue : v;
  }

  private int getInt(CfgProp key, int defaultValue) {
    String v = getProperty(key);
    return v == null ? defaultValue : Integer.parseInt(v);
  }

  private boolean getBoolean(CfgProp key, boolean defaultValue) {
    String v = getProperty(key);
    return v == null ? defaultValue : Boolean.parseBoolean(v);
  }

  public Properties getSolrProperties() {
    return solrProperties;
  }

  public SolrResourceLoader getSolrResourceLoader() {
    return loader;
  }

}

