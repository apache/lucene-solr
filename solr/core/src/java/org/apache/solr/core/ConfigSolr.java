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

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.solr.cloud.CloudConfigSetService;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.logging.LogWatcherConfig;
import org.apache.solr.util.DOMUtil;
import org.apache.solr.util.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;


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
      String originalXml = new String(buf, StandardCharsets.UTF_8);
      try (ByteArrayInputStream dup = new ByteArrayInputStream(buf)) {
        Config config = new Config(loader, null, new InputSource(dup), null, false);
        return fromConfig(config, originalXml);
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

  public static ConfigSolr fromConfig(Config config, String originalXml) {
    boolean oldStyle = (config.getNode("solr/cores", false) != null);
    return oldStyle ? new ConfigSolrXmlOld(config, originalXml)
                    : new ConfigSolrXml(config);
  }
  
  public abstract CoresLocator getCoresLocator();


  /**
   * The directory against which relative core instance dirs are resolved.  If none is
   * specified in the config, uses solr home.
   *
   * @return core root directory
   */
  public String getCoreRootDirectory() {
    return get(CfgProp.SOLR_COREROOTDIRECTORY, config.getResourceLoader().getInstanceDir());
  }

  public PluginInfo getShardHandlerFactoryPluginInfo() {
    Node node = config.getNode(getShardHandlerFactoryConfigPath(), false);
    return (node == null) ? null : new PluginInfo(node, "shardHandlerFactory", false, true);
  }

  protected abstract String getShardHandlerFactoryConfigPath();

  public String getZkHost() {
    String sysZkHost = System.getProperty("zkHost");
    if (sysZkHost != null)
      return sysZkHost;
    return get(CfgProp.SOLR_ZKHOST, null);
  }

  public int getZkClientTimeout() {
    String sysProp = System.getProperty("zkClientTimeout");
    if (sysProp != null)
      return Integer.parseInt(sysProp);
    return get(CfgProp.SOLR_ZKCLIENTTIMEOUT, DEFAULT_ZK_CLIENT_TIMEOUT);
  }

  private static final int DEFAULT_ZK_CLIENT_TIMEOUT = 15000;
  private static final int DEFAULT_LEADER_VOTE_WAIT = 180000;  // 3 minutes
  private static final int DEFAULT_LEADER_CONFLICT_RESOLVE_WAIT = 180000;
  private static final int DEFAULT_CORE_LOAD_THREADS = 3;

  protected static final String DEFAULT_CORE_ADMIN_PATH = "/admin/cores";

  public String getZkHostPort() {
    return get(CfgProp.SOLR_HOSTPORT, null);
  }

  public String getZkHostContext() {
    return get(CfgProp.SOLR_HOSTCONTEXT, null);
  }

  public String getHost() {
    return get(CfgProp.SOLR_HOST, null);
  }

  public int getLeaderVoteWait() {
    return get(CfgProp.SOLR_LEADERVOTEWAIT, DEFAULT_LEADER_VOTE_WAIT);
  }
  
  public int getLeaderConflictResolveWait() {
    return get(CfgProp.SOLR_LEADERCONFLICTRESOLVEWAIT, DEFAULT_LEADER_CONFLICT_RESOLVE_WAIT);
  }

  public boolean getGenericCoreNodeNames() {
    return get(CfgProp.SOLR_GENERICCORENODENAMES, false);
  }

  public int getDistributedConnectionTimeout() {
    return get(CfgProp.SOLR_DISTRIBUPDATECONNTIMEOUT, 0);
  }

  public int getDistributedSocketTimeout() {
    return get(CfgProp.SOLR_DISTRIBUPDATESOTIMEOUT, 0);
  }
  
  public int getMaxUpdateConnections() {
    return get(CfgProp.SOLR_MAXUPDATECONNECTIONS, 10000);
  }

  public int getMaxUpdateConnectionsPerHost() {
    return get(CfgProp.SOLR_MAXUPDATECONNECTIONSPERHOST, 100);
  }

  public int getCoreLoadThreadCount() {
    return get(ConfigSolr.CfgProp.SOLR_CORELOADTHREADS, DEFAULT_CORE_LOAD_THREADS);
  }

  public String getSharedLibDirectory() {
    return get(ConfigSolr.CfgProp.SOLR_SHAREDLIB , null);
  }

  public String getDefaultCoreName() {
    return get(CfgProp.SOLR_CORES_DEFAULT_CORE_NAME, null);
  }

  public abstract boolean isPersistent();

  public String getAdminPath() {
    return get(CfgProp.SOLR_ADMINPATH, DEFAULT_CORE_ADMIN_PATH);
  }

  public String getCoreAdminHandlerClass() {
    return get(CfgProp.SOLR_ADMINHANDLER, "org.apache.solr.handler.admin.CoreAdminHandler");
  }

  public String getCollectionsHandlerClass() {
    return get(CfgProp.SOLR_COLLECTIONSHANDLER, "org.apache.solr.handler.admin.CollectionsHandler");
  }

  public String getInfoHandlerClass() {
    return get(CfgProp.SOLR_INFOHANDLER, "org.apache.solr.handler.admin.InfoHandler");
  }

  public boolean hasSchemaCache() {
    return get(ConfigSolr.CfgProp.SOLR_SHARESCHEMA, false);
  }

  public String getManagementPath() {
    return get(CfgProp.SOLR_MANAGEMENTPATH, null);
  }

  public String getConfigSetBaseDirectory() {
    return get(CfgProp.SOLR_CONFIGSETBASEDIR, "configsets");
  }

  public LogWatcherConfig getLogWatcherConfig() {
    String loggingClass = get(CfgProp.SOLR_LOGGING_CLASS, null);
    String loggingWatcherThreshold = get(CfgProp.SOLR_LOGGING_WATCHER_THRESHOLD, null);
    return new LogWatcherConfig(
        get(CfgProp.SOLR_LOGGING_ENABLED, true),
        loggingClass,
        loggingWatcherThreshold,
        get(CfgProp.SOLR_LOGGING_WATCHER_SIZE, 50)
    );
  }

  public int getTransientCacheSize() {
    return get(CfgProp.SOLR_TRANSIENTCACHESIZE, Integer.MAX_VALUE);
  }

  public ConfigSetService createCoreConfigService(SolrResourceLoader loader, ZkController zkController) {
    if (getZkHost() != null || System.getProperty("zkRun") != null)
      return new CloudConfigSetService(loader, zkController);
    if (hasSchemaCache())
      return new ConfigSetService.SchemaCaching(loader, getConfigSetBaseDirectory());
    return new ConfigSetService.Default(loader, getConfigSetBaseDirectory());
  }

  // Ugly for now, but we'll at least be able to centralize all of the differences between 4x and 5x.
  protected static enum CfgProp {
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

    //TODO: Remove all of these elements for 5.0
    SOLR_PERSISTENT,
    SOLR_CORES_DEFAULT_CORE_NAME,
    SOLR_ADMINPATH
  }

  protected Config config;
  protected Map<CfgProp, Object> propMap = new HashMap<>();

  public ConfigSolr(Config config) {
    this.config = config;
    config.substituteProperties();
  }

  // for extension & testing.
  protected ConfigSolr() {

  }
  
  public Config getConfig() {
    return config;
  }

  @SuppressWarnings("unchecked")
  public <T> T get(CfgProp key, T defaultValue) {
    if (propMap.containsKey(key) && propMap.get(key) != null) {
      return (T) propMap.get(key);
    }
    return defaultValue;
  }

  public Properties getSolrProperties(String path) {
    try {
      return readProperties(((NodeList) config.evaluate(
          path, XPathConstants.NODESET)).item(0));
    } catch (Exception e) {
      SolrException.log(log, null, e);
    }
    return null;

  }
  
  protected Properties readProperties(Node node) throws XPathExpressionException {
    XPath xpath = config.getXPath();
    NodeList props = (NodeList) xpath.evaluate("property", node, XPathConstants.NODESET);
    Properties properties = new Properties();
    for (int i = 0; i < props.getLength(); i++) {
      Node prop = props.item(i);
      properties.setProperty(DOMUtil.getAttr(prop, "name"),
          PropertiesUtil.substituteProperty(DOMUtil.getAttr(prop, "value"), null));
    }
    return properties;
  }

}

