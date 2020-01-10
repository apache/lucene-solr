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

import javax.management.MBeanServer;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.google.common.base.Strings;
import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.logging.LogWatcherConfig;
import org.apache.solr.metrics.reporters.SolrJmxReporter;
import org.apache.solr.update.UpdateShardHandlerConfig;
import org.apache.solr.util.DOMUtil;
import org.apache.solr.util.JmxUtil;
import org.apache.solr.util.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import static org.apache.solr.common.params.CommonParams.NAME;


/**
 *
 */
public class SolrXmlConfig {

  public final static String SOLR_XML_FILE = "solr.xml";
  public final static String SOLR_DATA_HOME = "solr.data.home";

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static NodeConfig fromConfig(XmlConfigFile config) {

    checkForIllegalConfig(config);

    config.substituteProperties();

    CloudConfig cloudConfig = null;
    UpdateShardHandlerConfig deprecatedUpdateConfig = null;

    if (config.getNodeList("solr/solrcloud", false).getLength() > 0) {
      NamedList<Object> cloudSection = readNodeListAsNamedList(config, "solr/solrcloud/*[@name]", "<solrcloud>");
      deprecatedUpdateConfig = loadUpdateConfig(cloudSection, false);
      cloudConfig = fillSolrCloudSection(cloudSection);
    }

    NamedList<Object> entries = readNodeListAsNamedList(config, "solr/*[@name]", "<solr>");
    String nodeName = (String) entries.remove("nodeName");
    if (Strings.isNullOrEmpty(nodeName) && cloudConfig != null)
      nodeName = cloudConfig.getHost();

    UpdateShardHandlerConfig updateConfig;
    if (deprecatedUpdateConfig == null) {
      updateConfig = loadUpdateConfig(readNodeListAsNamedList(config, "solr/updateshardhandler/*[@name]", "<updateshardhandler>"), true);
    }
    else {
      updateConfig = loadUpdateConfig(readNodeListAsNamedList(config, "solr/updateshardhandler/*[@name]", "<updateshardhandler>"), false);
      if (updateConfig != null) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "UpdateShardHandler configuration defined twice in solr.xml");
      }
      updateConfig = deprecatedUpdateConfig;
    }

    NodeConfig.NodeConfigBuilder configBuilder = new NodeConfig.NodeConfigBuilder(nodeName, config.getResourceLoader());
    configBuilder.setUpdateShardHandlerConfig(updateConfig);
    configBuilder.setShardHandlerFactoryConfig(getShardHandlerFactoryPluginInfo(config));
    configBuilder.setSolrCoreCacheFactoryConfig(getTransientCoreCacheFactoryPluginInfo(config));
    configBuilder.setTracerConfig(getTracerPluginInfo(config));
    configBuilder.setLogWatcherConfig(loadLogWatcherConfig(config, "solr/logging/*[@name]", "solr/logging/watcher/*[@name]"));
    configBuilder.setSolrProperties(loadProperties(config));
    if (cloudConfig != null)
      configBuilder.setCloudConfig(cloudConfig);
    configBuilder.setBackupRepositoryPlugins(getBackupRepositoryPluginInfos(config));
    configBuilder.setMetricsConfig(getMetricsConfig(config));
    return fillSolrSection(configBuilder, entries);
  }

  public static NodeConfig fromFile(SolrResourceLoader loader, Path configFile) {

    log.info("Loading container configuration from {}", configFile);

    if (!Files.exists(configFile)) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "solr.xml does not exist in " + configFile.getParent() + " cannot start Solr");
    }

    try (InputStream inputStream = Files.newInputStream(configFile)) {
      return fromInputStream(loader, inputStream);
    } catch (SolrException exc) {
      throw exc;
    } catch (Exception exc) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Could not load SOLR configuration", exc);
    }
  }

  public static NodeConfig fromString(SolrResourceLoader loader, String xml) {
    return fromInputStream(loader, new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
  }

  public static NodeConfig fromInputStream(SolrResourceLoader loader, InputStream is) {
    try {
      byte[] buf = IOUtils.toByteArray(is);
      try (ByteArrayInputStream dup = new ByteArrayInputStream(buf)) {
        XmlConfigFile config = new XmlConfigFile(loader, null, new InputSource(dup), null, false);
        return fromConfig(config);
      }
    } catch (SolrException exc) {
      throw exc;
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  public static NodeConfig fromSolrHome(SolrResourceLoader loader, Path solrHome) {
    return fromFile(loader, solrHome.resolve(SOLR_XML_FILE));
  }

  public static NodeConfig fromSolrHome(Path solrHome) {
    SolrResourceLoader loader = new SolrResourceLoader(solrHome);
    return fromSolrHome(loader, solrHome);
  }

  private static void checkForIllegalConfig(XmlConfigFile config) {
    failIfFound(config, "solr/@coreLoadThreads");
    failIfFound(config, "solr/@persistent");
    failIfFound(config, "solr/@sharedLib");
    failIfFound(config, "solr/@zkHost");
    failIfFound(config, "solr/cores");

    assertSingleInstance("solrcloud", config);
    assertSingleInstance("logging", config);
    assertSingleInstance("logging/watcher", config);
    assertSingleInstance("backup", config);
  }

  private static void assertSingleInstance(String section, XmlConfigFile config) {
    if (config.getNodeList("/solr/" + section, false).getLength() > 1)
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Multiple instances of " + section + " section found in solr.xml");
  }

  private static void failIfFound(XmlConfigFile config, String xPath) {

    if (config.getVal(xPath, false) != null) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Should not have found " + xPath +
          "\n. Please upgrade your solr.xml: https://lucene.apache.org/solr/guide/format-of-solr-xml.html");
    }
  }

  private static Properties loadProperties(XmlConfigFile config) {
    try {
      Node node = ((NodeList) config.evaluate("solr", XPathConstants.NODESET)).item(0);
      XPath xpath = config.getXPath();
      NodeList props = (NodeList) xpath.evaluate("property", node, XPathConstants.NODESET);
      Properties properties = new Properties();
      for (int i = 0; i < props.getLength(); i++) {
        Node prop = props.item(i);
        properties.setProperty(DOMUtil.getAttr(prop, NAME),
            PropertiesUtil.substituteProperty(DOMUtil.getAttr(prop, "value"), null));
      }
      return properties;
    }
    catch (XPathExpressionException e) {
      log.warn("Error parsing solr.xml: " + e.getMessage());
      return null;
    }
  }

  private static NamedList<Object> readNodeListAsNamedList(XmlConfigFile config, String path, String section) {
    NodeList nodes = config.getNodeList(path, false);
    if (nodes == null) {
      return null;
    }
    return checkForDuplicates(section, DOMUtil.nodesToNamedList(nodes));
  }

  private static NamedList<Object> checkForDuplicates(String section, NamedList<Object> nl) {
    Set<String> keys = new HashSet<>();
    for (Map.Entry<String, Object> entry : nl) {
      if (!keys.add(entry.getKey()))
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            section + " section of solr.xml contains duplicated '" + entry.getKey() + "'");
    }
    return nl;
  }

  private static int parseInt(String field, String value) {
    try {
      return Integer.parseInt(value);
    }
    catch (NumberFormatException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Error parsing '" + field + "', value '" + value + "' cannot be parsed as int");
    }
  }

  private static NodeConfig fillSolrSection(NodeConfig.NodeConfigBuilder builder, NamedList<Object> nl) {

    for (Map.Entry<String, Object> entry : nl) {
      String name = entry.getKey();
      if (entry.getValue() == null)
        continue;
      String value = entry.getValue().toString();
      switch (name) {
        case "adminHandler":
          builder.setCoreAdminHandlerClass(value);
          break;
        case "collectionsHandler":
          builder.setCollectionsAdminHandlerClass(value);
          break;
        case "healthCheckHandler":
          builder.setHealthCheckHandlerClass(value);
          break;
        case "infoHandler":
          builder.setInfoHandlerClass(value);
          break;
        case "configSetsHandler":
          builder.setConfigSetsHandlerClass(value);
          break;
        case "coreRootDirectory":
          builder.setCoreRootDirectory(value);
          break;
        case "solrDataHome":
          builder.setSolrDataHome(value);
          break;
        case "maxBooleanClauses":
          builder.setBooleanQueryMaxClauseCount(parseInt(name, value));
          break;
        case "managementPath":
          builder.setManagementPath(value);
          break;
        case "sharedLib":
          builder.setSharedLibDirectory(value);
          break;
        case "configSetBaseDir":
          builder.setConfigSetBaseDirectory(value);
          break;
        case "shareSchema":
          builder.setUseSchemaCache(Boolean.parseBoolean(value));
          break;
        case "coreLoadThreads":
          builder.setCoreLoadThreads(parseInt(name, value));
          break;
        case "replayUpdatesThreads":
          builder.setReplayUpdatesThreads(parseInt(name, value));
          break;
        case "transientCacheSize":
          builder.setTransientCacheSize(parseInt(name, value));
          break;
        default:
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown configuration value in solr.xml: " + name);
      }
    }

    return builder.build();
  }

  private static UpdateShardHandlerConfig loadUpdateConfig(NamedList<Object> nl, boolean alwaysDefine) {

    if (nl == null && !alwaysDefine)
      return null;

    if (nl == null)
      return UpdateShardHandlerConfig.DEFAULT;

    boolean defined = false;

    int maxUpdateConnections = HttpClientUtil.DEFAULT_MAXCONNECTIONS;
    int maxUpdateConnectionsPerHost = HttpClientUtil.DEFAULT_MAXCONNECTIONSPERHOST;
    int distributedSocketTimeout = HttpClientUtil.DEFAULT_SO_TIMEOUT;
    int distributedConnectionTimeout = HttpClientUtil.DEFAULT_CONNECT_TIMEOUT;
    String metricNameStrategy = UpdateShardHandlerConfig.DEFAULT_METRICNAMESTRATEGY;
    int maxRecoveryThreads = UpdateShardHandlerConfig.DEFAULT_MAXRECOVERYTHREADS;

    Object muc = nl.remove("maxUpdateConnections");
    if (muc != null) {
      maxUpdateConnections = parseInt("maxUpdateConnections", muc.toString());
      defined = true;
    }

    Object mucph = nl.remove("maxUpdateConnectionsPerHost");
    if (mucph != null) {
      maxUpdateConnectionsPerHost = parseInt("maxUpdateConnectionsPerHost", mucph.toString());
      defined = true;
    }

    Object dst = nl.remove("distribUpdateSoTimeout");
    if (dst != null) {
      distributedSocketTimeout = parseInt("distribUpdateSoTimeout", dst.toString());
      defined = true;
    }

    Object dct = nl.remove("distribUpdateConnTimeout");
    if (dct != null) {
      distributedConnectionTimeout = parseInt("distribUpdateConnTimeout", dct.toString());
      defined = true;
    }

    Object mns = nl.remove("metricNameStrategy");
    if (mns != null)  {
      metricNameStrategy = mns.toString();
      defined = true;
    }

    Object mrt = nl.remove("maxRecoveryThreads");
    if (mrt != null)  {
      maxRecoveryThreads = parseInt("maxRecoveryThreads", mrt.toString());
      defined = true;
    }

    if (!defined && !alwaysDefine)
      return null;

    return new UpdateShardHandlerConfig(maxUpdateConnections, maxUpdateConnectionsPerHost, distributedSocketTimeout,
                                        distributedConnectionTimeout, metricNameStrategy, maxRecoveryThreads);

  }

  private static String removeValue(NamedList<Object> nl, String key) {
    Object value = nl.remove(key);
    if (value == null)
      return null;
    return value.toString();
  }

  private static String required(String section, String key, String value) {
    if (value != null)
      return value;
    throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, section + " section missing required entry '" + key + "'");
  }

  private static CloudConfig fillSolrCloudSection(NamedList<Object> nl) {

    String hostName = required("solrcloud", "host", removeValue(nl, "host"));
    int hostPort = parseInt("hostPort", required("solrcloud", "hostPort", removeValue(nl, "hostPort")));
    String hostContext = required("solrcloud", "hostContext", removeValue(nl, "hostContext"));

    CloudConfig.CloudConfigBuilder builder = new CloudConfig.CloudConfigBuilder(hostName, hostPort, hostContext);

    for (Map.Entry<String, Object> entry : nl) {
      String name = entry.getKey();
      if (entry.getValue() == null)
        continue;
      String value = entry.getValue().toString();
      switch (name) {
        case "leaderVoteWait":
          builder.setLeaderVoteWait(parseInt(name, value));
          break;
        case "leaderConflictResolveWait":
          builder.setLeaderConflictResolveWait(parseInt(name, value));
          break;
        case "zkClientTimeout":
          builder.setZkClientTimeout(parseInt(name, value));
          break;
        case "autoReplicaFailoverBadNodeExpiration": case "autoReplicaFailoverWorkLoopDelay":
          //TODO remove this in Solr 8.0
          log.info("Configuration parameter " + name + " is ignored");
          break;
        case "autoReplicaFailoverWaitAfterExpiration":
          builder.setAutoReplicaFailoverWaitAfterExpiration(parseInt(name, value));
          break;
        case "zkHost":
          builder.setZkHost(value);
          break;
        case "genericCoreNodeNames":
          builder.setUseGenericCoreNames(Boolean.parseBoolean(value));
          break;
        case "zkACLProvider":
          builder.setZkACLProviderClass(value);
          break;
        case "zkCredentialsProvider":
          builder.setZkCredentialsProviderClass(value);
          break;
        case "createCollectionWaitTimeTillActive":
          builder.setCreateCollectionWaitTimeTillActive(parseInt(name, value));
          break;
        case "createCollectionCheckLeaderActive":
          builder.setCreateCollectionCheckLeaderActive(Boolean.parseBoolean(value));
          break;
        default:
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown configuration parameter in <solrcloud> section of solr.xml: " + name);
      }
    }

    return builder.build();
  }

  private static LogWatcherConfig loadLogWatcherConfig(XmlConfigFile config, String loggingPath, String watcherPath) {

    String loggingClass = null;
    boolean enabled = true;
    int watcherQueueSize = 50;
    String watcherThreshold = null;

    for (Map.Entry<String, Object> entry : readNodeListAsNamedList(config, loggingPath, "<logging>")) {
      String name = entry.getKey();
      String value = entry.getValue().toString();
      switch (name) {
        case "class":
          loggingClass = value; break;
        case "enabled":
          enabled = Boolean.parseBoolean(value); break;
        default:
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown value in logwatcher config: " + name);
      }
    }

    for (Map.Entry<String, Object> entry : readNodeListAsNamedList(config, watcherPath, "<watcher>")) {
      String name = entry.getKey();
      String value = entry.getValue().toString();
      switch (name) {
        case "size":
          watcherQueueSize = parseInt(name, value); break;
        case "threshold":
          watcherThreshold = value; break;
        default:
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown value in logwatcher config: " + name);
      }
    }

    return new LogWatcherConfig(enabled, loggingClass, watcherThreshold, watcherQueueSize);

  }

  private static PluginInfo getShardHandlerFactoryPluginInfo(XmlConfigFile config) {
    Node node = config.getNode("solr/shardHandlerFactory", false);
    return (node == null) ? null : new PluginInfo(node, "shardHandlerFactory", false, true);
  }

  private static PluginInfo[] getBackupRepositoryPluginInfos(XmlConfigFile config) {
    NodeList nodes = (NodeList) config.evaluate("solr/backup/repository", XPathConstants.NODESET);
    if (nodes == null || nodes.getLength() == 0)
      return new PluginInfo[0];
    PluginInfo[] configs = new PluginInfo[nodes.getLength()];
    for (int i = 0; i < nodes.getLength(); i++) {
      configs[i] = new PluginInfo(nodes.item(i), "BackupRepositoryFactory", true, true);
    }
    return configs;
  }

  private static MetricsConfig getMetricsConfig(XmlConfigFile config) {
    MetricsConfig.MetricsConfigBuilder builder = new MetricsConfig.MetricsConfigBuilder();
    Node node = config.getNode("solr/metrics/suppliers/counter", false);
    if (node != null) {
      builder = builder.setCounterSupplier(new PluginInfo(node, "counterSupplier", false, false));
    }
    node = config.getNode("solr/metrics/suppliers/meter", false);
    if (node != null) {
      builder = builder.setMeterSupplier(new PluginInfo(node, "meterSupplier", false, false));
    }
    node = config.getNode("solr/metrics/suppliers/timer", false);
    if (node != null) {
      builder = builder.setTimerSupplier(new PluginInfo(node, "timerSupplier", false, false));
    }
    node = config.getNode("solr/metrics/suppliers/histogram", false);
    if (node != null) {
      builder = builder.setHistogramSupplier(new PluginInfo(node, "histogramSupplier", false, false));
    }
    node = config.getNode("solr/metrics/history", false);
    if (node != null) {
      builder = builder.setHistoryHandler(new PluginInfo(node, "history", false, false));
    }
    PluginInfo[] reporterPlugins = getMetricReporterPluginInfos(config);
    Set<String> hiddenSysProps = getHiddenSysProps(config);
    return builder
        .setMetricReporterPlugins(reporterPlugins)
        .setHiddenSysProps(hiddenSysProps)
        .build();
  }

  private static PluginInfo[] getMetricReporterPluginInfos(XmlConfigFile config) {
    NodeList nodes = (NodeList) config.evaluate("solr/metrics/reporter", XPathConstants.NODESET);
    List<PluginInfo> configs = new ArrayList<>();
    boolean hasJmxReporter = false;
    if (nodes != null && nodes.getLength() > 0) {
      for (int i = 0; i < nodes.getLength(); i++) {
        // we don't require class in order to support predefined replica and node reporter classes
        PluginInfo info = new PluginInfo(nodes.item(i), "SolrMetricReporter", true, false);
        String clazz = info.className;
        if (clazz != null && clazz.equals(SolrJmxReporter.class.getName())) {
          hasJmxReporter = true;
        }
        configs.add(info);
      }
    }
    // if there's an MBean server running but there was no JMX reporter then add a default one
    MBeanServer mBeanServer = JmxUtil.findFirstMBeanServer();
    if (mBeanServer != null && !hasJmxReporter) {
      log.info("MBean server found: " + mBeanServer + ", but no JMX reporters were configured - adding default JMX reporter.");
      Map<String,Object> attributes = new HashMap<>();
      attributes.put("name", "default");
      attributes.put("class", SolrJmxReporter.class.getName());
      PluginInfo defaultPlugin = new PluginInfo("reporter", attributes);
      configs.add(defaultPlugin);
    }
    return configs.toArray(new PluginInfo[configs.size()]);
  }

  private static Set<String> getHiddenSysProps(XmlConfigFile config) {
    NodeList nodes = (NodeList) config.evaluate("solr/metrics/hiddenSysProps/str", XPathConstants.NODESET);
    if (nodes == null || nodes.getLength() == 0) {
      return NodeConfig.NodeConfigBuilder.DEFAULT_HIDDEN_SYS_PROPS;
    }
    Set<String> props = new HashSet<>();
    for (int i = 0; i < nodes.getLength(); i++) {
      String prop = DOMUtil.getText(nodes.item(i));
      if (prop != null && !prop.trim().isEmpty()) {
        props.add(prop.trim());
      }
    }
    if (props.isEmpty()) {
      return NodeConfig.NodeConfigBuilder.DEFAULT_HIDDEN_SYS_PROPS;
    } else {
      return props;
    }
  }

  private static PluginInfo getTransientCoreCacheFactoryPluginInfo(XmlConfigFile config) {
    Node node = config.getNode("solr/transientCoreCacheFactory", false);
    return (node == null) ? null : new PluginInfo(node, "transientCoreCacheFactory", false, true);
  }

  private static PluginInfo getTracerPluginInfo(XmlConfigFile config) {
    Node node = config.getNode("solr/tracerConfig", false);
    return (node == null) ? null : new PluginInfo(node, "tracerConfig", false, true);
  }
}
