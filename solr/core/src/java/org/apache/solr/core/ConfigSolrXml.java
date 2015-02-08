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

import com.google.common.base.Function;
import com.google.common.base.Functions;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.DOMUtil;
import org.apache.solr.util.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;


/**
 *
 */
public class ConfigSolrXml extends ConfigSolr {

  protected static Logger log = LoggerFactory.getLogger(ConfigSolrXml.class);

  private final CoresLocator coresLocator;
  private final Config config;
  private final Map<CfgProp, Object> propMap = new HashMap<>();

  public ConfigSolrXml(Config config) {
    super(config.getResourceLoader(), loadProperties(config));
    this.config = config;
    this.config.substituteProperties();
    checkForIllegalConfig();
    fillPropMap();
    coresLocator = new CorePropertiesLocator(getCoreRootDirectory());
  }

  private void checkForIllegalConfig() {

    failIfFound("solr/@coreLoadThreads");
    failIfFound("solr/@persistent");
    failIfFound("solr/@sharedLib");
    failIfFound("solr/@zkHost");

    failIfFound("solr/cores");

    failIfFound("solr/@persistent");

  }

  private void failIfFound(String xPath) {

    if (config.getVal(xPath, false) != null) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Should not have found " + xPath +
          "\n. Please upgrade your solr.xml: https://cwiki.apache.org/confluence/display/solr/Format+of+solr.xml");
    }
  }

  protected String getProperty(CfgProp key) {
    if (!propMap.containsKey(key) || propMap.get(key) == null)
      return null;
    return propMap.get(key).toString();
  }

  private static Properties loadProperties(Config config) {
    try {
      Node node = ((NodeList) config.evaluate("solr", XPathConstants.NODESET)).item(0);
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
    catch (XPathExpressionException e) {
      log.warn("Error parsing solr.xml: " + e.getMessage());
      return null;
    }
  }

  private NamedList<Object> readNodeListAsNamedList(String path) {
    NodeList nodes = config.getNodeList(path, false);
    if (nodes != null) {
      NamedList<Object> namedList = DOMUtil.nodesToNamedList(nodes);
      return namedList;
    }
    return new NamedList<>();
  }

  private void fillPropMap() {
    NamedList<Object> unknownConfigParams = new NamedList<>();

    // shardHandlerFactory is parsed differently in the base class as a plugin, so we're excluding this node from the node list
    fillSolrSection(readNodeListAsNamedList("solr/*[@name][not(name()='shardHandlerFactory')]"));

    thereCanBeOnlyOne("solr/solrcloud","<solrcloud>");
    fillSolrCloudSection(readNodeListAsNamedList("solr/solrcloud/*[@name]"));

    thereCanBeOnlyOne("solr/logging","<logging>");
    thereCanBeOnlyOne("solr/logging/watcher","Logging <watcher>");
    fillLoggingSection(readNodeListAsNamedList("solr/logging/*[@name]"), 
                       readNodeListAsNamedList("solr/logging/watcher/*[@name]"));
  }

  private void fillSolrSection(NamedList<Object> nl) {
    String s = "Main";
    storeConfigPropertyAsString(s, nl, CfgProp.SOLR_ADMINHANDLER, "adminHandler");
    storeConfigPropertyAsString(s, nl, CfgProp.SOLR_COLLECTIONSHANDLER, "collectionsHandler");
    storeConfigPropertyAsString(s, nl, CfgProp.SOLR_INFOHANDLER, "infoHandler");
    storeConfigPropertyAsString(s, nl, CfgProp.SOLR_COREROOTDIRECTORY, "coreRootDirectory");
    storeConfigPropertyAsString(s, nl, CfgProp.SOLR_MANAGEMENTPATH, "managementPath");
    storeConfigPropertyAsString(s, nl, CfgProp.SOLR_SHAREDLIB, "sharedLib");
    storeConfigPropertyAsString(s, nl, CfgProp.SOLR_CONFIGSETBASEDIR, "configSetBaseDir");

    storeConfigPropertyAsBoolean(s, nl, CfgProp.SOLR_SHARESCHEMA, "shareSchema");

    storeConfigPropertyAsInt(s, nl, CfgProp.SOLR_CORELOADTHREADS, "coreLoadThreads");
    storeConfigPropertyAsInt(s, nl, CfgProp.SOLR_TRANSIENTCACHESIZE, "transientCacheSize");

    errorOnLeftOvers(s, nl);
  }

  private void fillSolrCloudSection(NamedList<Object> nl) {
    
    String s = "<solrcloud>";
    storeConfigPropertyAsInt(s, nl, CfgProp.SOLR_DISTRIBUPDATECONNTIMEOUT, "distribUpdateConnTimeout");
    storeConfigPropertyAsInt(s, nl, CfgProp.SOLR_DISTRIBUPDATESOTIMEOUT, "distribUpdateSoTimeout");
    storeConfigPropertyAsInt(s, nl, CfgProp.SOLR_MAXUPDATECONNECTIONS, "maxUpdateConnections");
    storeConfigPropertyAsInt(s, nl, CfgProp.SOLR_MAXUPDATECONNECTIONSPERHOST, "maxUpdateConnectionsPerHost");
    storeConfigPropertyAsInt(s, nl, CfgProp.SOLR_LEADERVOTEWAIT, "leaderVoteWait");
    storeConfigPropertyAsInt(s, nl, CfgProp.SOLR_LEADERCONFLICTRESOLVEWAIT, "leaderConflictResolveWait");
    storeConfigPropertyAsInt(s, nl, CfgProp.SOLR_ZKCLIENTTIMEOUT, "zkClientTimeout");
    storeConfigPropertyAsInt(s, nl, CfgProp.SOLR_AUTOREPLICAFAILOVERBADNODEEXPIRATION, "autoReplicaFailoverBadNodeExpiration");
    storeConfigPropertyAsInt(s, nl, CfgProp.SOLR_AUTOREPLICAFAILOVERWAITAFTEREXPIRATION, "autoReplicaFailoverWaitAfterExpiration");
    storeConfigPropertyAsInt(s, nl, CfgProp.SOLR_AUTOREPLICAFAILOVERWORKLOOPDELAY, "autoReplicaFailoverWorkLoopDelay");
    
    storeConfigPropertyAsString(s, nl, CfgProp.SOLR_HOST, "host");
    storeConfigPropertyAsString(s, nl, CfgProp.SOLR_HOSTCONTEXT, "hostContext");
    storeConfigPropertyAsString(s, nl, CfgProp.SOLR_HOSTPORT, "hostPort");
    storeConfigPropertyAsString(s, nl, CfgProp.SOLR_ZKHOST, "zkHost");

    storeConfigPropertyAsBoolean(s, nl, CfgProp.SOLR_GENERICCORENODENAMES, "genericCoreNodeNames");
    
    storeConfigPropertyAsString(s, nl, CfgProp.SOLR_ZKACLPROVIDER, "zkACLProvider");
    storeConfigPropertyAsString(s, nl, CfgProp.SOLR_ZKCREDENTIALSPROVIDER, "zkCredentialsProvider");
    
    errorOnLeftOvers(s, nl);
  }

  private void fillLoggingSection(NamedList<Object> loggingConfig, 
                                  NamedList<Object> loggingWatcherConfig) {
    
    String s = "<logging>";
    storeConfigPropertyAsString(s, loggingConfig, CfgProp.SOLR_LOGGING_CLASS, "class");
    storeConfigPropertyAsBoolean(s, loggingConfig, CfgProp.SOLR_LOGGING_ENABLED, "enabled");

    errorOnLeftOvers(s, loggingConfig);

    s = "Logging <watcher>";
    storeConfigPropertyAsInt(s, loggingWatcherConfig, CfgProp.SOLR_LOGGING_WATCHER_SIZE, "size");
    storeConfigPropertyAsString(s, loggingWatcherConfig, CfgProp.SOLR_LOGGING_WATCHER_THRESHOLD, "threshold");

    errorOnLeftOvers(s, loggingWatcherConfig);
  }


  private <T> void storeConfigProperty(String section, NamedList<Object> config, CfgProp propertyKey, String name, Function<Object, T> valueTransformer, Class<T> clazz) {
    List<Object> values = config.removeAll(name); 
    if (null != values && 0 != values.size()) {
      if (1 < values.size()) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                                String.format(Locale.ROOT, 
                                              "%s section of solr.xml contains duplicated '%s'"+
                                              " in solr.xml: %s", section, name, values));
      } else {
        Object value = values.get(0);
        if (value != null) {
          if (value.getClass().isAssignableFrom(clazz)) {
            propMap.put(propertyKey, value);
          } else {
            propMap.put(propertyKey, valueTransformer.apply(value));
          }
        } else {
          propMap.put(propertyKey, null);
        }
      }
    }
  }

  private void storeConfigPropertyAsString(String section, NamedList<Object> config, CfgProp propertyKey, String name) {
    storeConfigProperty(section, config, propertyKey, name, Functions.toStringFunction(), String.class);
  }

  private void storeConfigPropertyAsInt(String section, NamedList<Object> config, CfgProp propertyKey, String xmlElementName) {
    storeConfigProperty(section, config, propertyKey, xmlElementName, TO_INT_FUNCTION, Integer.class);
  }

  private void storeConfigPropertyAsBoolean(String section, NamedList<Object> config, CfgProp propertyKey, String name) {
    storeConfigProperty(section, config, propertyKey, name, TO_BOOLEAN_FUNCTION, Boolean.class);
  }

  /** throws an error if more then one element matching the xpath */
  private void thereCanBeOnlyOne(String xpath, String section) {
    NodeList lst = config.getNodeList(xpath, false);
    if (1 < lst.getLength())
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                              lst.getLength() + " instances of " + section + " found in solr.xml");
  }

  /** logs each item in leftovers and then throws an exception with a summary */
  private void errorOnLeftOvers(String section, NamedList<Object> leftovers) {

    if (null == leftovers || 0 == leftovers.size()) return;

    List<String> unknownElements = new ArrayList<String>(leftovers.size());
    for (Map.Entry<String, Object> unknownElement : leftovers) {
      log.error("Unknown config parameter in {} section of solr.xml: {} -> {}", 
                section, unknownElement.getKey(), unknownElement.getValue());
      unknownElements.add(unknownElement.getKey());
    }
    if (! unknownElements.isEmpty() ) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                              String.format(Locale.ROOT, "%s section of solr.xml contains %d unknown config parameter(s): %s", section, unknownElements.size(), unknownElements));
    }
  }

  public PluginInfo getShardHandlerFactoryPluginInfo() {
    Node node = config.getNode("solr/shardHandlerFactory", false);
    return (node == null) ? null : new PluginInfo(node, "shardHandlerFactory", false, true);
  }

  @Override
  public CoresLocator getCoresLocator() {
    return coresLocator;
  }

  private static final Function<Map.Entry<String, Object>, String> GET_KEY_FUNCTION = new Function<Map.Entry<String, Object>, String>() {
    @Override
    public String apply(Map.Entry<String, Object> input) {
      return input.getKey();
    }
  };

  private static final Function<Object, Integer> TO_INT_FUNCTION = new Function<Object, Integer>() {
    @Override
    public Integer apply(Object input) {
      try {
        return Integer.parseInt(String.valueOf(input));
      } catch (NumberFormatException exc) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, 
                                String.format(Locale.ROOT, 
                                              "Value of '%s' can not be parsed as 'int'", input));
      }
    }
  };

  private static final Function<Object, Boolean> TO_BOOLEAN_FUNCTION = new Function<Object, Boolean>() {
    @Override
    public Boolean apply(Object input) {
      if (input instanceof String) {
        return Boolean.valueOf(String.valueOf(input));
      }
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, String.format(Locale.ROOT, "Value of '%s' can not be parsed as 'bool'", input));
    }
  };
}

