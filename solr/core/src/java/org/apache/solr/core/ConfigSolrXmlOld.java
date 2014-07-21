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

import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.util.DOMUtil;
import org.apache.solr.util.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;


/**
 *
 */
public class ConfigSolrXmlOld extends ConfigSolr {

  protected static Logger log = LoggerFactory.getLogger(ConfigSolrXmlOld.class);

  private NodeList coreNodes = null;
  
  private final CoresLocator persistor;

  public static final String DEFAULT_DEFAULT_CORE_NAME = "collection1";

  @Override
  protected String getShardHandlerFactoryConfigPath() {
    return "solr/cores/shardHandlerFactory";
  }

  public ConfigSolrXmlOld(Config config, String originalXML) {
    super(config);
    try {
      checkForIllegalConfig();
      fillPropMap();
      initCoreList();
      this.persistor = isPersistent() ? new SolrXMLCoresLocator(originalXML, this)
                                      : new SolrXMLCoresLocator.NonPersistingLocator(originalXML, this);
    }
    catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  @Override
  public CoresLocator getCoresLocator() {
    return this.persistor;
  }
  
  private void checkForIllegalConfig() throws IOException {
    // Do sanity checks - we don't want to find new style
    // config
    failIfFound("solr/str[@name='adminHandler']");
    failIfFound("solr/int[@name='coreLoadThreads']");
    failIfFound("solr/str[@name='coreRootDirectory']");
    failIfFound("solr/solrcloud/int[@name='distribUpdateConnTimeout']");
    failIfFound("solr/solrcloud/int[@name='distribUpdateSoTimeout']");
    failIfFound("solr/solrcloud/str[@name='host']");
    failIfFound("solr/solrcloud/str[@name='hostContext']");
    failIfFound("solr/solrcloud/int[@name='hostPort']");
    failIfFound("solr/solrcloud/int[@name='leaderVoteWait']");
    failIfFound("solr/solrcloud/int[@name='genericCoreNodeNames']");
    failIfFound("solr/str[@name='managementPath']");
    failIfFound("solr/str[@name='sharedLib']");
    failIfFound("solr/str[@name='shareSchema']");
    failIfFound("solr/int[@name='transientCacheSize']");
    failIfFound("solr/solrcloud/int[@name='zkClientTimeout']");
    failIfFound("solr/solrcloud/int[@name='zkHost']");
    
    failIfFound("solr/logging/str[@name='class']");
    failIfFound("solr/logging/str[@name='enabled']");
    
    failIfFound("solr/logging/watcher/int[@name='size']");
    failIfFound("solr/logging/watcher/int[@name='threshold']");
  }
  
  private void failIfFound(String xPath) {

    if (config.getVal(xPath, false) != null) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Should not have found " + xPath +
          " solr.xml may be a mix of old and new style formats.");
    }
  }

  @Override
  public boolean isPersistent() {
    return config.getBool("solr/@persistent", false);
  }

  @Override
  public String getDefaultCoreName() {
    return get(CfgProp.SOLR_CORES_DEFAULT_CORE_NAME, DEFAULT_DEFAULT_CORE_NAME);
  }
  
  private void fillPropMap() {
    storeConfigPropertyAsInt(CfgProp.SOLR_CORELOADTHREADS, "solr/@coreLoadThreads");
    storeConfigPropertyAsString(CfgProp.SOLR_SHAREDLIB, "solr/@sharedLib");
    storeConfigPropertyAsString(CfgProp.SOLR_ZKHOST, "solr/@zkHost");
    storeConfigPropertyAsString(CfgProp.SOLR_LOGGING_CLASS, "solr/logging/@class");
    storeConfigPropertyAsBoolean(CfgProp.SOLR_LOGGING_ENABLED, "solr/logging/@enabled");
    storeConfigPropertyAsInt(CfgProp.SOLR_LOGGING_WATCHER_SIZE, "solr/logging/watcher/@size");
    storeConfigPropertyAsString(CfgProp.SOLR_LOGGING_WATCHER_THRESHOLD, "solr/logging/watcher/@threshold");
    storeConfigPropertyAsString(CfgProp.SOLR_ADMINHANDLER, "solr/cores/@adminHandler");
    storeConfigPropertyAsString(CfgProp.SOLR_COLLECTIONSHANDLER, "solr/cores/@collectionsHandler");
    storeConfigPropertyAsString(CfgProp.SOLR_INFOHANDLER, "solr/cores/@infoHandler");
    storeConfigPropertyAsInt(CfgProp.SOLR_DISTRIBUPDATECONNTIMEOUT, "solr/cores/@distribUpdateConnTimeout");
    storeConfigPropertyAsInt(CfgProp.SOLR_DISTRIBUPDATESOTIMEOUT, "solr/cores/@distribUpdateSoTimeout");
    storeConfigPropertyAsInt(CfgProp.SOLR_MAXUPDATECONNECTIONS, "solr/cores/@maxUpdateConnections");
    storeConfigPropertyAsInt(CfgProp.SOLR_MAXUPDATECONNECTIONSPERHOST, "solr/cores/@maxUpdateConnectionsPerHost");
    storeConfigPropertyAsString(CfgProp.SOLR_HOST, "solr/cores/@host");
    storeConfigPropertyAsString(CfgProp.SOLR_HOSTCONTEXT, "solr/cores/@hostContext");
    storeConfigPropertyAsString(CfgProp.SOLR_HOSTPORT, "solr/cores/@hostPort");
    storeConfigPropertyAsInt(CfgProp.SOLR_LEADERVOTEWAIT, "solr/cores/@leaderVoteWait");
    storeConfigPropertyAsBoolean(CfgProp.SOLR_GENERICCORENODENAMES, "solr/cores/@genericCoreNodeNames");
    storeConfigPropertyAsString(CfgProp.SOLR_MANAGEMENTPATH, "solr/cores/@managementPath");
    storeConfigPropertyAsBoolean(CfgProp.SOLR_SHARESCHEMA, "solr/cores/@shareSchema");
    storeConfigPropertyAsInt(CfgProp.SOLR_TRANSIENTCACHESIZE, "solr/cores/@transientCacheSize");
    storeConfigPropertyAsInt(CfgProp.SOLR_ZKCLIENTTIMEOUT, "solr/cores/@zkClientTimeout");
    storeConfigPropertyAsString(CfgProp.SOLR_CONFIGSETBASEDIR, "solr/cores/@configSetBaseDir");

    // These have no counterpart in 5.0, asking, for any of these in Solr 5.0
    // will result in an error being
    // thrown.
    storeConfigPropertyAsString(CfgProp.SOLR_CORES_DEFAULT_CORE_NAME, "solr/cores/@defaultCoreName");
    storeConfigPropertyAsString(CfgProp.SOLR_PERSISTENT, "solr/@persistent");
    storeConfigPropertyAsString(CfgProp.SOLR_ADMINPATH, "solr/cores/@adminPath");
  }

  private void storeConfigPropertyAsInt(CfgProp key, String xmlPath) {
    String valueAsString = config.getVal(xmlPath, false);
    if (StringUtils.isNotBlank(valueAsString)) {
      propMap.put(key, Integer.parseInt(valueAsString));
    } else {
      propMap.put(key, null);
    }
  }

  private void storeConfigPropertyAsBoolean(CfgProp key, String xmlPath) {
    String valueAsString = config.getVal(xmlPath, false);
    if (StringUtils.isNotBlank(valueAsString)) {
      propMap.put(key, Boolean.parseBoolean(valueAsString));
    } else {
      propMap.put(key, null);
    }
  }

  private void storeConfigPropertyAsString(CfgProp key, String xmlPath) {
    propMap.put(key, config.getVal(xmlPath, false));
  }

  private void initCoreList() throws IOException {
    
    coreNodes = (NodeList) config.evaluate("solr/cores/core",
        XPathConstants.NODESET);
    // Check a couple of error conditions
    Set<String> names = new HashSet<>(); // for duplicate names
    Map<String,String> dirs = new HashMap<>(); // for duplicate
                                                            // data dirs.
    
    for (int idx = 0; idx < coreNodes.getLength(); ++idx) {
      Node node = coreNodes.item(idx);
      String name = DOMUtil.getAttr(node, CoreDescriptor.CORE_NAME, null);

      String dataDir = DOMUtil.getAttr(node, CoreDescriptor.CORE_DATADIR, null);
      if (name != null) {
        if (!names.contains(name)) {
          names.add(name);
        } else {
          String msg = String.format(Locale.ROOT,
              "More than one core defined for core named %s", name);
          log.error(msg);
        }
      }

      String instDir = DOMUtil.getAttr(node, CoreDescriptor.CORE_INSTDIR, null);

      if (dataDir != null) {
        String absData = null;
        File dataFile = new File(dataDir);
        if (dataFile.isAbsolute()) {
          absData = dataFile.getCanonicalPath();
        } else if (instDir != null) {
          File instFile = new File(instDir);
          absData = new File(instFile, dataDir).getCanonicalPath();
        }
        if (absData != null) {
          if (!dirs.containsKey(absData)) {
            dirs.put(absData, name);
          } else {
            String msg = String
                .format(
                    Locale.ROOT,
                    "More than one core points to data dir %s. They are in %s and %s",
                    absData, dirs.get(absData), name);
            log.warn(msg);
          }
        }
      }
    }
    
  }

  public List<String> getAllCoreNames() {
    List<String> ret = new ArrayList<>();
    
    synchronized (coreNodes) {
      for (int idx = 0; idx < coreNodes.getLength(); ++idx) {
        Node node = coreNodes.item(idx);
        ret.add(DOMUtil.getAttr(node, CoreDescriptor.CORE_NAME, null));
      }
    }
    
    return ret;
  }

  public String getProperty(String coreName, String property, String defaultVal) {
    
    synchronized (coreNodes) {
      for (int idx = 0; idx < coreNodes.getLength(); ++idx) {
        Node node = coreNodes.item(idx);
        if (coreName.equals(DOMUtil.getAttr(node, CoreDescriptor.CORE_NAME,
            null))) {
          String propVal = DOMUtil.getAttr(node, property);
          if (propVal == null)
            propVal = defaultVal;
          return propVal;
        }
      }
    }
    return defaultVal;
    
  }

  public Properties getCoreProperties(String coreName) {
    synchronized (coreNodes) {
      for (int idx = 0; idx < coreNodes.getLength(); idx++) {
        Node node = coreNodes.item(idx);
        if (coreName.equals(DOMUtil.getAttr(node, CoreDescriptor.CORE_NAME, null))) {
          try {
            return readProperties(node);
          } catch (XPathExpressionException e) {
            SolrException.log(log, e);
          }
        }
      }
    }
    return new Properties();
  }
}
