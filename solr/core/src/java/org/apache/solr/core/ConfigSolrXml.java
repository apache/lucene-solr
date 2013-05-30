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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.IOUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.util.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;


/**
 *
 */
public class ConfigSolrXml extends ConfigSolr {
  protected static Logger log = LoggerFactory.getLogger(ConfigSolrXml.class);

  private SolrCoreDiscoverer solrCoreDiscoverer = new SolrCoreDiscoverer();
  private final Map<String, CoreDescriptor> coreDescriptorMap;

  public ConfigSolrXml(Config config, CoreContainer container)
      throws ParserConfigurationException, IOException, SAXException {
    super(config);
    checkForIllegalConfig();
    
    fillPropMap();
    
    String coreRoot = get(CfgProp.SOLR_COREROOTDIRECTORY, (container == null ? config.getResourceLoader().getInstanceDir() : container.getSolrHome()));
    coreDescriptorMap = solrCoreDiscoverer.discover(container, new File(coreRoot));
  }
  
  private void checkForIllegalConfig() throws IOException {
    
    // Do sanity checks - we don't want to find old style config
    failIfFound("solr/@coreLoadThreads");
    failIfFound("solr/@persist");
    failIfFound("solr/@sharedLib");
    failIfFound("solr/@zkHost");
    
    failIfFound("solr/logging/@class");
    failIfFound("solr/logging/@enabled");
    failIfFound("solr/logging/watcher/@size");
    failIfFound("solr/logging/watcher/@threshold");
    
    failIfFound("solr/cores/@adminHandler");
    failIfFound("solr/cores/@distribUpdateConnTimeout");
    failIfFound("solr/cores/@distribUpdateSoTimeout");
    failIfFound("solr/cores/@host");
    failIfFound("solr/cores/@hostContext");
    failIfFound("solr/cores/@hostPort");
    failIfFound("solr/cores/@leaderVoteWait");
    failIfFound("solr/cores/@managementPath");
    failIfFound("solr/cores/@shareSchema");
    failIfFound("solr/cores/@transientCacheSize");
    failIfFound("solr/cores/@zkClientTimeout");
    
    // These have no counterpart in 5.0, asking for any of these in Solr 5.0
    // will result in an error being
    // thrown.
    failIfFound("solr/cores/@defaultCoreName");
    failIfFound("solr/@persistent");
    failIfFound("solr/cores/@adminPath");

  }
  
  private void failIfFound(String xPath) {

    if (config.getVal(xPath, false) != null) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Should not have found " + xPath +
          " solr.xml may be a mix of old and new style formats.");
    }
  }

  // We can do this in 5.0 when we read the solr.xml since we don't need to keep the original around for persistence.
  private String doSub(String path) {
    String val = config.getVal(path, false);
    if (val != null) {
      val = PropertiesUtil.substituteProperty(val, null);
    }
    return val;
  }
  
  private void fillPropMap() {
    propMap.put(CfgProp.SOLR_ADMINHANDLER, doSub("solr/str[@name='adminHandler']"));
    propMap.put(CfgProp.SOLR_CORELOADTHREADS, doSub("solr/int[@name='coreLoadThreads']"));
    propMap.put(CfgProp.SOLR_COREROOTDIRECTORY, doSub("solr/str[@name='coreRootDirectory']"));
    propMap.put(CfgProp.SOLR_DISTRIBUPDATECONNTIMEOUT, doSub("solr/solrcloud/int[@name='distribUpdateConnTimeout']"));
    propMap.put(CfgProp.SOLR_DISTRIBUPDATESOTIMEOUT, doSub("solr/solrcloud/int[@name='distribUpdateSoTimeout']"));
    propMap.put(CfgProp.SOLR_HOST, doSub("solr/solrcloud/str[@name='host']"));
    propMap.put(CfgProp.SOLR_HOSTCONTEXT, doSub("solr/solrcloud/str[@name='hostContext']"));
    propMap.put(CfgProp.SOLR_HOSTPORT, doSub("solr/solrcloud/int[@name='hostPort']"));
    propMap.put(CfgProp.SOLR_LEADERVOTEWAIT, doSub("solr/solrcloud/int[@name='leaderVoteWait']"));
    propMap.put(CfgProp.SOLR_MANAGEMENTPATH, doSub("solr/str[@name='managementPath']"));
    propMap.put(CfgProp.SOLR_SHAREDLIB, doSub("solr/str[@name='sharedLib']"));
    propMap.put(CfgProp.SOLR_SHARESCHEMA, doSub("solr/str[@name='shareSchema']"));
    propMap.put(CfgProp.SOLR_TRANSIENTCACHESIZE, doSub("solr/int[@name='transientCacheSize']"));
    propMap.put(CfgProp.SOLR_ZKCLIENTTIMEOUT, doSub("solr/solrcloud/int[@name='zkClientTimeout']"));
    propMap.put(CfgProp.SOLR_ZKHOST, doSub("solr/solrcloud/str[@name='zkHost']"));

    propMap.put(CfgProp.SOLR_LOGGING_CLASS, doSub("solr/logging/str[@name='class']"));
    propMap.put(CfgProp.SOLR_LOGGING_ENABLED, doSub("solr/logging/str[@name='enabled']"));
    propMap.put(CfgProp.SOLR_LOGGING_WATCHER_SIZE, doSub("solr/logging/watcher/int[@name='size']"));
    propMap.put(CfgProp.SOLR_LOGGING_WATCHER_THRESHOLD, doSub("solr/logging/watcher/int[@name='threshold']"));
    propMap.put(CfgProp.SOLR_SHARDHANDLERFACTORY_CLASS, doSub("solr/shardHandlerFactory/@class"));
    propMap.put(CfgProp.SOLR_SHARDHANDLERFACTORY_NAME, doSub("solr/shardHandlerFactory/@name"));
    propMap.put(CfgProp.SOLR_SHARDHANDLERFACTORY_CONNTIMEOUT, doSub("solr/shardHandlerFactory/int[@name='connTimeout']"));
    propMap.put(CfgProp.SOLR_SHARDHANDLERFACTORY_SOCKETTIMEOUT, doSub("solr/shardHandlerFactory/int[@name='socketTimeout']"));
  }



  @Override
  public Map<String,String> readCoreAttributes(String coreName) {
    Map<String,String> attrs = new HashMap<String,String>();
    
    return attrs; // this is a no-op.... intentionally
  }

  @Override
  public List<String> getAllCoreNames() {
    List<String> ret = new ArrayList<String>(coreDescriptorMap.keySet());
    
    return ret;
  }
  
  @Override
  public String getProperty(String coreName, String property, String defaultVal) {
    CoreDescriptor cd = coreDescriptorMap.get(coreName);
    if (cd == null) return defaultVal;

    return cd.getProperty(property, defaultVal);
  }

  @Override
  public Properties readCoreProperties(String coreName) {
    CoreDescriptor cd = coreDescriptorMap.get(coreName);
    if (cd == null) return null;
    return new Properties(cd.getCoreProperties());
  }

  static Properties getCoreProperties(String instanceDir, CoreDescriptor dcore) {
    String file = dcore.getPropertiesName();
    if (file == null) file = "conf" + File.separator + "solrcore.properties";
    File corePropsFile = new File(file);
    if (!corePropsFile.isAbsolute()) {
      corePropsFile = new File(instanceDir, file);
    }
    Properties p = dcore.getCoreProperties();
    if (corePropsFile.exists() && corePropsFile.isFile()) {
      p = new Properties(dcore.getCoreProperties());
      InputStream is = null;
      try {
        is = new FileInputStream(corePropsFile);
        p.load(is);
      } catch (IOException e) {
        log.warn("Error loading properties ", e);
      } finally {
        IOUtils.closeQuietly(is);
      }
    }
    return p;
  }

  @Override
  public void substituteProperties() {
    config.substituteProperties();
  }

}

