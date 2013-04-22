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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.IOUtils;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.util.PropertiesUtil;
import org.apache.solr.util.SystemIdResolver;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;


/**
 *
 */
public class ConfigSolrXml extends ConfigSolr {
  protected static Logger log = LoggerFactory.getLogger(ConfigSolrXml.class);

  private final Map<String, CoreDescriptorPlus> coreDescriptorPlusMap = new HashMap<String, CoreDescriptorPlus>();

  public ConfigSolrXml(Config config, CoreContainer container)
      throws ParserConfigurationException, IOException, SAXException {
    super(config);
    init(container);
  }
  
  private void init(CoreContainer container) throws IOException {
    
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
    
    fillPropMap();
    initCoreList(container);
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

  private void initCoreList(CoreContainer container) throws IOException {
    if (container != null) { // TODO: 5.0. Yet another bit of nonsense only
                             // because of the test harness.
      synchronized (coreDescriptorPlusMap) {
        String coreRoot = get(CfgProp.SOLR_COREROOTDIRECTORY,
            container.getSolrHome());
        walkFromHere(new File(coreRoot), container,
            new HashMap<String,String>(), new HashMap<String,String>());
      }
    }
  }

  @Override
  public Map<String,String> readCoreAttributes(String coreName) {
    Map<String,String> attrs = new HashMap<String,String>();
    
    return attrs; // this is a no-op.... intentionally
  }

  // Basic recursive tree walking, looking for "core.properties" files. Once one is found, we'll stop going any
  // deeper in the tree.
  //
  // @param file - the directory we're to either read the properties file from or recurse into.
  private void walkFromHere(File file, CoreContainer container, Map<String, String> seenDirs, HashMap<String, String> seenCores)
      throws IOException {
    log.info("Looking for cores in " + file.getCanonicalPath());
    if (! file.exists()) return;

    for (File childFile : file.listFiles()) {
      // This is a little tricky, we are asking if core.properties exists in a child directory of the directory passed
      // in. In other words we're looking for core.properties in the grandchild directories of the parameter passed
      // in. That allows us to gracefully top recursing deep but continue looking wide.
      File propFile = new File(childFile, CORE_PROP_FILE);
      if (propFile.exists()) { // Stop looking after processing this file!
        addCore(container, seenDirs, seenCores, childFile, propFile);
        continue; // Go on to the sibling directory, don't descend any deeper.
      }
      if (childFile.isDirectory()) {
        walkFromHere(childFile, container, seenDirs, seenCores);
      }
    }
  }

  private void addCore(CoreContainer container, Map<String, String> seenDirs, Map<String, String> seenCores,
                       File childFile, File propFile) throws IOException {
    log.info("Discovered properties file {}, adding to cores", propFile.getAbsolutePath());
    Properties propsOrig = new Properties();
    InputStream is = new FileInputStream(propFile);
    try {
      propsOrig.load(is);
    } finally {
      IOUtils.closeQuietly(is);
    }

    Properties props = new Properties();
    for (String prop : propsOrig.stringPropertyNames()) {
      props.put(prop, PropertiesUtil.substituteProperty(propsOrig.getProperty(prop), null));
    }

    // Too much of the code depends on this value being here, but it is NOT supported in discovery mode, so
    // ignore it if present in the core.properties file.
    props.setProperty(CoreDescriptor.CORE_INSTDIR, childFile.getPath());

    if (props.getProperty(CoreDescriptor.CORE_NAME) == null) {
      // Should default to this directory
      props.setProperty(CoreDescriptor.CORE_NAME, childFile.getName());
    }
    CoreDescriptor desc = new CoreDescriptor(container, props);
    CoreDescriptorPlus plus = new CoreDescriptorPlus(propFile.getAbsolutePath(), desc, propsOrig);

    // It's bad to have two cores with the same name or same data dir.
    if (! seenCores.containsKey(desc.getName()) && ! seenDirs.containsKey(desc.getAbsoluteDataDir())) {
      coreDescriptorPlusMap.put(desc.getName(), plus);
      // Use the full path to the prop file so we can unambiguously report the place the error is.
      seenCores.put(desc.getName(), propFile.getAbsolutePath());
      seenDirs.put(desc.getAbsoluteDataDir(), propFile.getAbsolutePath());
      return;
    }

    // record the appropriate error
    if (seenCores.containsKey(desc.getName())) {
      String msg = String.format(Locale.ROOT, "More than one core defined for core named '%s', paths are '%s' and '%s'  Removing both cores.",
          desc.getName(), propFile.getAbsolutePath(), seenCores.get(desc.getName()));
      log.error(msg);
      // Load up as many errors as there are.
      if (badConfigCores.containsKey(desc.getName())) msg += " " + badConfigCores.get(desc.getName());
      badConfigCores.put(desc.getName(), msg);
    }
    // There's no reason both errors may not have occurred.
    if (seenDirs.containsKey(desc.getAbsoluteDataDir())) {
      String msg = String.format(Locale.ROOT, "More than one core points to data dir '%s'. They are in '%s' and '%s'. Removing all offending cores.",
          desc.getAbsoluteDataDir(), propFile.getAbsolutePath(), seenDirs.get(desc.getAbsoluteDataDir()));
      if (badConfigCores.containsKey(desc.getName())) msg += " " + badConfigCores.get(desc.getName());
      log.warn(msg);
    }
    coreDescriptorPlusMap.remove(desc.getName());
  }

  public IndexSchema getSchemaFromZk(ZkController zkController, String zkConfigName, String schemaName,
                                     SolrConfig config)
      throws KeeperException, InterruptedException {
    byte[] configBytes = zkController.getConfigFileData(zkConfigName, schemaName);
    InputSource is = new InputSource(new ByteArrayInputStream(configBytes));
    is.setSystemId(SystemIdResolver.createSystemIdFromResourceName(schemaName));
    IndexSchema schema = new IndexSchema(config, schemaName, is);
    return schema;
  }

  @Override
  public String getCoreNameFromOrig(String origCoreName,
      SolrResourceLoader loader, String coreName) {
    
    // first look for an exact match
    for (Map.Entry<String,CoreDescriptorPlus> ent : coreDescriptorPlusMap
        .entrySet()) {
      
      String name = ent.getValue().getCoreDescriptor()
          .getProperty(CoreDescriptor.CORE_NAME, null);
      if (origCoreName.equals(name)) {
        if (coreName.equals(origCoreName)) {
          return name;
        }
        return coreName;
      }
    }
    
    for (Map.Entry<String,CoreDescriptorPlus> ent : coreDescriptorPlusMap
        .entrySet()) {
      String name = ent.getValue().getCoreDescriptor()
          .getProperty(CoreDescriptor.CORE_NAME, null);
      // see if we match with substitution
      if (origCoreName.equals(PropertiesUtil.substituteProperty(name,
          loader.getCoreProperties()))) {
        if (coreName.equals(origCoreName)) {
          return name;
        }
        return coreName;
      }
    }
    return null;
  }

  @Override
  public List<String> getAllCoreNames() {
    List<String> ret = new ArrayList<String>(coreDescriptorPlusMap.keySet());
    
    return ret;
  }
  
  @Override
  public String getProperty(String coreName, String property, String defaultVal) {
    
    CoreDescriptorPlus plus = coreDescriptorPlusMap.get(coreName);
    if (plus == null) return defaultVal;
    CoreDescriptor desc = plus.getCoreDescriptor();
    if (desc == null) return defaultVal;
    return desc.getProperty(property, defaultVal);
    
  }

  @Override
  public Properties readCoreProperties(String coreName) {
    
    CoreDescriptorPlus plus = coreDescriptorPlusMap.get(coreName);
    if (plus == null) return null;
    return new Properties(plus.getCoreDescriptor().getCoreProperties());

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

