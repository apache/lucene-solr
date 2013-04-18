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
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.handler.component.HttpShardHandlerFactory;
import org.apache.solr.handler.component.ShardHandlerFactory;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.util.DOMUtil;
import org.apache.solr.util.PropertiesUtil;
import org.apache.solr.util.SystemIdResolver;
import org.apache.solr.util.plugin.PluginInfoInitialized;
import org.apache.zookeeper.KeeperException;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * ConfigSolrXml
 * <p/>
 * This class is entirely to localize the dealing with specific issues when transitioning from old-style solr.xml
 * to new-style xml, enumeration/discovery of defined cores. See SOLR-4196 for background.
 * <p/>
 * @since solr 4.3
 *
 * It's a bit twisted, but we decided to NOT do the solr.properties switch. But since there's already an interface
 * it makes sense to leave it in so we can use other methods of providing the Solr information that is contained
 * in solr.xml. Perhaps something form SolrCloud in the future?
 *
 */


public class ConfigSolrXml extends Config implements ConfigSolr {

  private boolean is50OrLater = false;

  private final Map<String, CoreDescriptorPlus> coreDescriptorPlusMap = new HashMap<String, CoreDescriptorPlus>();
  private NodeList coreNodes = null;
  private final Map<String, String> badConfigCores = new HashMap<String, String>();
    // List of cores that we should _never_ load. Ones with dup names or duplicate datadirs or...


  private Map<CfgProp, String> propMap = new HashMap<CfgProp, String>();

  public ConfigSolrXml(SolrResourceLoader loader, String name, InputStream is, String prefix,
                       boolean subProps, CoreContainer container)
      throws ParserConfigurationException, IOException, SAXException {

    super(loader, name, new InputSource(is), prefix, subProps);
    init(container);
  }


  public ConfigSolrXml(SolrResourceLoader loader, Config cfg, CoreContainer container)
      throws TransformerException, IOException {

    super(loader, null, copyDoc(cfg.getDocument())); // Mimics a call from CoreContainer.
    init(container);
  }
  
  private void init(CoreContainer container) throws IOException {
    is50OrLater = getNode("solr/cores", false) == null;

    // Do sanity checks, old and new style. Pretty exhaustive for now, but want to hammer this.
    // TODO: 5.0 maybe remove this checking, it's mostly for correctness as we make this transition.

    if (is50OrLater()) {
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

      // These have no counterpart in 5.0, asking for any of these in Solr 5.0 will result in an error being
      // thrown.
      failIfFound("solr/cores/@defaultCoreName");
      failIfFound("solr/@persistent");
      failIfFound("solr/cores/@adminPath");
    } else {
      failIfFound("solr/str[@name='adminHandler']");
      failIfFound("solr/int[@name='coreLoadThreads']");
      failIfFound("solr/str[@name='coreRootDirectory']");
      failIfFound("solr/solrcloud/int[@name='distribUpdateConnTimeout']");
      failIfFound("solr/solrcloud/int[@name='distribUpdateSoTimeout']");
      failIfFound("solr/solrcloud/str[@name='host']");
      failIfFound("solr/solrcloud/str[@name='hostContext']");
      failIfFound("solr/solrcloud/int[@name='hostPort']");
      failIfFound("solr/solrcloud/int[@name='leaderVoteWait']");
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
    fillPropMap();
    initCoreList(container);
  }
  private void failIfFound(String xPath) {

    if (getVal(xPath, false) != null) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Should not have found " + xPath +
          " solr.xml may be a mix of old and new style formats.");
    }
  }

  // We can do this in 5.0 when we read the solr.xml since we don't need to keep the original around for persistence.
  private String doSub(String path) {
    String val = getVal(path, false);
    if (val != null) {
      val = PropertiesUtil.substituteProperty(val, null);
    }
    return val;
  }
  
  private void fillPropMap() {
    if (is50OrLater) { // Can do the prop subs early here since we don't need to preserve them for persistence.
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
    } else {
      propMap.put(CfgProp.SOLR_CORELOADTHREADS, getVal("solr/@coreLoadThreads", false));
      propMap.put(CfgProp.SOLR_SHAREDLIB, getVal("solr/@sharedLib", false));
      propMap.put(CfgProp.SOLR_ZKHOST, getVal("solr/@zkHost", false));

      propMap.put(CfgProp.SOLR_LOGGING_CLASS, getVal("solr/logging/@class", false));
      propMap.put(CfgProp.SOLR_LOGGING_ENABLED, getVal("solr/logging/@enabled", false));
      propMap.put(CfgProp.SOLR_LOGGING_WATCHER_SIZE, getVal("solr/logging/watcher/@size", false));
      propMap.put(CfgProp.SOLR_LOGGING_WATCHER_THRESHOLD, getVal("solr/logging/watcher/@threshold", false));

      propMap.put(CfgProp.SOLR_ADMINHANDLER, getVal("solr/cores/@adminHandler", false));
      propMap.put(CfgProp.SOLR_DISTRIBUPDATECONNTIMEOUT, getVal("solr/cores/@distribUpdateConnTimeout", false));
      propMap.put(CfgProp.SOLR_DISTRIBUPDATESOTIMEOUT, getVal("solr/cores/@distribUpdateSoTimeout", false));
      propMap.put(CfgProp.SOLR_HOST, getVal("solr/cores/@host", false));
      propMap.put(CfgProp.SOLR_HOSTCONTEXT, getVal("solr/cores/@hostContext", false));
      propMap.put(CfgProp.SOLR_HOSTPORT, getVal("solr/cores/@hostPort", false));
      propMap.put(CfgProp.SOLR_LEADERVOTEWAIT, getVal("solr/cores/@leaderVoteWait", false));
      propMap.put(CfgProp.SOLR_MANAGEMENTPATH, getVal("solr/cores/@managementPath", false));
      propMap.put(CfgProp.SOLR_SHARESCHEMA, getVal("solr/cores/@shareSchema", false));
      propMap.put(CfgProp.SOLR_TRANSIENTCACHESIZE, getVal("solr/cores/@transientCacheSize", false));
      propMap.put(CfgProp.SOLR_ZKCLIENTTIMEOUT, getVal("solr/cores/@zkClientTimeout", false));
      propMap.put(CfgProp.SOLR_SHARDHANDLERFACTORY_CLASS, getVal("solr/shardHandlerFactory/@class", false));
      propMap.put(CfgProp.SOLR_SHARDHANDLERFACTORY_NAME, getVal("solr/shardHandlerFactory/@name", false));
      propMap.put(CfgProp.SOLR_SHARDHANDLERFACTORY_CONNTIMEOUT, getVal(  "solr/shardHandlerFactory/int[@connTimeout]", false));
      propMap.put(CfgProp.SOLR_SHARDHANDLERFACTORY_SOCKETTIMEOUT, getVal("solr/shardHandlerFactory/int[@socketTimeout]", false));

      // These have no counterpart in 5.0, asking, for any of these in Solr 5.0 will result in an error being
      // thrown.
      propMap.put(CfgProp.SOLR_CORES_DEFAULT_CORE_NAME, getVal("solr/cores/@defaultCoreName", false));
      propMap.put(CfgProp.SOLR_PERSISTENT, getVal("solr/@persistent", false));
      propMap.put(CfgProp.SOLR_ADMINPATH, getVal("solr/cores/@adminPath", false));
    }
  }

  private void initCoreList(CoreContainer container) throws IOException {
    if (is50OrLater) {
      if (container != null) { //TODO: 5.0. Yet another bit of nonsense only because of the test harness.
        synchronized (coreDescriptorPlusMap) {
          String coreRoot = get(CfgProp.SOLR_COREROOTDIRECTORY, container.getSolrHome());
          walkFromHere(new File(coreRoot), container, new HashMap<String, String>(), new HashMap<String, String>());
        }
      }
    } else {
      coreNodes = (NodeList) evaluate("solr/cores/core",
          XPathConstants.NODESET);
      // Check a couple of error conditions
      Set<String> names = new HashSet<String>(); // for duplicate names
      Map<String, String> dirs = new HashMap<String, String>(); // for duplicate data dirs.

      for (int idx = 0; idx < coreNodes.getLength(); ++idx) {
        Node node = coreNodes.item(idx);
        String name = DOMUtil.getAttr(node,  CoreDescriptor.CORE_NAME, null);
        String dataDir = DOMUtil.getAttr(node,  CoreDescriptor.CORE_DATADIR, null);
        if (name != null) {
          if (! names.contains(name)) {
            names.add(name);
          } else {
            String msg = String.format(Locale.ROOT, "More than one core defined for core named %s", name);
            log.error(msg);
          }
        }

        if (dataDir != null) {
          if (! dirs.containsKey(dataDir)) {
            dirs.put(dataDir, name);
          } else {
            String msg = String.format(Locale.ROOT, "More than one core points to data dir %s. They are in %s and %s",
                dataDir, dirs.get(dataDir), name);
            log.warn(msg);
          }
        }
      }
    }
  }

  @Override
  public String getBadConfigCoreMessage(String name) {
    return badConfigCores.get(name);
  }
  
  public static Document copyDoc(Document doc) throws TransformerException {
    TransformerFactory tfactory = TransformerFactory.newInstance();
    Transformer tx = tfactory.newTransformer();
    DOMSource source = new DOMSource(doc);
    DOMResult result = new DOMResult();
    tx.transform(source, result);
    return (Document) result.getNode();
  }

  //TODO: For 5.0, you shouldn't have to do the sbustituteProperty, this is anothe bit
  // of awkward back-compat due to persistence.
  @Override
  public int getInt(CfgProp prop, int def) {
    String val = propMap.get(prop);
    if (val != null) val = PropertiesUtil.substituteProperty(val, null);
    return (val == null) ? def : Integer.parseInt(val);
  }

  @Override
  public boolean getBool(CfgProp prop, boolean defValue) {
    String val = propMap.get(prop);
    if (val != null) val = PropertiesUtil.substituteProperty(val, null);
    return (val == null) ? defValue : Boolean.parseBoolean(val);
  }

  @Override
  public String get(CfgProp prop, String def) {
    String val = propMap.get(prop);
    if (val != null) val = PropertiesUtil.substituteProperty(val, null);
    return (val == null) ? def : val;
  }

  // For saving the original property, ${} syntax and all.
  @Override
  public String getOrigProp(CfgProp prop, String def) {
    String val = propMap.get(prop);
    return (val == null) ? def : val;
  }


  public ShardHandlerFactory initShardHandler() {
    PluginInfo info = null;
    Node shfn = getNode("solr/cores/shardHandlerFactory", false);

    if (shfn != null) {
      info = new PluginInfo(shfn, "shardHandlerFactory", false, true);
    } else {
      Map m = new HashMap();
      m.put("class", HttpShardHandlerFactory.class.getName());
      info = new PluginInfo("shardHandlerFactory", m, null, Collections.<PluginInfo>emptyList());
    }

    ShardHandlerFactory fac;
    try {
       fac = getResourceLoader().findClass(info.className, ShardHandlerFactory.class).newInstance();
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                              "Error instantiating shardHandlerFactory class " + info.className);
    }
    if (fac instanceof PluginInfoInitialized) {
      ((PluginInfoInitialized) fac).init(info);
    }
    return fac;
  }

  @Override
  public Properties getSolrProperties(String path) {
    try {
      return readProperties(((NodeList) evaluate(
          path, XPathConstants.NODESET)).item(0));
    } catch (Throwable e) {
      SolrException.log(log, null, e);
    }
    return null;

  }

  Properties readProperties(Node node) throws XPathExpressionException {
    XPath xpath = getXPath();
    NodeList props = (NodeList) xpath.evaluate("property", node, XPathConstants.NODESET);
    Properties properties = new Properties();
    for (int i = 0; i < props.getLength(); i++) {
      Node prop = props.item(i);
      properties.setProperty(DOMUtil.getAttr(prop, "name"), DOMUtil.getAttr(prop, "value"));
    }
    return properties;
  }

  @Override
  public Map<String, String> readCoreAttributes(String coreName) {
    Map<String, String> attrs = new HashMap<String, String>();

    if (is50OrLater) {
      return attrs; // this is a no-op.... intentionally
    }
    synchronized (coreNodes) {
      for (int idx = 0; idx < coreNodes.getLength(); ++idx) {
        Node node = coreNodes.item(idx);
        if (coreName.equals(DOMUtil.getAttr(node, CoreDescriptor.CORE_NAME, null))) {
          NamedNodeMap attributes = node.getAttributes();
          for (int i = 0; i < attributes.getLength(); i++) {
            Node attribute = attributes.item(i);
            String val = attribute.getNodeValue();
            if (CoreDescriptor.CORE_DATADIR.equals(attribute.getNodeName()) ||
                CoreDescriptor.CORE_INSTDIR.equals(attribute.getNodeName())) {
              if (val.indexOf('$') == -1) {
                val = (val != null && !val.endsWith("/")) ? val + '/' : val;
              }
            }
            attrs.put(attribute.getNodeName(), val);
          }
          return attrs;
        }
      }
    }
    return attrs;
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
  public SolrConfig getSolrConfigFromZk(ZkController zkController, String zkConfigName, String solrConfigFileName,
                                        SolrResourceLoader resourceLoader) {
    SolrConfig cfg = null;
    try {
      byte[] config = zkController.getConfigFileData(zkConfigName, solrConfigFileName);
      InputSource is = new InputSource(new ByteArrayInputStream(config));
      is.setSystemId(SystemIdResolver.createSystemIdFromResourceName(solrConfigFileName));
      cfg = solrConfigFileName == null ? new SolrConfig(
          resourceLoader, SolrConfig.DEFAULT_CONF_FILE, is) : new SolrConfig(
          resourceLoader, solrConfigFileName, is);
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "getSolrConfigFromZK failed for " + zkConfigName + " " + solrConfigFileName, e);
    }
    return cfg;
  }

  static List<SolrXMLSerializer.SolrCoreXMLDef> solrCoreXMLDefs = new ArrayList<SolrXMLSerializer.SolrCoreXMLDef>();
  // Do this when re-using a ConfigSolrXml.

  // These two methods are part of SOLR-4196 and are awkward, should go away with 5.0
  @Override
  public void initPersist() {
    initPersistStatic();
  }

  public static void initPersistStatic() {
    solrCoreXMLDefs = new ArrayList<SolrXMLSerializer.SolrCoreXMLDef>();
    solrXMLSerializer = new SolrXMLSerializer();
  }

  @Override
  public void addPersistCore(String coreName, Properties attribs, Map<String, String> props) {
    addPersistCore(attribs, props);
  }

  static void addPersistCore(Properties props, Map<String, String> attribs) {
    SolrXMLSerializer.SolrCoreXMLDef solrCoreXMLDef = new SolrXMLSerializer.SolrCoreXMLDef();
    solrCoreXMLDef.coreAttribs = attribs;
    solrCoreXMLDef.coreProperties = props;
    solrCoreXMLDefs.add(solrCoreXMLDef);
  }

  private static SolrXMLSerializer solrXMLSerializer = new SolrXMLSerializer();

  @Override
  public void addPersistAllCores(Properties containerProperties, Map<String, String> rootSolrAttribs, Map<String, String> coresAttribs,
                                 File file) {
    addPersistAllCoresStatic(containerProperties, rootSolrAttribs, coresAttribs, file);
  }

  // Fortunately, we don't iterate over these too often, so the waste is probably tolerable.

  @Override
  public String getCoreNameFromOrig(String origCoreName, SolrResourceLoader loader, String coreName) {

    if (is50OrLater) {
      // first look for an exact match
      for (Map.Entry<String, CoreDescriptorPlus> ent : coreDescriptorPlusMap.entrySet()) {

        String name = ent.getValue().getCoreDescriptor().getProperty(CoreDescriptor.CORE_NAME, null);
        if (origCoreName.equals(name)) {
          if (coreName.equals(origCoreName)) {
            return name;
          }
          return coreName;
        }
      }

      for (Map.Entry<String, CoreDescriptorPlus> ent : coreDescriptorPlusMap.entrySet()) {
        String name = ent.getValue().getCoreDescriptor().getProperty(CoreDescriptor.CORE_NAME, null);
        // see if we match with substitution
        if (origCoreName.equals(PropertiesUtil.substituteProperty(name, loader.getCoreProperties()))) {
          if (coreName.equals(origCoreName)) {
            return name;
          }
          return coreName;
        }
      }
    } else {
      // look for an existing node
      synchronized (coreNodes) {
        // first look for an exact match
        Node coreNode = null;
        for (int i = 0; i < coreNodes.getLength(); i++) {
          Node node = coreNodes.item(i);

          String name = DOMUtil.getAttr(node, CoreDescriptor.CORE_NAME, null);
          if (origCoreName.equals(name)) {
            if (coreName.equals(origCoreName)) {
              return name;
            }
            return coreName;
          }
        }

        if (coreNode == null) {
          // see if we match with substitution
          for (int i = 0; i < coreNodes.getLength(); i++) {
            Node node = coreNodes.item(i);
            String name = DOMUtil.getAttr(node, CoreDescriptor.CORE_NAME, null);
            if (origCoreName.equals(PropertiesUtil.substituteProperty(name,
                loader.getCoreProperties()))) {
              if (coreName.equals(origCoreName)) {
                return name;
              }
              return coreName;
            }
          }
        }
      }
    }
    return null;
  }

  @Override
  public List<String> getAllCoreNames() {
    List<String> ret = new ArrayList<String>();
    if (is50OrLater) {
      ret = new ArrayList<String>(coreDescriptorPlusMap.keySet());
    } else {
      synchronized (coreNodes) {
        for (int idx = 0; idx < coreNodes.getLength(); ++idx) {
          Node node = coreNodes.item(idx);
          ret.add(DOMUtil.getAttr(node, CoreDescriptor.CORE_NAME, null));
        }
      }
    }
    return ret;
  }

  @Override
  public String getProperty(String coreName, String property, String defaultVal) {
    if (is50OrLater) {
      CoreDescriptorPlus plus = coreDescriptorPlusMap.get(coreName);
      if (plus == null) return defaultVal;
      CoreDescriptor desc = plus.getCoreDescriptor();
      if (desc == null) return defaultVal;
      return desc.getProperty(property, defaultVal);
    } else {
      synchronized (coreNodes) {
        for (int idx = 0; idx < coreNodes.getLength(); ++idx) {
          Node node = coreNodes.item(idx);
          if (coreName.equals(DOMUtil.getAttr(node, CoreDescriptor.CORE_NAME, null))) {
            return DOMUtil.getAttr(node, property, defaultVal);
          }
        }
      }
      return defaultVal;
    }
  }

  @Override
  public Properties readCoreProperties(String coreName) {
    if (is50OrLater) {
      CoreDescriptorPlus plus = coreDescriptorPlusMap.get(coreName);
      if (plus == null) return null;
      return new Properties(plus.getCoreDescriptor().getCoreProperties());
    } else {
      synchronized (coreNodes) {
        for (int idx = 0; idx < coreNodes.getLength(); ++idx) {
          Node node = coreNodes.item(idx);
          if (coreName.equals(DOMUtil.getAttr(node, CoreDescriptor.CORE_NAME, null))) {
            try {
              return readProperties(node);
            } catch (XPathExpressionException e) {
              return null;
            }
          }
        }
      }
    }
    return null;
  }

  @Override
  public boolean is50OrLater() { return is50OrLater; }

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


  static void addPersistAllCoresStatic(Properties containerProperties, Map<String, String> rootSolrAttribs, Map<String, String> coresAttribs,
                                       File file) {
    SolrXMLSerializer.SolrXMLDef solrXMLDef = new SolrXMLSerializer.SolrXMLDef();
    solrXMLDef.coresDefs = solrCoreXMLDefs;
    solrXMLDef.containerProperties = containerProperties;
    solrXMLDef.solrAttribs = rootSolrAttribs;
    solrXMLDef.coresAttribs = coresAttribs;
    solrXMLSerializer.persistFile(file, solrXMLDef);

  }

  static final String DEF_SOLR_XML = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n"
      + "<solr persistent=\"false\">\n"
      + "  <cores adminPath=\"/admin/cores\" defaultCoreName=\""
      + CoreContainer.DEFAULT_DEFAULT_CORE_NAME
      + "\""
      + " host=\"${host:}\" hostPort=\"${hostPort:}\" hostContext=\"${hostContext:}\" zkClientTimeout=\"${zkClientTimeout:15000}\""
      + ">\n"
      + "    <core name=\""
      + CoreContainer.DEFAULT_DEFAULT_CORE_NAME
      + "\" shard=\"${shard:}\" collection=\"${collection:}\" instanceDir=\"collection1\" />\n"
      + "  </cores>\n" + "</solr>";

}

// It's mightily convenient to have all of the original path names and property values when persisting cores, so
// this little convenience class is just for that.
// Also, let's keep track of anything we added here, especially the instance dir for persistence purposes. We don't
// want, for instance, to persist instanceDir if it was not specified originally.
//
// I suspect that for persistence purposes, we may want to expand this idea to record, say, ${blah}
class CoreDescriptorPlus {
  private CoreDescriptor coreDescriptor;
  private String filePath;
  private Properties propsOrig; // TODO: 5.0. Remove this since it's only really used for persisting.

  CoreDescriptorPlus(String filePath, CoreDescriptor descriptor, Properties propsOrig) {
    coreDescriptor = descriptor;
    this.filePath = filePath;
    this.propsOrig = propsOrig;
  }

  CoreDescriptor getCoreDescriptor() {
    return coreDescriptor;
  }

  String getFilePath() {
    return filePath;
  }

  Properties getPropsOrig() {
    return propsOrig;
  }
}

