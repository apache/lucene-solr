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
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * ConfigSolrXml
 * <p/>
 * This class is entirely to localize the backwards compatibility for dealing with specific issues when transitioning
 * from solr.xml to a solr.properties-based, enumeration/discovery of defined cores. See SOLR-4196 for background.
 * <p/>
 * @since solr 4.3
 *
 * It's a bit twisted, but we decided to NOT do the solr.properties switch. But since there's already an interface
 * it makes sense to leave it in so we can use other methods of providing the Solr information that is contained
 * in solr.xml. Perhapse something form SolrCloud in the future?
 *
 */

public class ConfigSolrXml extends Config implements ConfigSolr {

  private static Map<ConfLevel, String> prefixes;
  private boolean isAutoDiscover = false;

  private final Map<String, CoreDescriptorPlus> coreDescriptorPlusMap = new HashMap<String, CoreDescriptorPlus>();
  private NodeList coreNodes = null;

  static {
    prefixes = new HashMap<ConfLevel, String>();

    prefixes.put(ConfLevel.SOLR, "solr/@");
    prefixes.put(ConfLevel.SOLR_CORES, "solr/cores/@");
    prefixes.put(ConfLevel.SOLR_CORES_CORE, "solr/cores/core/@");
    prefixes.put(ConfLevel.SOLR_LOGGING, "solr/logging/@");
    prefixes.put(ConfLevel.SOLR_LOGGING_WATCHER, "solr/logging/watcher/@");
  }

  public ConfigSolrXml(SolrResourceLoader loader, String name, InputStream is, String prefix,
                       boolean subProps, CoreContainer container) throws ParserConfigurationException, IOException, SAXException {
    super(loader, name, new InputSource(is), prefix, subProps);
    initCoreList(container);
  }


  public ConfigSolrXml(SolrResourceLoader loader, Config cfg, CoreContainer container) throws TransformerException, IOException {
    super(loader, null, copyDoc(cfg.getDocument())); // Mimics a call from CoreContainer.
    initCoreList(container);
  }

  public void initCoreList(CoreContainer container) throws IOException {
    isAutoDiscover = getBool(ConfigSolr.ConfLevel.SOLR_CORES, "autoDiscoverCores", false);
    if (isAutoDiscover) {
      synchronized (coreDescriptorPlusMap) {
        walkFromHere(new File(container.getSolrHome()), container);
      }

    } else {
      coreNodes = (NodeList) evaluate("solr/cores/core",
          XPathConstants.NODESET);
    }

  }
  public static Document copyDoc(Document doc) throws TransformerException {
    TransformerFactory tfactory = TransformerFactory.newInstance();
    Transformer tx = tfactory.newTransformer();
    DOMSource source = new DOMSource(doc);
    DOMResult result = new DOMResult();
    tx.transform(source, result);
    return (Document) result.getNode();
  }

  @Override
  public int getInt(ConfLevel level, String tag, int def) {
    return getInt(prefixes.get(level) + tag, def);
  }

  @Override
  public boolean getBool(ConfLevel level, String tag, boolean defValue) {
    return getBool(prefixes.get(level) + tag, defValue);
  }

  @Override
  public String get(ConfLevel level, String tag, String def) {
    return get(prefixes.get(level) + tag, def);
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
  public Properties getSolrProperties(ConfigSolr cfg, String context) {
    try {
      return readProperties(((NodeList) evaluate(
          context, XPathConstants.NODESET)).item(0));
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

    if (isAutoDiscover) {
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
  private void walkFromHere(File file, CoreContainer container) throws IOException {
    log.info("Looking for cores in " + file.getAbsolutePath());
    for (File childFile : file.listFiles()) {
      // This is a little tricky, we are asking if core.properties exists in a child directory of the directory passed
      // in. In other words we're looking for core.properties in the grandchild directories of the parameter passed
      // in. That allows us to gracefully top recursing deep but continue looking wide.
      File propFile = new File(childFile, CORE_PROP_FILE);
      if (propFile.exists()) { // Stop looking after processing this file!
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

        if (props.getProperty(CoreDescriptor.CORE_INSTDIR) == null) {
          props.setProperty(CoreDescriptor.CORE_INSTDIR, childFile.getPath());
        }

        if (props.getProperty(CoreDescriptor.CORE_NAME) == null) {
          // Should default to this directory
          props.setProperty(CoreDescriptor.CORE_NAME, childFile.getName());
        }
        CoreDescriptor desc = new CoreDescriptor(container, props);
        CoreDescriptorPlus plus = new CoreDescriptorPlus(propFile.getAbsolutePath(), desc, propsOrig);
        coreDescriptorPlusMap.put(desc.getName(), plus);
        continue; // Go on to the sibling directory
      }
      if (childFile.isDirectory()) {
        walkFromHere(childFile, container);
      }
    }
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

    if (isAutoDiscover) {
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
    if (isAutoDiscover) {
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
    if (isAutoDiscover) {
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
    if (isAutoDiscover) {
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
  private Properties propsOrig;

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

