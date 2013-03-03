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
import org.apache.commons.lang.StringUtils;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.HttpShardHandlerFactory;
import org.apache.solr.handler.component.ShardHandlerFactory;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.util.PropertiesUtil;
import org.apache.solr.util.SystemIdResolver;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * This is the new way of dealing with solr properties replacing solr.xml. This is simply a high-level set of
 * properties. Cores are no longer defined in the solr.xml file, they are discovered by enumerating all of the
 * directories under the base path and creating cores as necessary.
 *
 * @since Solr 4.2
 */
public class SolrProperties implements ConfigSolr {
  public final static String SOLR_PROPERTIES_FILE = "solr.properties";
  public final static String SOLR_XML_FILE = "solr.xml";
  final static String CORE_PROP_FILE = "core.properties";

  private final static String SHARD_HANDLER_FACTORY = "shardHandlerFactory";
  private final static String SHARD_HANDLER_NAME = SHARD_HANDLER_FACTORY + ".name";
  private final static String SHARD_HANDLER_CLASS = SHARD_HANDLER_FACTORY + ".class";

  public static final Logger log = LoggerFactory.getLogger(SolrProperties.class);

  protected final CoreContainer container;
  protected Properties solrProperties = new Properties();
  protected final Properties origsolrprops = new Properties();
  protected String name;
  protected SolrResourceLoader loader;

  private final Map<String, CoreDescriptorPlus> coreDescriptorPlusMap = new HashMap<String, CoreDescriptorPlus>();

  private static Map<ConfLevel, String> prefixesprefixes;

  static {
    prefixesprefixes = new HashMap<ConfLevel, String>();

    prefixesprefixes.put(ConfLevel.SOLR_CORES, "cores.");
    prefixesprefixes.put(ConfLevel.SOLR_LOGGING, "logging.");
    prefixesprefixes.put(ConfLevel.SOLR_LOGGING_WATCHER, "logging.watcher.");

  }


  /**
   * Create a SolrProperties object given just the resource loader
   *
   * @param container - the container for this Solr instance. There should be one and only one...
   * @param loader    - Solr resource loader
   * @param solrCfg   - a config file whose values will be transferred to the properties object that can be changed
   * @throws IOException - It's possible to walk a very deep tree, if that process goes awry, or if reading any
   *                     of the files found doesn't work, you'll get an IO exception
   */
  SolrProperties(CoreContainer container, SolrResourceLoader loader, SolrProperties solrCfg) throws IOException {
    origsolrprops.putAll(solrCfg.getOriginalProperties());
    this.loader = loader;
    this.container = container;
    init(solrCfg.name);
  }

  /**
   * Create a SolrProperties object from an opened input stream, useful for creating defaults
   *
   * @param container - the container for this Solr instance. There should be one and only one...
   * @param is        - Input stream for loading properties.
   * @param fileName  - the name for this properties object.
   * @throws IOException - It's possible to walk a very deep tree, if that process goes awry, or if reading any
   *                     of the files found doesn't work, you'll get an IO exception
   */
  public SolrProperties(CoreContainer container, InputStream is, String fileName) throws IOException {
    origsolrprops.load(is);
    this.container = container;
    init(fileName);
  }

  //Just localize the common constructor operations
  private void init(String name) throws IOException {
    this.name = name;
    for (String s : origsolrprops.stringPropertyNames()) {
      solrProperties.put(s, System.getProperty(s, origsolrprops.getProperty(s)));
    }
    synchronized (coreDescriptorPlusMap) {
      walkFromHere(new File(container.getSolrHome()), container);
    }
  }

  // Just localizes default substitution and the ability to log an error if the value isn't present.
  private String getVal(String path, boolean errIfMissing, String defVal) {
    String val = solrProperties.getProperty(path, defVal);

    if (StringUtils.isNotBlank(val)) {
      log.debug(name + ' ' + path + val);
      return val;
    }

    if (!errIfMissing) {
      log.debug(name + "missing optional " + path);
      return null;
    }

    throw new RuntimeException(name + " missing " + path);
  }

  /**
   * Get a property and convert it to a boolean value. Does not log a message if the value is absent
   *
   * @param prop     - name of the property to fetch
   * @param defValue - value to return if the property is absent
   * @return property value or default if property is not present.
   */
  public boolean getBool(String prop, boolean defValue) {
    String def = defValue ? "true" : "false";
    String val = getVal(prop, false, def);
    return (StringUtils.equalsIgnoreCase(val, "true"));
  }

  /**
   * Fetch a string value, for the given property. Does not log a message if the valued is absent.
   *
   * @param prop - the property name to fetch
   * @param def  - the default value to return if not present
   * @return - the fetched property or the default value if the property is absent
   */
  public String get(String prop, String def) {
    String val = getVal(prop, false, def);
    if (val == null || val.length() == 0) {
      return def;
    }
    return val;
  }

  /**
   * Fetch the string value of the property. May log a message and returns null if absent
   *
   * @param prop         - the name of the property to fetch
   * @param errIfMissing - if true, log a message that the property is not present
   * @return - the property value or null if absent
   */
  public String getVal(String prop, boolean errIfMissing) {
    return getVal(prop, errIfMissing, null);
  }

  /**
   * Returns a property as an integer
   *
   * @param prop   - the name of the property to fetch
   * @param defVal - the value to return if the property is missing
   * @return - the fetch property as an int or the def value if absent
   */
  public int getInt(String prop, int defVal) {
    String val = getVal(prop, false, Integer.toString(defVal));
    return Integer.parseInt(val);
  }

  @Override
  public int getInt(ConfLevel level, String tag, int def) {
    return getInt(prefixesprefixes.get(level) + tag, def);
  }

  @Override
  public boolean getBool(ConfLevel level, String tag, boolean defValue) {
    return getBool(prefixesprefixes.get(level) + tag, defValue);
  }

  @Override
  public String get(ConfLevel level, String tag, String def) {
    return get(prefixesprefixes.get(level) + tag, def);
  }

  /**
   * For all values in the properties structure, find if any system properties are defined and substitute them.
   */
  public void substituteProperties() {
    for (String prop : solrProperties.stringPropertyNames()) {
      String subProp = PropertiesUtil.substituteProperty(solrProperties.getProperty(prop), solrProperties);
      if (subProp != null && !subProp.equals(solrProperties.getProperty(prop))) {
        solrProperties.put(prop, subProp);
      }
    }
  }

  /**
   * Fetches the properties as originally read from the properties file without any system variable substitution
   *
   * @return - a copy of the original properties.
   */
  public Properties getOriginalProperties() {
    Properties ret = new Properties();
    ret.putAll(origsolrprops);
    return ret;
  }

  @Override
  public ShardHandlerFactory initShardHandler(/*boolean isTest*/) {

    PluginInfo info = null;
    Map<String, String> attrs = new HashMap<String, String>();
    NamedList args = new NamedList();
    boolean haveHandler = false;
    for (String s : solrProperties.stringPropertyNames()) {
      String val = solrProperties.getProperty(s);
      if (s.indexOf(SHARD_HANDLER_FACTORY) != -1) {
        haveHandler = true;
        if (SHARD_HANDLER_NAME.equals(s) || SHARD_HANDLER_CLASS.equals(s)) {
          attrs.put(s, val);
        } else {
          args.add(s, val);
        }
      }
    }

    if (haveHandler) {
      //  public PluginInfo(String type, Map<String, String> attrs ,NamedList initArgs, List<PluginInfo> children) {

      info = new PluginInfo(SHARD_HANDLER_FACTORY, attrs, args, null);
    } else {
      Map m = new HashMap();
      m.put("class", HttpShardHandlerFactory.class.getName());
      info = new PluginInfo("shardHandlerFactory", m, null, Collections.<PluginInfo>emptyList());
    }
    HttpShardHandlerFactory fac = new HttpShardHandlerFactory();
    if (info != null) {
      fac.init(info);
    }
    return fac;
  }

  // Strictly for compatibility with i'face. TODO: remove for 5.0
  @Override
  public Properties getSolrProperties(ConfigSolr cfg, String context) {
    return getSolrProperties();
  }

  /**
   * Return the original properties that were defined, without substitutions from solr.properties
   *
   * @return - the Properties as originally defined.
   */
  public Properties getSolrProperties() {
    return solrProperties;
  }

  /**
   * given a core and attributes, find the core.properties file from whence it came and update it with the current
   * <p/>
   * Note, when the cores were discovered, we stored away the path that it came from for reference later. Remember
   * that these cores aren't necessarily loaded all the time, they may be transient.
   * It's not clear what the magic is that the calling methods (see CoreContainer) are doing, but they seem to be
   * "doing the right thing" so that the attribs properties are the ones that contain the correct data. All the
   * tests pass, but it's magic at this point.
   *
   * @param coreName - the core whose attributes we are to change
   * @param attribs  - the attribs to change to, see note above.
   * @param props    - ignored, here to make the i'face work in combination with ConfigSolrXmlBackCompat
   */

  @Override
  public void addPersistCore(String coreName, Properties attribs, Map<String, String> props) {
    String val = container.getContainerProperties().getProperty("solr.persistent", "false");
    if (!Boolean.parseBoolean(val)) return;

    CoreDescriptorPlus plus;
    plus = coreDescriptorPlusMap.get(coreName);
    if (plus == null) {
      log.error("Expected to find core for persisting, but we did not. Core: " + coreName);
      return;
    }

    Properties outProps = new Properties();
    // I don't quite get this, but somehow the attribs passed in are the originals (plus any newly-added ones). Never
    // one to look a gift horse in the mouth I'll just use that.

    // Take care NOT to write out properties like ${blah blah blah}
    outProps.putAll(attribs);
    Properties corePropsOrig = plus.getPropsOrig();
    for (String prop : corePropsOrig.stringPropertyNames()) {
      val = corePropsOrig.getProperty(prop);
      if (val.indexOf("$") != -1) { // it was originally a system property, keep it so
        outProps.put(prop, val);
        continue;
      }
      // Make sure anything that used to be in the properties file still is.
      if (outProps.getProperty(prop) == null) {
        outProps.put(prop, val);
      }
    }
    // Any of our standard properties that weren't in the original properties file should NOT be persisted, I think
    for (String prop : CoreDescriptor.standardPropNames) {
      if (corePropsOrig.getProperty(prop) == null) {
        outProps.remove(prop);
      }
    }

    OutputStream os = null;
    try {
      os = new FileOutputStream(plus.getFilePath());
      outProps.store(os, null);
    } catch (IOException e) {
      log.error("Failed to persist core {}, filepath {}", coreName, plus.getFilePath());
    } finally {
      IOUtils.closeQuietly(os);
    }

  }

  /**
   * PersistSolrProperties persists the Solr.properties file only,
   * <p/>
   * The old version (i.e. using solr.xml) persisted _everything_ in a single file. This version will just
   * persist the solr.properties file for an individual core.
   * The individual cores were persisted in addPersistCore calls above.
   */
  // It seems like a lot of this could be done by using the Properties defaults

  /**
   * PersistSolrProperties persists the Solr.properties file only,
   * <p/>
   * The old version (i.e. using solr.xml) persisted _everything_ in a single file. This version will just
   * persist the solr.properties file for an individual core.
   * The individual cores were persisted in addPersistCore calls above.
   * <p/>
   * TODO: Remove all parameters for 5.0 when we obsolete ConfigSolrXmlBackCompat
   *
   * @param containerProperties - ignored, here for back compat.
   * @param rootSolrAttribs     - ignored, here for back compat.
   * @param coresAttribs        - ignored, here for back compat.
   * @param file                - ignored, here for back compat.
   */

  @Override
  public void addPersistAllCores(Properties containerProperties, Map<String, String> rootSolrAttribs,
                                 Map<String, String> coresAttribs, File file) {
    String val = container.getContainerProperties().getProperty("solr.persistent", "false");
    if (!Boolean.parseBoolean(val)) return;

    // First persist solr.properties
    File parent = new File(container.getSolrHome());
    File props = new File(parent, SOLR_PROPERTIES_FILE);
    Properties propsOut = new Properties();
    propsOut.putAll(container.getContainerProperties());
    for (String prop : origsolrprops.stringPropertyNames()) {
      String toTest = origsolrprops.getProperty(prop);
      if (toTest.indexOf("$") != -1) { // Don't store away things that should be system properties
        propsOut.put(prop, toTest);
      }
    }
    OutputStream os = null;
    try {
      os = new FileOutputStream(props);
      propsOut.store(os, null);
    } catch (IOException e) {
      log.error("Failed to persist file " + props.getAbsolutePath(), e);
    } finally {
      IOUtils.closeQuietly(os);
    }
  }


  // Copied verbatim from the old code, presumably this will be tested when we eliminate solr.xml
  @Override
  public IndexSchema getSchemaFromZk(ZkController zkController, String zkConfigName, String schemaName,
                                     SolrConfig config)
      throws KeeperException, InterruptedException {
    byte[] configBytes = zkController.getConfigFileData(zkConfigName, schemaName);
    InputSource is = new InputSource(new ByteArrayInputStream(configBytes));
    is.setSystemId(SystemIdResolver.createSystemIdFromResourceName(schemaName));
    IndexSchema schema = new IndexSchema(config, schemaName, is);
    return schema;
  }

  // Copied verbatim from the old code, presumably this will be tested when we eliminate solr.xml
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

  @Override
  public void initPersist() {
    //NOOP
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
  public String getCoreNameFromOrig(String origCoreName, SolrResourceLoader loader, String coreName) {
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
    return null;
  }

  @Override
  public List<String> getAllCoreNames() {
    List<String> ret;
    ret = new ArrayList<String>(coreDescriptorPlusMap.keySet());
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

  @Override
  public Map<String, String> readCoreAttributes(String coreName) {
    return new HashMap<String, String>();  // Should be a no-op.
  }
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
