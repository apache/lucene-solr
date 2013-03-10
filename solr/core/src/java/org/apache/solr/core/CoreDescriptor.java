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

import java.util.Properties;
import java.io.File;

import org.apache.commons.lang.StringUtils;
import org.apache.solr.cloud.CloudDescriptor;

/**
 * A Solr core descriptor
 *
 * @since solr 1.3
 */
public class CoreDescriptor {

  // Properties file name constants
  public static final String CORE_NAME = "name";
  public static final String CORE_CONFIG = "config";
  public static final String CORE_INSTDIR = "instanceDir";
  public static final String CORE_DATADIR = "dataDir";
  public static final String CORE_ULOGDIR = "ulogDir";
  public static final String CORE_SCHEMA = "schema";
  public static final String CORE_SHARD = "shard";
  public static final String CORE_COLLECTION = "collection";
  public static final String CORE_ROLES = "roles";
  public static final String CORE_PROPERTIES = "properties";
  public static final String CORE_LOADONSTARTUP = "loadOnStartup";
  public static final String CORE_TRANSIENT = "transient";
  public static final String CORE_NODE_NAME = "coreNodeName";

  static final String[] standardPropNames = {
      CORE_NAME,
      CORE_CONFIG,
      CORE_INSTDIR,
      CORE_DATADIR,
      CORE_ULOGDIR,
      CORE_SCHEMA,
      CORE_SHARD,
      CORE_COLLECTION,
      CORE_ROLES,
      CORE_PROPERTIES,
      CORE_LOADONSTARTUP,
      CORE_TRANSIENT
  };

  // As part of moving away from solr.xml (see SOLR-4196), it's _much_ easier to keep these as properties than set
  // them individually.
  private Properties coreProperties = new Properties();

  private final CoreContainer coreContainer;

  private CloudDescriptor cloudDesc;

  private CoreDescriptor(CoreContainer cont) {
    // Just a place to put initialization since it's a pain to add to the descriptor in every c'tor.
    this.coreContainer = cont;
    coreProperties.put(CORE_LOADONSTARTUP, "true");
    coreProperties.put(CORE_TRANSIENT, "false");

  }
  public CoreDescriptor(CoreContainer container, String name, String instanceDir) {
    this(container);
    doInit(name, instanceDir);
  }


  public CoreDescriptor(CoreDescriptor descr) {
    this(descr.coreContainer);
    coreProperties.put(CORE_INSTDIR, descr.getInstanceDir());
    coreProperties.put(CORE_CONFIG, descr.getConfigName());
    coreProperties.put(CORE_SCHEMA, descr.getSchemaName());
    coreProperties.put(CORE_NAME, descr.getName());
    coreProperties.put(CORE_DATADIR, descr.getDataDir());
  }

  /**
   * CoreDescriptor - create a core descriptor given default properties from a core.properties file. This will be
   * used in the "solr.xml-less (See SOLR-4196) world where there are no &lt;core&gt; &lt;/core&gt; tags at all, thus  much
   * of the initialization that used to be done when reading solr.xml needs to be done here instead, particularly
   * setting any defaults (e.g. schema.xml, directories, whatever).
   *
   * @param container - the CoreContainer that holds all the information about our cores, loaded, lazy etc.
   * @param propsIn - A properties structure "core.properties" found while walking the file tree to discover cores.
   *                  Any properties set in this param will overwrite the any defaults.
   */
  public CoreDescriptor(CoreContainer container, Properties propsIn) {
    this(container);

    // Set some default, normalize a directory or two
    doInit(propsIn.getProperty(CORE_NAME), propsIn.getProperty(CORE_INSTDIR));

    coreProperties.putAll(propsIn);
  }

  private void doInit(String name, String instanceDir) {
    if (name == null) {
      throw new RuntimeException("Core needs a name");
    }

    coreProperties.put(CORE_NAME, name);

    if(coreContainer != null && coreContainer.getZkController() != null) {
      this.cloudDesc = new CloudDescriptor();
      // cloud collection defaults to core name
      cloudDesc.setCollectionName(name);
    }

    if (instanceDir == null) {
      throw new NullPointerException("Missing required \'instanceDir\'");
    }
    instanceDir = SolrResourceLoader.normalizeDir(instanceDir);
    coreProperties.put(CORE_INSTDIR, instanceDir);
    coreProperties.put(CORE_CONFIG, getDefaultConfigName());
    coreProperties.put(CORE_SCHEMA, getDefaultSchemaName());
  }

  public Properties initImplicitProperties() {
    Properties implicitProperties = new Properties(coreContainer.getContainerProperties());
    implicitProperties.setProperty(CORE_NAME, getName());
    implicitProperties.setProperty(CORE_INSTDIR, getInstanceDir());
    implicitProperties.setProperty(CORE_DATADIR, getDataDir());
    implicitProperties.setProperty(CORE_CONFIG, getConfigName());
    implicitProperties.setProperty(CORE_SCHEMA, getSchemaName());
    return implicitProperties;
  }

  /**@return the default config name. */
  public String getDefaultConfigName() {
    return "solrconfig.xml";
  }

  /**@return the default schema name. */
  public String getDefaultSchemaName() {
    return "schema.xml";
  }

  /**@return the default data directory. */
  public String getDefaultDataDir() {
    return "data" + File.separator;
  }

  public String getPropertiesName() {
    return coreProperties.getProperty(CORE_PROPERTIES);
  }

  public void setPropertiesName(String propertiesName) {
    coreProperties.put(CORE_PROPERTIES, propertiesName);
  }

  public String getDataDir() {
    String dataDir = coreProperties.getProperty(CORE_DATADIR);
    if (dataDir == null) dataDir = getDefaultDataDir();
    return dataDir;
  }

  public void setDataDir(String s) {
    // normalize zero length to null.
    if (StringUtils.isBlank(s)) {
      coreProperties.remove(s);
    } else {
      coreProperties.put(CORE_DATADIR, s);
    }
  }
  
  public boolean usingDefaultDataDir() {
    // DO NOT use the getDataDir method here since it'll assign something regardless.
    return coreProperties.getProperty(CORE_DATADIR) == null;
  }

  /**@return the core instance directory. */
  public String getRawInstanceDir() {
    return coreProperties.getProperty(CORE_INSTDIR);
  }

  /**
   *
   * @return the core instance directory, prepended with solr_home if not an absolute path.
   */
  public String getInstanceDir() {
    String instDir = coreProperties.getProperty(CORE_INSTDIR);
    if (instDir == null) return null; // No worse than before.

    if (new File(instDir).isAbsolute()) {
      return SolrResourceLoader.normalizeDir(
          SolrResourceLoader.normalizeDir(instDir));
    }
    return SolrResourceLoader.normalizeDir(coreContainer.getSolrHome() +
        SolrResourceLoader.normalizeDir(instDir));
  }

  /**Sets the core configuration resource name. */
  public void setConfigName(String name) {
    if (name == null || name.length() == 0)
      throw new IllegalArgumentException("name can not be null or empty");
    coreProperties.put(CORE_CONFIG, name);
  }

  /**@return the core configuration resource name. */
  public String getConfigName() {
    return coreProperties.getProperty(CORE_CONFIG);
  }

  /**Sets the core schema resource name. */
  public void setSchemaName(String name) {
    if (name == null || name.length() == 0)
      throw new IllegalArgumentException("name can not be null or empty");
    coreProperties.put(CORE_SCHEMA, name);
  }

  /**@return the core schema resource name. */
  public String getSchemaName() {
    return coreProperties.getProperty(CORE_SCHEMA);
  }

  /**@return the initial core name */
  public String getName() {
    return coreProperties.getProperty(CORE_NAME);
  }

  public CoreContainer getCoreContainer() {
    return coreContainer;
  }

  Properties getCoreProperties() {
    return coreProperties;
  }

  /**
   * Set this core's properties. Please note that some implicit values will be added to the
   * Properties instance passed into this method. This means that the Properties instance
   * sent to this method will have different (less) key/value pairs than the Properties
   * instance returned by #getCoreProperties method.
   *
   * Under any circumstance, the properties passed in will override any already present.Merge
   */
  public void setCoreProperties(Properties coreProperties) {
    if (this.coreProperties == null) {
      Properties p = initImplicitProperties();
      this.coreProperties = new Properties(p);
    }
    // The caller presumably wants whatever properties passed in to override the current core props, so just add them.
    if(coreProperties != null) {
      this.coreProperties.putAll(coreProperties);
    }
  }

  public CloudDescriptor getCloudDescriptor() {
    return cloudDesc;
  }
  
  public void setCloudDescriptor(CloudDescriptor cloudDesc) {
    this.cloudDesc = cloudDesc;
  }
  public boolean isLoadOnStartup() {
    String tmp = coreProperties.getProperty(CORE_LOADONSTARTUP, "false");
    return Boolean.parseBoolean(tmp);
  }

  public void setLoadOnStartup(boolean loadOnStartup) {
    coreProperties.put(CORE_LOADONSTARTUP, Boolean.toString(loadOnStartup));
  }

  public boolean isTransient() {
    String tmp = coreProperties.getProperty(CORE_TRANSIENT, "false");
    return (Boolean.parseBoolean(tmp));
  }

  public void setTransient(boolean isTransient) {
    coreProperties.put(CORE_TRANSIENT, Boolean.toString(isTransient));
  }

  public String getUlogDir() {
    return coreProperties.getProperty(CORE_ULOGDIR);
  }

  public void setUlogDir(String ulogDir) {
    coreProperties.put(CORE_ULOGDIR, ulogDir);
  }

  /**
   * Reads a property defined in the core.properties file that's replacing solr.xml (if present).
   * @param prop    - value to read from the properties structure.
   * @param defVal  - return if no property found.
   * @return associated string. May be null.
   */
  public String getProperty(String prop, String defVal) {
    return coreProperties.getProperty(prop, defVal);
  }

  /**
   * gReads a property defined in the core.properties file that's replacing solr.xml (if present).
   * @param prop  value to read from the properties structure.
   * @return associated string. May be null.
   */
  public String getProperty(String prop) {
    return coreProperties.getProperty(prop);
  }
  /**
   * This will eventually replace _all_ of the setters. Puts a value in the "new" (obsoleting solr.xml JIRAs) properties
   * structures.
   *
   * Will replace any currently-existing property with the key "prop".
   *
   * @param prop - property name
   * @param val  - property value
   */
  public void putProperty(String prop, String val) {
    coreProperties.put(prop, val);
  }
}
