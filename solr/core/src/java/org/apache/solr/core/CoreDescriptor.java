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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang.StringUtils;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.util.IOUtils;
import org.apache.solr.util.PropertiesUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkNotNull;

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
  public static final String CORE_ABS_INSTDIR = "absoluteInstDir";
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
  public static final String CORE_CONFIGSET = "configSet";
  public static final String SOLR_CORE_PROP_PREFIX = "solr.core.";

  public static final String DEFAULT_EXTERNAL_PROPERTIES_FILE = "conf" + File.separator + "solrcore.properties";

  /**
   * Get the standard properties in persistable form
   * @return the standard core properties in persistable form
   */
  public Properties getPersistableStandardProperties() {
    return originalCoreProperties;
  }

  /**
   * Get user-defined core properties in persistable form
   * @return user-defined core properties in persistable form
   */
  public Properties getPersistableUserProperties() {
    return originalExtraProperties;
  }

  private static ImmutableMap<String, String> defaultProperties = ImmutableMap.of(
      CORE_CONFIG, "solrconfig.xml",
      CORE_SCHEMA, "schema.xml",
      CORE_DATADIR, "data" + File.separator,
      CORE_TRANSIENT, "false",
      CORE_LOADONSTARTUP, "true"
  );

  private static ImmutableList<String> requiredProperties = ImmutableList.of(
      CORE_NAME, CORE_INSTDIR, CORE_ABS_INSTDIR
  );

  public static ImmutableList<String> standardPropNames = ImmutableList.of(
      CORE_NAME,
      CORE_CONFIG,
      CORE_INSTDIR,
      CORE_DATADIR,
      CORE_ULOGDIR,
      CORE_SCHEMA,
      CORE_PROPERTIES,
      CORE_LOADONSTARTUP,
      CORE_TRANSIENT,
      CORE_CONFIGSET,
      // cloud props
      CORE_SHARD,
      CORE_COLLECTION,
      CORE_ROLES,
      CORE_NODE_NAME,
      CloudDescriptor.NUM_SHARDS
  );

  private final CoreContainer coreContainer;

  private final CloudDescriptor cloudDesc;

  /** The original standard core properties, before substitution */
  protected final Properties originalCoreProperties = new Properties();

  /** The original extra core properties, before substitution */
  protected final Properties originalExtraProperties = new Properties();

  /** The properties for this core, as available through getProperty() */
  protected final Properties coreProperties = new Properties();

  /** The properties for this core, substitutable by resource loaders */
  protected final Properties substitutableProperties = new Properties();

  /**
   * Create a new CoreDescriptor.
   * @param container       the CoreDescriptor's container
   * @param name            the CoreDescriptor's name
   * @param instanceDir     a String containing the instanceDir
   * @param coreProps       a Properties object of the properties for this core
   */
  public CoreDescriptor(CoreContainer container, String name, String instanceDir,
                        Properties coreProps) {
    this(container, name, instanceDir, coreProps, null);
  }
  
  /**
   * Create a new CoreDescriptor.
   * @param container       the CoreDescriptor's container
   * @param name            the CoreDescriptor's name
   * @param instanceDir     a String containing the instanceDir
   * @param coreProps       a Properties object of the properties for this core
   * @param params          additional params
   */
  public CoreDescriptor(CoreContainer container, String name, String instanceDir,
                        Properties coreProps, SolrParams params) {

    this.coreContainer = container;

    originalCoreProperties.setProperty(CORE_NAME, name);
    originalCoreProperties.setProperty(CORE_INSTDIR, instanceDir);

    Properties containerProperties = container.getContainerProperties();
    name = PropertiesUtil.substituteProperty(checkPropertyIsNotEmpty(name, CORE_NAME),
                                             containerProperties);
    instanceDir = PropertiesUtil.substituteProperty(checkPropertyIsNotEmpty(instanceDir, CORE_INSTDIR),
                                                    containerProperties);

    coreProperties.putAll(defaultProperties);
    coreProperties.put(CORE_NAME, name);
    coreProperties.put(CORE_INSTDIR, instanceDir);
    coreProperties.put(CORE_ABS_INSTDIR, convertToAbsolute(instanceDir, container.getCoreRootDirectory()));

    for (String propname : coreProps.stringPropertyNames()) {

      String propvalue = coreProps.getProperty(propname);

      if (isUserDefinedProperty(propname))
        originalExtraProperties.put(propname, propvalue);
      else
        originalCoreProperties.put(propname, propvalue);

      if (!requiredProperties.contains(propname))   // Required props are already dealt with
        coreProperties.setProperty(propname,
            PropertiesUtil.substituteProperty(propvalue, containerProperties));
    }

    loadExtraProperties();
    buildSubstitutableProperties();

    // TODO maybe make this a CloudCoreDescriptor subclass?
    if (container.isZooKeeperAware()) {
      cloudDesc = new CloudDescriptor(name, coreProperties, this);
      if (params != null) {
        cloudDesc.setParams(params);
      }
    }
    else {
      cloudDesc = null;
    }
  }

  /**
   * Load properties specified in an external properties file.
   *
   * The file to load can be specified in a {@code properties} property on
   * the original Properties object used to create this CoreDescriptor.  If
   * this has not been set, then we look for {@code conf/solrcore.properties}
   * underneath the instance dir.
   *
   * File paths are taken as read from the core's instance directory
   * if they are not absolute.
   */
  protected void loadExtraProperties() {
    String filename = coreProperties.getProperty(CORE_PROPERTIES, DEFAULT_EXTERNAL_PROPERTIES_FILE);
    File propertiesFile = resolvePaths(filename);
    if (propertiesFile.exists()) {
      FileInputStream in = null;
      try {
        in = new FileInputStream(propertiesFile);
        Properties externalProps = new Properties();
        externalProps.load(new InputStreamReader(in, StandardCharsets.UTF_8));
        coreProperties.putAll(externalProps);
      } catch (IOException e) {
        String message = String.format(Locale.ROOT, "Could not load properties from %s: %s:",
            propertiesFile.getAbsoluteFile(), e.toString());
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, message);
      } finally {
        IOUtils.closeQuietly(in);
      }
    }
  }

  /**
   * Create the properties object used by resource loaders, etc, for property
   * substitution.  The default solr properties are prefixed with 'solr.core.', so,
   * e.g., 'name' becomes 'solr.core.name'
   */
  protected void buildSubstitutableProperties() {
    for (String propName : coreProperties.stringPropertyNames()) {
      String propValue = coreProperties.getProperty(propName);
      if (!isUserDefinedProperty(propName))
        propName = SOLR_CORE_PROP_PREFIX + propName;
      substitutableProperties.setProperty(propName, propValue);
    }
  }

  protected File resolvePaths(String filepath) {
    File file = new File(filepath);
    if (file.isAbsolute())
      return file;
    return new File(getInstanceDir(), filepath);
  }

  /**
   * Is this property a Solr-standard property, or is it an extra property
   * defined per-core by the user?
   * @param propName the Property name
   * @return @{code true} if this property is user-defined
   */
  protected static boolean isUserDefinedProperty(String propName) {
    return !standardPropNames.contains(propName);
  }

  public static String checkPropertyIsNotEmpty(String value, String propName) {
    if (StringUtils.isEmpty(value)) {
      String message = String.format(Locale.ROOT, "Cannot create core with empty %s value", propName);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, message);
    }
    return value;
  }

  /**
   * Create a new CoreDescriptor with a given name and instancedir
   * @param container     the CoreDescriptor's container
   * @param name          the CoreDescriptor's name
   * @param instanceDir   the CoreDescriptor's instancedir
   */
  public CoreDescriptor(CoreContainer container, String name, String instanceDir) {
    this(container, name, instanceDir, new Properties());
  }

  /**
   * Create a new CoreDescriptor using the properties of an existing one
   * @param coreName the new CoreDescriptor's name
   * @param other    the CoreDescriptor to copy
   */
  public CoreDescriptor(String coreName, CoreDescriptor other) {
    this.coreContainer = other.coreContainer;
    this.cloudDesc = other.cloudDesc;
    this.originalExtraProperties.putAll(other.originalExtraProperties);
    this.originalCoreProperties.putAll(other.originalCoreProperties);
    this.coreProperties.putAll(other.coreProperties);
    this.substitutableProperties.putAll(other.substitutableProperties);
    this.coreProperties.setProperty(CORE_NAME, coreName);
    this.originalCoreProperties.setProperty(CORE_NAME, coreName);
    this.substitutableProperties.setProperty(SOLR_CORE_PROP_PREFIX + CORE_NAME, coreName);
  }

  public String getPropertiesName() {
    return coreProperties.getProperty(CORE_PROPERTIES);
  }

  public String getDataDir() {
    return coreProperties.getProperty(CORE_DATADIR);
  }
  
  public boolean usingDefaultDataDir() {
    return defaultProperties.get(CORE_DATADIR).equals(coreProperties.getProperty(CORE_DATADIR));
  }

  /**@return the core instance directory. */
  public String getRawInstanceDir() {
    return coreProperties.getProperty(CORE_INSTDIR);
  }

  private static String convertToAbsolute(String instDir, String solrHome) {
    checkNotNull(instDir);
    File f = new File(instDir);
    if (f.isAbsolute())
      return SolrResourceLoader.normalizeDir(instDir);
    return SolrResourceLoader.normalizeDir(solrHome + SolrResourceLoader.normalizeDir(instDir));
  }

  /**
   *
   * @return the core instance directory, prepended with solr_home if not an absolute path.
   */
  public String getInstanceDir() {
    return coreProperties.getProperty(CORE_ABS_INSTDIR);
  }

  /**@return the core configuration resource name. */
  public String getConfigName() {
    return coreProperties.getProperty(CORE_CONFIG);
  }

  /**@return the core schema resource name. */
  public String getSchemaName() {
    return coreProperties.getProperty(CORE_SCHEMA);
  }

  /**@return the initial core name */
  public String getName() {
    return coreProperties.getProperty(CORE_NAME);
  }

  public String getCollectionName() {
    return cloudDesc == null ? null : cloudDesc.getCollectionName();
  }

  public CoreContainer getCoreContainer() {
    return coreContainer;
  }

  public CloudDescriptor getCloudDescriptor() {
    return cloudDesc;
  }

  public boolean isLoadOnStartup() {
    String tmp = coreProperties.getProperty(CORE_LOADONSTARTUP, "false");
    return Boolean.parseBoolean(tmp);
  }

  public boolean isTransient() {
    String tmp = coreProperties.getProperty(CORE_TRANSIENT, "false");
    return PropertiesUtil.toBoolean(tmp);
  }

  public String getUlogDir() {
    return coreProperties.getProperty(CORE_ULOGDIR);
  }

  /**
   * Returns a specific property defined on this CoreDescriptor
   * @param prop    - value to read from the properties structure.
   * @param defVal  - return if no property found.
   * @return associated string. May be null.
   */
  public String getCoreProperty(String prop, String defVal) {
    return coreProperties.getProperty(prop, defVal);
  }

  /**
   * Returns all substitutable properties defined on this CoreDescriptor
   * @return all substitutable properties defined on this CoreDescriptor
   */
  public Properties getSubstitutableProperties() {
    return substitutableProperties;
  }

  @Override
  public String toString() {
    return new StringBuilder("CoreDescriptor[name=")
        .append(this.getName())
        .append(";instanceDir=")
        .append(this.getInstanceDir())
        .append("]")
        .toString();
  }

  public String getConfigSet() {
    return coreProperties.getProperty(CORE_CONFIGSET);
  }
}
