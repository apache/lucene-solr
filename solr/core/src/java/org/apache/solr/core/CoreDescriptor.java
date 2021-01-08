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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Metadata about a {@link SolrCore}.
 * It's mostly loaded from a file on disk at the very beginning of loading a core.
 *
 * It's mostly but not completely immutable; we should fix this!
 *
 * @since solr 1.3
 */
public class CoreDescriptor {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  // Properties file name constants
  public static final String CORE_NAME = "name";
  public static final String CORE_CONFIG = "config";
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
  public static final String CORE_CONFIGSET_PROPERTIES = "configSetProperties";
  public static final String SOLR_CORE_PROP_PREFIX = "solr.core.";

  public static final String DEFAULT_EXTERNAL_PROPERTIES_FILE = "conf" + File.separator + "solrcore.properties";

  /**
   * Whether this core was configured using a configSet that was trusted.
   * This helps in avoiding the loading of plugins that have potential
   * vulnerabilities, when the configSet was not uploaded from a trusted
   * user.
   */
  private boolean trustedConfigSet = true;

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

  private static ImmutableMap<String, String> defaultProperties = new ImmutableMap.Builder<String, String>()
      .put(CORE_CONFIG, "solrconfig.xml")
      .put(CORE_SCHEMA, "schema.xml")
      .put(CORE_CONFIGSET_PROPERTIES, ConfigSetProperties.DEFAULT_FILENAME)
      .put(CORE_DATADIR, "data" + File.separator)
      .put(CORE_TRANSIENT, "false")
      .put(CORE_LOADONSTARTUP, "true")
      .build();

  private static ImmutableList<String> requiredProperties = ImmutableList.of(
      CORE_NAME
  );

  public static ImmutableList<String> standardPropNames = ImmutableList.of(
      CORE_NAME,
      CORE_CONFIG,
      CORE_DATADIR,
      CORE_ULOGDIR,
      CORE_SCHEMA,
      CORE_PROPERTIES,
      CORE_CONFIGSET_PROPERTIES,
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

  private final CloudDescriptor cloudDesc;

  private final Path instanceDir;

  /** The original standard core properties, before substitution */
  protected final Properties originalCoreProperties = new Properties();

  /** The original extra core properties, before substitution */
  protected final Properties originalExtraProperties = new Properties();

  /** The properties for this core, as available through getProperty() */
  protected final Properties coreProperties = new Properties();

  /** The properties for this core, substitutable by resource loaders */
  protected final Properties substitutableProperties = new Properties();

  /** TESTS ONLY */
  public CoreDescriptor(String name, Path instanceDir, CoreContainer coreContainer, String... coreProps) {
    this(name, instanceDir, toMap(coreProps), coreContainer.getContainerProperties(), coreContainer.getZkController());
  }

  private static Map<String, String> toMap(String... properties) {
    Map<String, String> props = new HashMap<>();
    assert properties.length % 2 == 0;
    for (int i = 0; i < properties.length; i += 2) {
      props.put(properties[i], properties[i+1]);
    }
    return props;
  }

  /**
   * Create a new CoreDescriptor using the properties of an existing one
   * @param coreName the new CoreDescriptor's name
   * @param other    the CoreDescriptor to copy
   */
  public CoreDescriptor(String coreName, CoreDescriptor other) {
    this.cloudDesc = other.cloudDesc;
    this.instanceDir = other.instanceDir;
    this.originalExtraProperties.putAll(other.originalExtraProperties);
    this.originalCoreProperties.putAll(other.originalCoreProperties);
    this.coreProperties.putAll(other.coreProperties);
    this.substitutableProperties.putAll(other.substitutableProperties);
    this.coreProperties.setProperty(CORE_NAME, coreName);
    this.originalCoreProperties.setProperty(CORE_NAME, coreName);
    this.substitutableProperties.setProperty(SOLR_CORE_PROP_PREFIX + CORE_NAME, coreName);
    this.trustedConfigSet = other.trustedConfigSet;
  }

  /**
   * Create a new CoreDescriptor.
   * @param name            the CoreDescriptor's name
   * @param instanceDir     a Path resolving to the instanceDir
   * @param coreProps       a Map of the properties for this core
   * @param containerProperties the properties from the enclosing container.
   * @param zkController    the ZkController in SolrCloud mode, otherwise null.
   */
  public CoreDescriptor(String name, Path instanceDir, Map<String, String> coreProps,
                        Properties containerProperties, ZkController zkController) {
    this.instanceDir = instanceDir.toAbsolutePath();

    originalCoreProperties.setProperty(CORE_NAME, name);

    name = PropertiesUtil.substituteProperty(checkPropertyIsNotEmpty(name, CORE_NAME),
                                             containerProperties);

    coreProperties.putAll(defaultProperties);
    coreProperties.put(CORE_NAME, name);

    for (Map.Entry<String, String> entry : coreProps.entrySet()) {
      String propname = entry.getKey();
      String propvalue = entry.getValue();

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
    if (zkController != null) {
      cloudDesc = new CloudDescriptor(this, name, coreProperties);
    } else {
      cloudDesc = null;
    }
    log.debug("Created CoreDescriptor: {}", coreProperties);
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
    Path propertiesFile = instanceDir.resolve(filename);
    if (Files.exists(propertiesFile)) {
      try (InputStream is = Files.newInputStream(propertiesFile)) {
        Properties externalProps = new Properties();
        externalProps.load(new InputStreamReader(is, StandardCharsets.UTF_8));
        coreProperties.putAll(externalProps);
      } catch (IOException e) {
        String message = String.format(Locale.ROOT, "Could not load properties from %s: %s:",
            propertiesFile.toString(), e.toString());
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, message);
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
    substitutableProperties.setProperty("solr.core.instanceDir", instanceDir.toAbsolutePath().toString());
  }

  /**
   * Is this property a Solr-standard property, or is it an extra property
   * defined per-core by the user?
   * @param propName the Property name
   * @return {@code true} if this property is user-defined
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

  public String getPropertiesName() {
    return coreProperties.getProperty(CORE_PROPERTIES);
  }

  public String getDataDir() {
    return coreProperties.getProperty(CORE_DATADIR);
  }
  
  public boolean usingDefaultDataDir() {
    return defaultProperties.get(CORE_DATADIR).equals(coreProperties.getProperty(CORE_DATADIR));
  }

  /** The core instance directory (absolute). */
  public Path getInstanceDir() {
    return instanceDir;
  }

  /**@return the core configuration resource name. */
  public String getConfigName() {
    return coreProperties.getProperty(CORE_CONFIG);
  }

  /**@return the core schema resource name.  Not actually used if schema is managed mode. */
  public String getSchemaName() {
    return coreProperties.getProperty(CORE_SCHEMA);
  }

  /**@return the initial core name */
  public String getName() {
    return coreProperties.getProperty(CORE_NAME);
  }

  /** TODO remove mutability */
  void setProperty(String prop, String val) {
    if (substitutableProperties.containsKey(prop)) {
      substitutableProperties.setProperty(prop, val);
      return;
    }
    coreProperties.setProperty(prop, val);
  }

  public String getCollectionName() {
    return cloudDesc == null ? null : cloudDesc.getCollectionName();
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
    return "CoreDescriptor[name=" + this.getName() + ";instanceDir=" + this.getInstanceDir() + "]";
  }

  public String getConfigSet() {
    //TODO consider falling back on "collection.configName" ( CollectionAdminParams.COLL_CONF )
    return coreProperties.getProperty(CORE_CONFIGSET);
  }

  /** TODO remove mutability or at least make this non-public? */
  public void setConfigSet(String configSetName) {
    coreProperties.setProperty(CORE_CONFIGSET, configSetName);
  }

  public String getConfigSetPropertiesName() {
    return coreProperties.getProperty(CORE_CONFIGSET_PROPERTIES);
  }

  public boolean isConfigSetTrusted() {
    return trustedConfigSet;
  }

  /** TODO remove mutability */
  public void setConfigSetTrusted(boolean trusted) {
    this.trustedConfigSet = trusted;
  }
}
