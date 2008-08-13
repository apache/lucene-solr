/**
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


/**
 * A Solr core descriptor
 * 
 * @since solr 1.3
 */
public class CoreDescriptor implements Cloneable {
  protected String name;
  protected String instanceDir;
  protected String configName;
  protected String schemaName;
  protected SolrCore core = null;
  private final CoreContainer coreContainer;

  public CoreDescriptor(CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
  }

  /** Initialize defaults from instance directory. */
  public void init(String name, String instanceDir) {
    if (name == null) {
      throw new RuntimeException("Core needs a name");
    }
    if (instanceDir == null) {
      throw new NullPointerException("Missing required \'instanceDir\'");
    }
    this.name = name;
    if (!instanceDir.endsWith("/")) instanceDir = instanceDir + "/";
    this.instanceDir = instanceDir;
    this.configName = getDefaultConfigName();
    this.schemaName = getDefaultSchemaName();
  }

  public CoreDescriptor(CoreDescriptor descr) {
    this.name = descr.name;
    this.instanceDir = descr.instanceDir;
    this.configName = descr.configName;
    this.schemaName = descr.schemaName;
    coreContainer = descr.coreContainer;
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
    return this.instanceDir + "data/";
  }
  
  /**@return the core name. */
  public String getName() {
    return name;
  }
  
  /** Sets the core name. */
  public void setName(String name) {
    this.name = name;
  }
  
  /**@return the core instance directory. */
  public String getInstanceDir() {
    return instanceDir;
  }
  
  /**Sets the core configuration resource name. */
  public void setConfigName(String name) {
    if (name == null || name.length() == 0)
      throw new IllegalArgumentException("name can not be null or empty");
    this.configName = name;
  }
  
  /**@return the core configuration resource name. */
  public String getConfigName() {
    return this.configName;
  }

  /**Sets the core schema resource name. */
  public void setSchemaName(String name) {
    if (name == null || name.length() == 0)
      throw new IllegalArgumentException("name can not be null or empty");
    this.schemaName = name;
  }
  
  /**@return the core schema resource name. */
  public String getSchemaName() {
    return this.schemaName;
  }
  
  public SolrCore getCore() {
    return core;
  }
  
  public void setCore(SolrCore core) {
    this.core = core;
  }

  public CoreContainer getMultiCore() {
    return coreContainer;
  }
}
