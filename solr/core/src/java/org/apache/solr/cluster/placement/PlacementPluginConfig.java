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

package org.apache.solr.cluster.placement;

/**
 * <p>Configuration passed by Solr to {@link PlacementPluginFactory#createPluginInstance(PlacementPluginConfig)} so that plugin instances
 * ({@link PlacementPlugin}) created by the factory can easily retrieve their configuration.</p>
 *
 * <p>A plugin writer decides the names and the types of the configurable parameters it needs. Available types are
 * {@link String}, {@link Long}, {@link Boolean}, {@link Double}. This configuration currently lives in the {@code /clusterprops.json}
 * file in Zookeeper (this could change in the future, the plugin code will not change but the way to store its configuration
 * in the cluster might). {@code clusterprops.json} also contains the name of the plugin factory class implementing
 * {@link org.apache.logging.log4j.core.config.plugins.PluginBuilderFactory}.</p>
 *
 * <p>In order to configure a plugin to be used for placement decisions, the following {@code curl} command (or something
 * equivalent) has to be executed once the cluster is already running to set the configuration.
 * Replace {@code localhost:8983} by one of your servers' IP address and port.</p>
 *
 * <pre>
 *
 * curl -X POST -H 'Content-type:application/json' -d '{
 *   "set-placement-plugin": {
 *     "class": "factory.class.name$inner",
 *     "myfirstString": "a text value",
 *     "aLong": 50,
 *     "aDoubleConfig": 3.1415928,
 *     "shouldIStay": true
 *   }
 * }' http://localhost:8983/api/cluster
 * </pre>
 *
 * <p>The consequence will be the creation (or replacement if it exists) of an element in the Zookeeper file
 * {@code /clusterprops.json} as follows:</p>
 *
 * <pre>
 *
 * "placement-plugin":{
 *     "class":"factory.class.name$inner",
 *     "myfirstString": "a text value",
 *     "aLong": 50,
 *     "aDoubleConfig": 3.1415928,
 *     "shouldIStay": true}
 * </pre>
 *
 * <p>In order to delete the placement-plugin section from {@code /clusterprops.json} (and to fallback to either Legacy
 * or rule based placement if so configured for a collection), execute:</p>
 *
 * <pre>
 *
 * curl -X POST -H 'Content-type:application/json' -d '{
 *   "set-placement-plugin" : null
 * }' http://localhost:8983/api/cluster
 * </pre>
 */
public interface PlacementPluginConfig {

  /**
   * The key in {@code clusterprops.json} under which the plugin factory and the plugin configuration are defined.
   */
  String PLACEMENT_PLUGIN_CONFIG_KEY = "placement-plugin";
  /**
   * Name of the property containing the factory class
   */
  String FACTORY_CLASS = "class";

  /**
   * @return the configured {@link String} value corresponding to {@code configName} if one exists (could be the empty
   * string) and {@code null} otherwise.
   */
  String getStringConfig(String configName);

  /**
   * @return the configured {@link String} value corresponding to {@code configName} if one exists (could be the empty
   * string) and {@code defaultValue} otherwise.
   */
  String getStringConfig(String configName, String defaultValue);

  /**
   * @return the configured {@link Boolean} value corresponding to {@code configName} if one exists, {@code null} otherwise.
   */
  Boolean getBooleanConfig(String configName);

  /**
   * @return the configured {@link Boolean} value corresponding to {@code configName} if one exists, a boxed {@code defaultValue}
   * otherwise (this method never returns {@code null}.
   */
  Boolean getBooleanConfig(String configName, boolean defaultValue);

  /**
   * @return the configured {@link Long} value corresponding to {@code configName} if one exists, {@code null} otherwise.
   */
  Long getLongConfig(String configName);

  /**
   * @return the configured {@link Long} value corresponding to {@code configName} if one exists, a boxed {@code defaultValue}
   * otherwise (this method never returns {@code null}.
   */
  Long getLongConfig(String configName, long defaultValue);

  /**
   * @return the configured {@link Double} value corresponding to {@code configName} if one exists, {@code null} otherwise.
   */
  Double getDoubleConfig(String configName);

  /**
   * @return the configured {@link Double} value corresponding to {@code configName} if one exists, a boxed {@code defaultValue}
   * otherwise (this method never returns {@code null}.
   */
  Double getDoubleConfig(String configName, double defaultValue);
}
