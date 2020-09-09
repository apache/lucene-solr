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
 * ({@link PlacementPlugin}) created by the factory can easily retrieve their configuration.
 */
public interface PlacementPluginConfig {
  /**
   * @return the configured {@link String} value corresponding to {@code configName} (a {@code str} in {@code solr.xml})
   * if one exists (could be the empty string) and {@code null} otherwise.
   */
  String getStringConfig(String configName);

  /**
   * @return the configured {@link Boolean} value corresponding to {@code configName} (a {@code bool} in {@code solr.xml})
   * if one exists, {@code null} otherwise.
   */
  Boolean getBooleanConfig(String configName);

  /**
   * @return the configured {@link Integer} value corresponding to {@code configName} (an {@code int} in {@code solr.xml})
   * if one exists, {@code null} otherwise.
   */
  Integer getIntegerConfig(String configName);

  /**
   * @return the configured {@link Long} value corresponding to {@code configName} (a {@code long} in {@code solr.xml})
   * if one exists, {@code null} otherwise.
   */
  Long getLongConfig(String configName);

  /**
   * @return the configured {@link Float} value corresponding to {@code configName} (a {@code float} in {@code solr.xml})
   * if one exists, {@code null} otherwise.
   */
  Float getFloatConfig(String configName);

  /**
   * @return the configured {@link Double} value corresponding to {@code configName} (a {@code double} in {@code solr.xml})
   * if one exists, {@code null} otherwise.
   */
  Double getDoubleConfig(String configName);
}
