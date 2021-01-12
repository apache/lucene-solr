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

import org.apache.solr.api.ConfigurablePlugin;

/**
 * Factory implemented by client code and configured in container plugins
 * (see {@link org.apache.solr.handler.admin.ContainerPluginsApi#editAPI})
 * allowing the creation of instances of
 * {@link PlacementPlugin} to be used for replica placement computation.
 * <p>Note: configurable factory implementations should also implement
 * {@link org.apache.solr.api.ConfigurablePlugin} with the appropriate configuration
 * bean type.</p>
 */
public interface PlacementPluginFactory<T extends PlacementPluginConfig> extends ConfigurablePlugin<T> {
  /**
   * The key in the plugins registry under which this plugin and its configuration are defined.
   */
  String PLUGIN_NAME = ".placement-plugin";

  /**
   * Returns an instance of the plugin that will be repeatedly (and concurrently) called to compute placement. Multiple
   * instances of a plugin can be used in parallel (for example if configuration has to change, but plugin instances with
   * the previous configuration are still being used).
   * <p>If this method returns null then a simple legacy assignment strategy will be used
   * (see {@link org.apache.solr.cloud.api.collections.Assign.LegacyAssignStrategy}).</p>
   */
  PlacementPlugin createPluginInstance();

  /**
   * Default implementation is a no-op. Override to provide meaningful
   * behavior if needed.
   * @param cfg value deserialized from JSON, not null.
   */
  @Override
  default void configure(T cfg) {
    // no-op
  }

  /**
   * Return the configuration of the plugin.
   * Default implementation returns null.
   */
  default T getConfig() {
    return null;
  }

  /**
   * Useful type for plugins that don't use any configuration.
   */
  class NoConfig implements PlacementPluginConfig {
  }
}
