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
package org.apache.solr.metrics;

import java.io.Closeable;

import org.apache.solr.core.PluginInfo;
import org.apache.solr.util.SolrPluginUtils;
import org.apache.solr.util.plugin.PluginInfoInitialized;

/**
 * Interface for 'pluggable' metric reporters.
 */
public abstract class SolrMetricReporter implements Closeable, PluginInfoInitialized {

  protected final String registryName;
  protected final SolrMetricManager metricManager;
  protected PluginInfo pluginInfo;

  /**
   * Create a reporter for metrics managed in a named registry.
   * @param registryName registry to use, one of registries managed by
   *                     {@link SolrMetricManager}
   */
  protected SolrMetricReporter(SolrMetricManager metricManager, String registryName) {
    this.registryName = registryName;
    this.metricManager = metricManager;
  }

  /**
   * Initializes a {@link SolrMetricReporter} with the plugin's configuration.
   *
   * @param pluginInfo the plugin's configuration
   */
  @SuppressWarnings("unchecked")
  public void init(PluginInfo pluginInfo) {
    if (pluginInfo != null) {
      this.pluginInfo = pluginInfo.copy();
      if (this.pluginInfo.initArgs != null) {
        SolrPluginUtils.invokeSetters(this, this.pluginInfo.initArgs);
      }
    }
    validate();
  }

  /**
   * Get the effective {@link PluginInfo} instance that was used for
   * initialization of this plugin.
   * @return plugin info, or null if not yet initialized.
   */
  public PluginInfo getPluginInfo() {
    return pluginInfo;
  }

  /**
   * Validates that the reporter has been correctly configured.
   *
   * @throws IllegalStateException if the reporter is not properly configured
   */
  protected abstract void validate() throws IllegalStateException;

  @Override
  public String toString() {
    return getClass().getName() + "{" +
        "registryName='" + registryName + '\'' +
        ", pluginInfo=" + pluginInfo +
        '}';
  }
}
