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

import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.PluginInfo;

/**
 * A {@link SolrMetricReporter} that has access to its {@link CoreContainer}.
 */
abstract public class SolrCoreContainerReporter extends SolrMetricReporter {

  protected CoreContainer coreContainer;

  protected SolrCoreContainerReporter(SolrMetricManager metricManager, String registryName) {
    super(metricManager, registryName);
  }

  @Override
  final public void init(PluginInfo pluginInfo) {
    throw new UnsupportedOperationException(getClass().getCanonicalName()+".init(PluginInfo) is not supported, use init(PluginInfo,CoreContainer) instead.");
  }

  public void init(PluginInfo pluginInfo, CoreContainer coreContainer) {
    super.init(pluginInfo);
    this.coreContainer = coreContainer;
  }

  public CoreContainer getCoreContainer() {
    return coreContainer;
  }

}
