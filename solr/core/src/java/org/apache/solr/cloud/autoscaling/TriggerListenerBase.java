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
package org.apache.solr.cloud.autoscaling;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.core.SolrResourceLoader;

/**
 * Base class for implementations of {@link TriggerListener}.
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public abstract class TriggerListenerBase implements TriggerListener {

  protected AutoScalingConfig.TriggerListenerConfig config;
  protected SolrCloudManager cloudManager;
  protected SolrResourceLoader loader;
  protected boolean enabled;
  /**
   * Set of valid property names. Subclasses may add to this set
   * using {@link TriggerUtils#validProperties(Set, String...)}
   */
  protected final Set<String> validProperties = new HashSet<>();
  /**
   * Set of required property names. Subclasses may add to this set
   * using {@link TriggerUtils#requiredProperties(Set, Set, String...)}
   * (required properties are also valid properties).
   */
  protected final Set<String> requiredProperties = new HashSet<>();
  /**
   * Subclasses can add to this set if they want to allow arbitrary properties that
   * start with one of valid prefixes.
   */
  protected final Set<String> validPropertyPrefixes = new HashSet<>();

  protected TriggerListenerBase() {
    TriggerUtils.requiredProperties(requiredProperties, validProperties, "trigger");
    TriggerUtils.validProperties(validProperties, "name", "class", "stage", "beforeAction", "afterAction", "enabled");
  }

  @Override
  public void configure(SolrResourceLoader loader, SolrCloudManager cloudManager, AutoScalingConfig.TriggerListenerConfig config) throws TriggerValidationException {
    this.loader = loader;
    this.cloudManager = cloudManager;
    this.config = config;
    this.enabled = Boolean.parseBoolean(String.valueOf(config.properties.getOrDefault("enabled", true)));
    // validate the config
    Map<String, String> results = new HashMap<>();
    // prepare a copy to treat the prefix-based properties
    Map<String, Object> propsToCheck = new HashMap<>(config.properties);
    propsToCheck.keySet().removeIf(k ->
      validPropertyPrefixes.stream().anyMatch(p -> k.startsWith(p)));
    TriggerUtils.checkProperties(propsToCheck, results, requiredProperties, validProperties);
    if (!results.isEmpty()) {
      throw new TriggerValidationException(config.name, results);
    }
  }

  @Override
  public AutoScalingConfig.TriggerListenerConfig getConfig() {
    return config;
  }

  @Override
  public boolean isEnabled() {
    return enabled;
  }

  @Override
  public void init() throws Exception {

  }

  @Override
  public void close() throws IOException {

  }
}
