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

import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.core.SolrResourceLoader;

/**
 * Base class for {@link TriggerAction} implementations.
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public abstract class TriggerActionBase implements TriggerAction {

  protected Map<String, Object> properties = new HashMap<>();
  protected SolrResourceLoader loader;
  protected SolrCloudManager cloudManager;
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

  protected TriggerActionBase() {
    // not strictly needed here because they are already checked during instantiation
    TriggerUtils.validProperties(validProperties, "name", "class");
  }

  @Override
  public String getName() {
    String name = (String) properties.get("name");
    if (name != null) {
      return name;
    } else {
      return getClass().getSimpleName();
    }
  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public void configure(SolrResourceLoader loader, SolrCloudManager cloudManager, Map<String, Object> properties) throws TriggerValidationException {
    this.loader = loader;
    this.cloudManager = cloudManager;
    if (properties != null) {
      this.properties.putAll(properties);
    }
    // validate the config
    Map<String, String> results = new HashMap<>();
    TriggerUtils.checkProperties(this.properties, results, requiredProperties, validProperties);
    if (!results.isEmpty()) {
      throw new TriggerValidationException(getName(), results);
    }
  }

  @Override
  public void init() throws Exception {

  }
}
