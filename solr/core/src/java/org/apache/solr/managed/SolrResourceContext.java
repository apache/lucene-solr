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
package org.apache.solr.managed;

import java.io.Closeable;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resource context contains information needed for proper registration and un-registration of a component.
 */
public class SolrResourceContext implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final ResourceManager resourceManager;
  private final String[] poolNames;
  private final ManagedComponent component;

  /**
   * Create a resource context. This constructor also registers the component in specified pool(s).
   * @param resourceManager resource manager instance
   * @param component component instance
   * @param poolName required primary pool name
   * @param otherPools optional additional pools (may be null or empty)
   */
  public SolrResourceContext(ResourceManager resourceManager, ManagedComponent component, String poolName, String... otherPools) {
    Objects.nonNull(resourceManager);
    Objects.nonNull(component);
    Objects.nonNull(poolName);
    this.resourceManager = resourceManager;
    Set<String> pools = new LinkedHashSet<>();
    pools.add(poolName);
    if (otherPools != null) {
      Collections.addAll(pools, otherPools);
    }
    this.poolNames = (String[])pools.toArray(new String[pools.size()]);
    this.component = component;
    for (String pool : poolNames) {
      this.resourceManager.registerComponent(pool, component);
    }
  }

  public ResourceManager getResourceManager() {
    return resourceManager;
  }

  /**
   * Return all pool names where the component was registered.
   */
  public String[] getPoolNames() {
    return poolNames;
  }

  @Override
  public void close() {
    log.debug("-- closing managed component " + component.getManagedComponentId() + " in pools " + Arrays.toString(poolNames));
    for (String poolName : poolNames) {
      resourceManager.unregisterComponent(poolName, component.getManagedComponentId());
    }
  }
}
