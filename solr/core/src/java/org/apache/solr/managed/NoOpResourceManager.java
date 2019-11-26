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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 *
 */
public class NoOpResourceManager extends ResourceManager {
  public static final String NOOP = "--noop--";

  public static final NoOpResourceManager INSTANCE = new NoOpResourceManager();

  public static class NoOpManagedComponent implements ManagedComponent {
    @Override
    public ManagedComponentId getManagedComponentId() {
      return ManagedComponentId.of(NOOP);
    }

    @Override
    public void initializeManagedComponent(ResourceManager resourceManager, String poolName, String... otherPools) {

    }

    @Override
    public SolrResourceContext getSolrResourceContext() {
      return null;
    }
  }

  public static final class NoOpResourcePool<NoOpManagedComponent> extends ResourceManagerPool {
    static final NoOpResourcePool<NoOpResourceManager.NoOpManagedComponent> INSTANCE =
        new NoOpResourcePool<>(NoOpResourceManager.INSTANCE, Collections.emptyMap(), Collections.emptyMap());

    public NoOpResourcePool(ResourceManager resourceManager, Map poolLimits, Map poolParams) {
      super(NOOP, NOOP, resourceManager, poolLimits, poolParams);
    }

    @Override
    public Map<String, Object> getMonitoredValues(ManagedComponent component) {
      return Collections.emptyMap();
    }

    @Override
    protected Object doSetResourceLimit(ManagedComponent component, String limitName, Object value) throws Exception {
      return value;
    }

    @Override
    public Map<String, Object> getResourceLimits(ManagedComponent component) throws Exception {
      return Collections.emptyMap();
    }

    @Override
    protected void doManage() throws Exception {

    }
  }

  @Override
  protected void doInit() throws Exception {
    // no-op
  }

  @Override
  public ResourceManagerPoolFactory getResourceManagerPoolFactory() {
    return null;
  }

  @Override
  public ResourceManagerPool createPool(String name, String type, Map<String, Object> poolLimits, Map<String, Object> args) throws Exception {
    return NoOpResourcePool.INSTANCE;
  }

  @Override
  public Collection<String> listPools() {
    return Collections.singleton(NoOpResourcePool.INSTANCE.getName());
  }

  @Override
  public ResourceManagerPool getPool(String name) {
    return NoOpResourcePool.INSTANCE;
  }

  @Override
  public void setPoolLimits(String name, Map<String, Object> poolLimits) throws Exception {
    // no-op
  }

  @Override
  public void removePool(String name) throws Exception {
    // no-op
  }

  @Override
  public void registerComponent(String pool, ManagedComponent managedComponent) {
    // no-op
  }

  @Override
  public boolean unregisterComponent(String pool, String componentId) {
    return false;
  }

  @Override
  public void close() throws IOException {
    // no-op
  }
}
