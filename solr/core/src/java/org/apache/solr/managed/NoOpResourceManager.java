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

  private static final class NoOpResourceManagerPlugin implements ResourceManagerPlugin {
    static final NoOpResourceManagerPlugin INSTANCE = new NoOpResourceManagerPlugin();

    @Override
    public String getType() {
      return NOOP;
    }

    @Override
    public Collection<String> getMonitoredParams() {
      return Collections.emptySet();
    }

    @Override
    public Collection<String> getControlledParams() {
      return Collections.emptySet();
    }

    @Override
    public Map<String, Object> getMonitoredValues(ManagedComponent component) throws Exception {
      return Collections.emptyMap();
    }

    @Override
    public void setResourceLimit(ManagedComponent component, String limitName, Object value) throws Exception {
      // no-op
    }

    @Override
    public Map<String, Object> getResourceLimits(ManagedComponent component) throws Exception {
      return Collections.emptyMap();
    }

    @Override
    public void manage(ResourceManagerPool pool) throws Exception {
      // no-op
    }

    @Override
    public void init(Map params) {
      // no-op
    }
  }

  private static final class NoOpResourcePool implements ResourceManagerPool {
    static final NoOpResourcePool INSTANCE = new NoOpResourcePool();

    @Override
    public String getName() {
      return NOOP;
    }

    @Override
    public String getType() {
      return NOOP;
    }

    @Override
    public ResourceManagerPlugin getResourceManagerPlugin() {
      return NoOpResourceManagerPlugin.INSTANCE;
    }

    @Override
    public void registerComponent(ManagedComponent managedComponent) {
      // no-op
    }

    @Override
    public boolean unregisterComponent(String name) {
      return false;
    }

    @Override
    public boolean isRegistered(String componentId) {
      return false;
    }

    @Override
    public Map<String, ManagedComponent> getComponents() {
      return Collections.emptyMap();
    }

    @Override
    public Map<String, Map<String, Object>> getCurrentValues() throws InterruptedException {
      return Collections.emptyMap();
    }

    @Override
    public Map<String, Object> getPoolLimits() {
      return Collections.emptyMap();
    }

    @Override
    public Map<String, Object> getParams() {
      return Collections.emptyMap();
    }

    @Override
    public void setPoolLimits(Map<String, Object> poolLimits) {
      // no-op
    }

    @Override
    public PoolContext getPoolContext() {
      return null;
    }

    @Override
    public void close() throws IOException {
      // no-op
    }

    @Override
    public void run() {
      // no-op
    }
  }

  @Override
  protected void doInit() throws Exception {
    // no-op
  }

  @Override
  public void createPool(String name, String type, Map<String, Object> poolLimits, Map<String, Object> args) throws Exception {
    // no-op
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
