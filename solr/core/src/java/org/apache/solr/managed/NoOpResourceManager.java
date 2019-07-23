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

  private static final class NoOpResourcePool implements ResourceManagerPool {
    static NoOpResourcePool INSTANCE = new NoOpResourcePool();

    @Override
    public String getName() {
      return NOOP;
    }

    @Override
    public String getType() {
      return NOOP;
    }

    @Override
    public void addResource(ManagedResource managedResource) {

    }

    @Override
    public boolean removeResource(String name) {
      return false;
    }

    @Override
    public Map<String, ManagedResource> getResources() {
      return Collections.emptyMap();
    }

    @Override
    public Map<String, Map<String, Object>> getCurrentValues() throws InterruptedException {
      return Collections.emptyMap();
    }

    @Override
    public Map<String, Float> getTotalValues() throws InterruptedException {
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

    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void run() {

    }
  }

  @Override
  protected void doInit() throws Exception {

  }

  @Override
  public void createPool(String name, String type, Map<String, Object> poolLimits, Map<String, Object> args) throws Exception {

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

  }

  @Override
  public void removePool(String name) throws Exception {

  }

  @Override
  public void addResource(String pool, ManagedResource managedResource) throws Exception {

  }

  @Override
  public boolean removeResource(String pool, String resourceId) {
    return false;
  }

  @Override
  public void close() throws IOException {

  }
}
