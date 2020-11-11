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

package org.apache.solr.core;

import org.apache.solr.api.ContainerPluginsRegistry;
import org.apache.solr.cloud.ClusterSingleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Helper class to manage the initial registration of {@link ClusterSingleton} plugins and
 * to track the changes in loaded plugins in {@link ContainerPluginsRegistry}.
 */
public class ClusterSingletons {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Map<String, ClusterSingleton> singletonMap = new ConcurrentHashMap<>();
  private final Supplier<Boolean> runSingletons;
  private final Consumer<Runnable> asyncRunner;
  private final ContainerPluginsRegistry.PluginRegistryListener pluginListener;

  public static final int DEFAULT_WAIT_TIMEOUT_SEC = 60;

  // we use this latch to delay the initial startup of singletons, due to
  // the leader election occurring in parallel with the rest of the load() method.
  private final CountDownLatch readyLatch = new CountDownLatch(1);

  /**
   * Create a helper to manage singletons.
   * @param runSingletons this function returns true when singletons should be running. It's
   *                      Used when adding or modifying existing plugins, and when invoking
   *                      {@link #startClusterSingletons()}.
   * @param asyncRunner async runner that will be used for starting up each singleton.
   */
  public ClusterSingletons(Supplier<Boolean> runSingletons, Consumer<Runnable> asyncRunner) {
    this.runSingletons = runSingletons;
    this.asyncRunner = asyncRunner;
    // create plugin registry listener
    pluginListener = new ContainerPluginsRegistry.PluginRegistryListener() {
      @Override
      public void added(ContainerPluginsRegistry.ApiInfo plugin) {
        if (plugin == null || plugin.getInstance() == null) {
          return;
        }
        // register new api
        Object instance = plugin.getInstance();
        if (instance instanceof ClusterSingleton) {
          ClusterSingleton singleton = (ClusterSingleton) instance;
          singletonMap.put(singleton.getName(), singleton);
          // check to see if we should immediately start this singleton
          if (isReady() && runSingletons.get()) {
            try {
              singleton.start();
            } catch (Exception exc) {
              log.warn("Exception starting ClusterSingleton {}: {}", plugin, exc);
            }
          }
        }
      }

      @Override
      public void deleted(ContainerPluginsRegistry.ApiInfo plugin) {
        if (plugin == null || plugin.getInstance() == null) {
          return;
        }
        Object instance = plugin.getInstance();
        if (instance instanceof ClusterSingleton) {
          ClusterSingleton singleton = (ClusterSingleton) instance;
          singleton.stop();
          singletonMap.remove(singleton.getName());
        }
      }

      @Override
      public void modified(ContainerPluginsRegistry.ApiInfo old, ContainerPluginsRegistry.ApiInfo replacement) {
        added(replacement);
        deleted(old);
      }
    };
  }

  public ContainerPluginsRegistry.PluginRegistryListener getPluginRegistryListener() {
    return pluginListener;
  }

  /**
   * Return modifiable registry of name / {@link ClusterSingleton}.
   */
  public Map<String, ClusterSingleton> getSingletons() {
    return singletonMap;
  }

  /**
   * Return true when this helper is ready to be used for singleton management.
   */
  public boolean isReady() {
    return readyLatch.getCount() == 0;
  }

  /**
   * Mark this helper as ready to be used for singleton management.
   */
  public void setReady() {
    readyLatch.countDown();
  }

  /**
   * Wait for this helper to become ready.
   * @param timeout timeout value.
   * @param timeUnit timeout unit.
   * @throws InterruptedException on this thread being interrupted.
   * @throws TimeoutException when specified timeout has elapsed but the helper is not ready.
   */
  public void waitUntilReady(long timeout, TimeUnit timeUnit)
      throws InterruptedException, TimeoutException {
    boolean await = readyLatch.await(timeout, timeUnit);
    if (!await) {
      throw new TimeoutException("Timed out waiting for ClusterSingletons to become ready.");
    }
  }

  /**
   * Start singletons when the helper is ready and when it's supposed to start
   * (as determined by {@link #runSingletons} function). If the helper is not ready this
   * method will use {@link #asyncRunner} to wait in another thread for
   * {@link #DEFAULT_WAIT_TIMEOUT_SEC} seconds.
   */
  public void startClusterSingletons() {
    final Runnable initializer = () -> {
      try {
        waitUntilReady(DEFAULT_WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        log.warn("Interrupted initialization of ClusterSingleton-s");
        return;
      } catch (TimeoutException te) {
        log.warn("Timed out during initialization of ClusterSingleton-s (waited {} sec)", DEFAULT_WAIT_TIMEOUT_SEC);
        return;
      }
      if (!runSingletons.get()) {
        return;
      }
      singletonMap.forEach((name, singleton) -> {
        if (!runSingletons.get()) {
          return;
        }
        try {
          singleton.start();
        } catch (Exception e) {
          log.warn("Exception starting ClusterSingleton {}: {}", singleton, e);
        }
      });
    };
    if (!isReady()) {
      // wait until all singleton-s are ready for the first startup
      asyncRunner.accept(initializer);
    } else {
      initializer.run();
    }
  }

  /**
   * Stop all registered singletons.
   */
  public void stopClusterSingletons() {
    singletonMap.forEach((name, singleton) -> {
      singleton.stop();
    });
  }
}
