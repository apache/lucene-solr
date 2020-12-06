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
package org.apache.solr.cluster.events.impl;

import org.apache.solr.api.ContainerPluginsRegistry;
import org.apache.solr.cluster.events.ClusterEvent;
import org.apache.solr.cluster.events.ClusterEventListener;
import org.apache.solr.cluster.events.ClusterEventProducer;
import org.apache.solr.cluster.events.ClusterEventProducerBase;
import org.apache.solr.cluster.events.NoOpProducer;
import org.apache.solr.core.CoreContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Set;

/**
 * This class helps in handling the initial registration of plugin-based listeners,
 * when both the final {@link ClusterEventProducer} implementation and listeners
 * are configured using plugins.
 */
public class ClusterEventProducerFactory extends ClusterEventProducerBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private ContainerPluginsRegistry.PluginRegistryListener initialPluginListener;
  private boolean created = false;

  public ClusterEventProducerFactory(CoreContainer cc) {
    super(cc);
    // this initial listener is used only for capturing plugin registrations
    // done by other nodes while this CoreContainer is still loading
    initialPluginListener = new ContainerPluginsRegistry.PluginRegistryListener() {
      @Override
      public void added(ContainerPluginsRegistry.ApiInfo plugin) {
        if (plugin == null || plugin.getInstance() == null) {
          return;
        }
        Object instance = plugin.getInstance();
        if (instance instanceof ClusterEventListener) {
          registerListener((ClusterEventListener) instance);
        }
      }

      @Override
      public void deleted(ContainerPluginsRegistry.ApiInfo plugin) {
        if (plugin == null || plugin.getInstance() == null) {
          return;
        }
        Object instance = plugin.getInstance();
        if (instance instanceof ClusterEventListener) {
          unregisterListener((ClusterEventListener) instance);
        }
      }

      @Override
      public void modified(ContainerPluginsRegistry.ApiInfo old, ContainerPluginsRegistry.ApiInfo replacement) {
        added(replacement);
        deleted(old);
      }
    };
  }

  @Override
  public Set<ClusterEvent.EventType> getSupportedEventTypes() {
    return NoOpProducer.ALL_EVENT_TYPES;
  }

  /**
   * This method returns an initial plugin registry listener that helps to capture the
   * freshly loaded listener plugins before the final cluster event producer is created.
   * @return initial listener
   */
  public ContainerPluginsRegistry.PluginRegistryListener getPluginRegistryListener() {
    return initialPluginListener;
  }

  /**
   * Create a {@link ClusterEventProducer} based on the current plugin configurations.
   * <p>NOTE: this method can only be called once because it has side-effects, such as
   * transferring the initially collected listeners to the resulting producer's instance, and
   * installing a {@link org.apache.solr.api.ContainerPluginsRegistry.PluginRegistryListener}.
   * Calling this method more than once will result in an exception.</p>
   * @param plugins current plugin configurations
   * @return configured instance of cluster event producer (with side-effects, see above)
   */
  public DelegatingClusterEventProducer create(ContainerPluginsRegistry plugins) {
    if (created) {
      throw new RuntimeException("this factory can be called only once!");
    }
    final DelegatingClusterEventProducer clusterEventProducer = new DelegatingClusterEventProducer(cc);
    // since this is a ClusterSingleton, register it as such, under unique name
    cc.getClusterSingletons().getSingletons().put(ClusterEventProducer.PLUGIN_NAME +"_delegate", clusterEventProducer);
    ContainerPluginsRegistry.ApiInfo clusterEventProducerInfo = plugins.getPlugin(ClusterEventProducer.PLUGIN_NAME);
    if (clusterEventProducerInfo != null) {
      // the listener in ClusterSingletons already registered this instance
      clusterEventProducer.setDelegate((ClusterEventProducer) clusterEventProducerInfo.getInstance());
    } else {
      // use the default NoOp impl
    }
    // transfer those listeners that were already registered to the initial impl
    transferListeners(clusterEventProducer, plugins);

    // install plugin registry listener that maintains plugin-based listeners in
    // the event producer impl
    ContainerPluginsRegistry.PluginRegistryListener pluginListener = new ContainerPluginsRegistry.PluginRegistryListener() {
      @Override
      public void added(ContainerPluginsRegistry.ApiInfo plugin) {
        if (plugin == null || plugin.getInstance() == null) {
          return;
        }
        Object instance = plugin.getInstance();
        if (instance instanceof ClusterEventListener) {
          ClusterEventListener listener = (ClusterEventListener) instance;
          clusterEventProducer.registerListener(listener);
        } else if (instance instanceof ClusterEventProducer) {
          if (ClusterEventProducer.PLUGIN_NAME.equals(plugin.getInfo().name)) {
            // replace the existing impl
            if (cc.getClusterEventProducer() instanceof DelegatingClusterEventProducer) {
              ((DelegatingClusterEventProducer) cc.getClusterEventProducer())
                  .setDelegate((ClusterEventProducer) instance);
            } else {
              log.warn("Can't configure plugin-based ClusterEventProducer while CoreContainer is still loading - " +
                  " using existing implementation {}", cc.getClusterEventProducer().getClass().getName());
            }
          } else {
            log.warn("Ignoring ClusterEventProducer config with non-standard name: {}", plugin.getInfo());
          }
        }
      }

      @Override
      public void deleted(ContainerPluginsRegistry.ApiInfo plugin) {
        if (plugin == null || plugin.getInstance() == null) {
          return;
        }
        Object instance = plugin.getInstance();
        if (instance instanceof ClusterEventListener) {
          ClusterEventListener listener = (ClusterEventListener) instance;
          clusterEventProducer.unregisterListener(listener);
        } else if (instance instanceof ClusterEventProducer) {
          if (ClusterEventProducer.PLUGIN_NAME.equals(plugin.getInfo().name)) {
            // replace the existing impl with NoOp
            if (cc.getClusterEventProducer() instanceof DelegatingClusterEventProducer) {
              ((DelegatingClusterEventProducer) cc.getClusterEventProducer())
                  .setDelegate(new NoOpProducer(cc));
            } else {
              log.warn("Can't configure plugin-based ClusterEventProducer while CoreContainer is still loading - " +
                  " using existing implementation {}", cc.getClusterEventProducer().getClass().getName());
            }
          } else {
            log.warn("Ignoring ClusterEventProducer config with non-standard name: {}", plugin.getInfo());
          }
        }
      }

      @Override
      public void modified(ContainerPluginsRegistry.ApiInfo old, ContainerPluginsRegistry.ApiInfo replacement) {
        deleted(old);
        added(replacement);
      }
    };
    plugins.registerListener(pluginListener);
    created = true;
    return clusterEventProducer;
  }

  private void transferListeners(ClusterEventProducer target, ContainerPluginsRegistry plugins) {
    synchronized (listeners) {
      // stop capturing listener plugins
      plugins.unregisterListener(initialPluginListener);
      // transfer listeners that are already registered
      listeners.forEach((type, listenersSet) -> {
        listenersSet.forEach(listener -> target.registerListener(listener, type));
      });
      listeners.clear();
    }
  }

  @Override
  public void start() throws Exception {
    state = State.RUNNING;
  }

  @Override
  public void stop() {
    state = State.STOPPED;
  }
}
