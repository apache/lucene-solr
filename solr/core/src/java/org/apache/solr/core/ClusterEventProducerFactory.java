package org.apache.solr.core;

import org.apache.solr.api.CustomContainerPlugins;
import org.apache.solr.cluster.events.ClusterEvent;
import org.apache.solr.cluster.events.ClusterEventListener;
import org.apache.solr.cluster.events.ClusterEventProducer;
import org.apache.solr.cluster.events.impl.DefaultClusterEventProducer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This class helps in handling the initial registration of plugin-based listeners,
 * when both the final {@link ClusterEventProducer} implementation and listeners
 * are configured using plugins.
 */
public class ClusterEventProducerFactory implements ClusterEventProducer {
  private Map<ClusterEvent.EventType, Set<ClusterEventListener>> initialListeners = new HashMap<>();
  private CustomContainerPlugins.PluginRegistryListener initialPluginListener;
  private final CoreContainer cc;
  private boolean created = false;

  public ClusterEventProducerFactory(CoreContainer cc) {
    this.cc = cc;
    initialPluginListener = new CustomContainerPlugins.PluginRegistryListener() {
      @Override
      public void added(CustomContainerPlugins.ApiInfo plugin) {
        if (plugin == null || plugin.getInstance() == null) {
          return;
        }
        Object instance = plugin.getInstance();
        if (instance instanceof ClusterEventListener) {
          registerListener((ClusterEventListener) instance);
        }
      }

      @Override
      public void deleted(CustomContainerPlugins.ApiInfo plugin) {
        if (plugin == null || plugin.getInstance() == null) {
          return;
        }
        Object instance = plugin.getInstance();
        if (instance instanceof ClusterEventListener) {
          unregisterListener((ClusterEventListener) instance);
        }
      }

      @Override
      public void modified(CustomContainerPlugins.ApiInfo old, CustomContainerPlugins.ApiInfo replacement) {
        added(replacement);
        deleted(old);
      }
    };
  }

  /**
   * This method returns an initial plugin registry listener that helps to capture the
   * freshly loaded listener plugins before the final cluster event producer is created.
   * @return initial listener
   */
  public CustomContainerPlugins.PluginRegistryListener getPluginRegistryListener() {
    return initialPluginListener;
  }

  /**
   * Create a {@link ClusterEventProducer} based on the current plugin configurations.
   * <p>NOTE: this method can only be called once because it has side-effects, such as
   * transferring the initially collected listeners to the resulting producer's instance, and
   * installing a {@link org.apache.solr.api.CustomContainerPlugins.PluginRegistryListener}.
   * Calling this method more than once will result in an exception.</p>
   * @param plugins current plugin configurations
   * @return configured instance of cluster event producer (with side-effects, see above)
   */
  public ClusterEventProducer create(CustomContainerPlugins plugins) {
    if (created) {
      throw new RuntimeException("this factory can be called only once!");
    }
    final ClusterEventProducer clusterEventProducer;
    CustomContainerPlugins.ApiInfo clusterEventProducerInfo = plugins.getPlugin(ClusterEventProducer.PLUGIN_NAME);
    if (clusterEventProducerInfo != null) {
      // the listener in ClusterSingletons already registered it
      clusterEventProducer = (ClusterEventProducer) clusterEventProducerInfo.getInstance();
    } else {
      // create the default impl
      clusterEventProducer = new DefaultClusterEventProducer(cc);
      // since this is a ClusterSingleton, register it as such
      cc.getClusterSingletons().getSingletons().put(ClusterEventProducer.PLUGIN_NAME, clusterEventProducer);
    }
    // transfer those listeners that were already registered to the initial impl
    transferListeners(clusterEventProducer, plugins);
    // install plugin registry listener
    CustomContainerPlugins.PluginRegistryListener pluginListener = new CustomContainerPlugins.PluginRegistryListener() {
      @Override
      public void added(CustomContainerPlugins.ApiInfo plugin) {
        if (plugin == null || plugin.getInstance() == null) {
          return;
        }
        Object instance = plugin.getInstance();
        if (instance instanceof ClusterEventListener) {
          ClusterEventListener listener = (ClusterEventListener) instance;
          clusterEventProducer.registerListener(listener);
        }
      }

      @Override
      public void deleted(CustomContainerPlugins.ApiInfo plugin) {
        if (plugin == null || plugin.getInstance() == null) {
          return;
        }
        Object instance = plugin.getInstance();
        if (instance instanceof ClusterEventListener) {
          ClusterEventListener listener = (ClusterEventListener) instance;
          clusterEventProducer.unregisterListener(listener);
        }
      }

      @Override
      public void modified(CustomContainerPlugins.ApiInfo old, CustomContainerPlugins.ApiInfo replacement) {
        added(replacement);
        deleted(old);
      }
    };
    plugins.registerListener(pluginListener);
    created = true;
    return clusterEventProducer;
  }

  private void transferListeners(ClusterEventProducer target, CustomContainerPlugins plugins) {
    // stop capturing listener plugins
    plugins.unregisterListener(initialPluginListener);
    // transfer listeners that are already registered
    initialListeners.forEach((type, listeners) -> {
      listeners.forEach(listener -> target.registerListener(listener, type));
    });
    initialListeners.clear();
    initialListeners = null;
  }

  // ClusterEventProducer API, parts needed to register initial listeners.

  @Override
  public void registerListener(ClusterEventListener listener, ClusterEvent.EventType... eventTypes) {
    if (eventTypes == null || eventTypes.length == 0) {
      eventTypes = ClusterEvent.EventType.values();
    }
    for (ClusterEvent.EventType type : eventTypes) {
      initialListeners.computeIfAbsent(type, t -> new HashSet<>())
          .add(listener);
    }
  }

  @Override
  public void unregisterListener(ClusterEventListener listener, ClusterEvent.EventType... eventTypes) {
    throw new UnsupportedOperationException("unregister listener not implemented");
  }

  @Override
  public void start() throws Exception {
    throw new UnsupportedOperationException("start not implemented");
  }

  @Override
  public State getState() {
    return State.STOPPED;
  }

  @Override
  public void stop() {
    throw new UnsupportedOperationException("stop not implemented");
  }


}
