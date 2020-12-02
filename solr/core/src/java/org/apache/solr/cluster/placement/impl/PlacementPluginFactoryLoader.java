package org.apache.solr.cluster.placement.impl;

import org.apache.solr.api.ContainerPluginsRegistry;
import org.apache.solr.cluster.placement.PlacementPlugin;
import org.apache.solr.cluster.placement.PlacementPluginFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

/**
 * Utility class to load the configured {@link PlacementPluginFactory} plugin and
 * then keep it up to date as the plugin configuration changes.
 */
public class PlacementPluginFactoryLoader {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static PlacementPluginFactory load(ContainerPluginsRegistry plugins) {
    final DelegatingPlacementPluginFactory pluginFactory = new DelegatingPlacementPluginFactory();
    ContainerPluginsRegistry.ApiInfo pluginFactoryInfo = plugins.getPlugin(PlacementPluginFactory.PLUGIN_NAME);
    if (pluginFactoryInfo != null && (pluginFactoryInfo.getInstance() instanceof PlacementPluginFactory)) {
      pluginFactory.setDelegate((PlacementPluginFactory) pluginFactoryInfo.getInstance());
    }
    ContainerPluginsRegistry.PluginRegistryListener pluginListener = new ContainerPluginsRegistry.PluginRegistryListener() {
      @Override
      public void added(ContainerPluginsRegistry.ApiInfo plugin) {
        if (plugin == null || plugin.getInstance() == null) {
          return;
        }
        Object instance = plugin.getInstance();
        if (instance instanceof PlacementPluginFactory) {
          if (PlacementPluginFactory.PLUGIN_NAME.equals(plugin.getInfo().name)) {
            pluginFactory.setDelegate((PlacementPluginFactory) instance);
          } else {
            log.warn("Ignoring PlacementPluginFactory plugin with non-standard name: " + plugin.getInfo());
          }
        }
      }

      @Override
      public void deleted(ContainerPluginsRegistry.ApiInfo plugin) {
        if (plugin == null || plugin.getInstance() == null) {
          return;
        }
        Object instance = plugin.getInstance();
        if (instance instanceof PlacementPluginFactory) {
          if (PlacementPluginFactory.PLUGIN_NAME.equals(plugin.getInfo().name)) {
            pluginFactory.setDelegate(null);
          } else {
            log.warn("Ignoring PlacementPluginFactory plugin with non-standard name: " + plugin.getInfo());
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
    return pluginFactory;
  }

  /**
   * Helper class to support dynamic reloading of plugin implementations.
   */
  public static final class DelegatingPlacementPluginFactory implements PlacementPluginFactory {

    private PlacementPluginFactory delegate;
    // support for tests to make sure the update is completed
    private int version;

    @Override
    public PlacementPlugin createPluginInstance() {
      if (delegate != null) {
        return delegate.createPluginInstance();
      } else {
        return null;
      }
    }

    public void setDelegate(PlacementPluginFactory delegate) {
      this.delegate = delegate;
      this.version++;
    }

    public PlacementPluginFactory getDelegate() {
      return delegate;
    }

    public int getVersion() {
      return version;
    }
  }
}
