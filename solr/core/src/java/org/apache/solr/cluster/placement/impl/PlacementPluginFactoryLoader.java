package org.apache.solr.cluster.placement.impl;

import org.apache.solr.api.ContainerPluginsRegistry;
import org.apache.solr.cluster.placement.PlacementPlugin;
import org.apache.solr.cluster.placement.PlacementPluginFactory;

/**
 * Utility class to load the configured {@link PlacementPluginFactory} plugin and
 * then keep it up to date as the plugin configuration changes.
 */
public class PlacementPluginFactoryLoader {

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
          pluginFactory.setDelegate((PlacementPluginFactory) instance);
        }
      }

      @Override
      public void deleted(ContainerPluginsRegistry.ApiInfo plugin) {
        if (plugin == null || plugin.getInstance() == null) {
          return;
        }
        Object instance = plugin.getInstance();
        if (instance instanceof PlacementPluginFactory) {
          pluginFactory.setDelegate(null);
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
  private static final class DelegatingPlacementPluginFactory implements PlacementPluginFactory {

    private PlacementPluginFactory delegate;

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
    }
  }
}
