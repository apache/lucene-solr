package org.apache.solr.managed;

import java.util.HashMap;
import java.util.Map;

import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.managed.plugins.CacheManagerPlugin;

/**
 *
 */
public class DefaultResourceManagerPluginFactory implements ResourceManagerPluginFactory {

  private static final Map<String, String> typeToClass = new HashMap<>();

  static {
    typeToClass.put(CacheManagerPlugin.TYPE, CacheManagerPlugin.class.getName());
  }

  private final SolrResourceLoader loader;

  public DefaultResourceManagerPluginFactory(SolrResourceLoader loader) {
    this.loader = loader;
  }

  @Override
  public ResourceManagerPlugin create(String type, Map<String, Object> params) throws Exception {
    String pluginClazz = typeToClass.get(type);
    if (pluginClazz == null) {
      throw new IllegalArgumentException("Unsupported plugin type '" + type + "'");
    }
    ResourceManagerPlugin resourceManagerPlugin = loader.newInstance(pluginClazz, ResourceManagerPlugin.class);
    resourceManagerPlugin.init(params);
    return resourceManagerPlugin;
  }
}
