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

package org.apache.solr.cluster.placement.impl;

import java.util.HashMap;
import java.util.Map;

import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.cluster.placement.PlacementPlugin;
import org.apache.solr.cluster.placement.PlacementPluginConfig;
import org.apache.solr.cluster.placement.PlacementPluginFactory;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.Utils;

/**
 * <p>This concrete class is implementing the config as visible by the placement plugins and contains the code transforming the
 * plugin configuration (currently stored in {@code clusterprops.json} into a strongly typed abstraction (that will not
 * change if internally plugin configuration is moved to some other place).</p>
 *
 * <p>This class also contains the (static) code dealing with instantiating the plugin factory config (it is config, even though
 * of a slightly different type). This code is not accessed by the plugin code but used from the
 * {@link org.apache.solr.cloud.api.collections.Assign} class.</p>
 */
public class PlacementPluginConfigImpl implements PlacementPluginConfig {
  /**
   * The key in {@code clusterprops.json} under which the plugin factory (and the plugin itself) configuration lives.
   */
  final public static String PLACEMENT_PLUGIN_CONFIG_KEY = "placement-plugin";
  final public static String CONFIG_CLASS = "class";
  final public static String CONFIG_VERSION = "version";

  // Separating configs into typed maps based on the element names in solr.xml
  private final Map<String, String> stringConfigs;
  private final Map<String, Integer> intConfigs;
  private final Map<String, Long> longConfigs;
  private final Map<String, Boolean> boolConfigs;
  private final Map<String, Float> floatConfigs;
  private final Map<String, Double> doubleConfigs;


  private PlacementPluginConfigImpl(Map<String, String> stringConfigs,
                                    Map<String, Integer> intConfigs,
                                    Map<String, Long> longConfigs,
                                    Map<String, Boolean> boolConfigs,
                                    Map<String, Float> floatConfigs,
                                    Map<String, Double> doubleConfigs) {
    this.stringConfigs = stringConfigs;
    this.intConfigs = intConfigs;
    this.longConfigs = longConfigs;
    this.boolConfigs = boolConfigs;
    this.floatConfigs = floatConfigs;
    this.doubleConfigs = doubleConfigs;
  }

  @Override
  public String getStringConfig(String configName) {
    return stringConfigs.get(configName);
  }

  @Override
  public String getStringConfig(String configName, String defaultValue) {
    String retval = stringConfigs.get(configName);
    return retval != null ? retval : defaultValue;
  }

  @Override
  public Boolean getBooleanConfig(String configName) {
    return boolConfigs.get(configName);
  }

  @Override
  public Boolean getBooleanConfig(String configName, boolean defaultValue) {
    Boolean retval = boolConfigs.get(configName);
    return retval != null ? retval : defaultValue;
  }

  @Override
  public Integer getIntegerConfig(String configName) {
    return intConfigs.get(configName);
  }

  @Override
  public Integer getIntegerConfig(String configName, int defaultValue) {
    Integer retval = intConfigs.get(configName);
    return retval != null ? retval : defaultValue;
  }

  @Override
  public Long getLongConfig(String configName) {
    return longConfigs.get(configName);
  }

  @Override
  public Long getLongConfig(String configName, long defaultValue) {
    Long  retval = longConfigs.get(configName);
    return retval != null ? retval : defaultValue;
  }

  @Override
  public Float getFloatConfig(String configName) {
    return floatConfigs.get(configName);
  }

  @Override
  public Float getFloatConfig(String configName, float defaultValue) {
    Float retval = floatConfigs.get(configName);
    return retval != null ? retval : defaultValue;
  }

  @Override
  public Double getDoubleConfig(String configName) {
    return doubleConfigs.get(configName);
  }

  @Override
  public Double getDoubleConfig(String configName, double defaultValue) {
    Double retval = doubleConfigs.get(configName);
    return retval != null ? retval : defaultValue;
  }

  /**
   * <p>Parses the {@link Map} obtained as the value for key {@link #PLACEMENT_PLUGIN_CONFIG_KEY} from
   * the {@code clusterprops.json} configuration {@link Map} (obtained by calling
   * {@link org.apache.solr.client.solrj.impl.ClusterStateProvider#getClusterProperties()}) and translates it into a
   * configuration consumable by the plugin (and that will not change as Solr changes internally how and where it stores
   * configuration).</p>
   *
   * <p>A way to create the corresponding configuration in {@code clusterprops.json} is by executing something along the following
   * while SolrCloud is running (see SOLR-14404 and {@link org.apache.solr.handler.admin.ContainerPluginsApi}):</p>
   *
   * <pre>
   * curl -X POST -H 'Content-type:application/json' --data-binary '
   * {
   *   "set-placement-plugin": {
   *      "class": "plugin.factory.full.ClassName$innerClass",
   *      "key1" : "val1"
   *   }
   * }' http://localhost:8983/api/cluster/plugins
   * </pre>
   *
   * In order to remove this configuration, do instead:
   *
   * <pre>
   * curl -X POST -H 'Content-type:application/json' --data-binary '
   * {
   *    "set-placement-plugin":  null
   * }' http://localhost:8983/api/cluster/plugins
   * </pre>
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  static PlacementPluginConfig createConfigFromProperties(Map<String, Object> pluginConfig) {
    final Map<String, String> stringConfigs = new HashMap<>();
    final Map<String, Integer> intConfigs = new HashMap<>();
    final Map<String, Long> longConfigs = new HashMap<>();
    final Map<String, Boolean> boolConfigs = new HashMap<>();
    final Map<String, Float> floatConfigs = new HashMap<>();
    final Map<String, Double> doubleConfigs = new HashMap<>();

    for (Map.Entry<String, Object> e : pluginConfig.entrySet()) {
      String key = e.getKey();
      if (CONFIG_CLASS.equals(key) || CONFIG_VERSION.equals(key)) {
        // These are reserved configuration names, see getPlacementPlugin() below
        continue;
      }

      if (key == null) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Missing config name attribute in parameter of " + PLACEMENT_PLUGIN_CONFIG_KEY);
      }

      Object value = e.getValue();

      if (value == null) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Missing config value for parameter " + key + " of " + PLACEMENT_PLUGIN_CONFIG_KEY);
      }

      if (value instanceof String) {
        stringConfigs.put(key, (String) value);
      } else if (value instanceof Integer) {
        intConfigs.put(key, (Integer) value);
      } else if (value instanceof Long) {
        longConfigs.put(key, (Long) value);
      } else if (value instanceof Boolean) {
        boolConfigs.put(key, (Boolean) value);
      } else if (value instanceof Float) {
        floatConfigs.put(key, (Float) value);
      } else if (value instanceof Double) {
        doubleConfigs.put(key, (Double) value);
      } else {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unsupported config type " + value.getClass().getName() +
            " for parameter " + key + " of " + PLACEMENT_PLUGIN_CONFIG_KEY);
      }
    }

    return new PlacementPluginConfigImpl(stringConfigs, intConfigs, longConfigs, boolConfigs, floatConfigs, doubleConfigs);
  }

  /**
   * <p>This is where the plugin configuration is being read (from wherever in Solr it lives, and this will likely change with time),
   * a {@link org.apache.solr.cluster.placement.PlacementPluginFactory} (as configured) instantiated and a plugin instance
   * created from this factory.</p>
   *
   * <p>The initial implementation you see here is crude! the configuration is read anew each time and the factory class
   * as well as the plugin class instantiated each time.
   * This has to be changed once the code is accepted overall, to register a listener that is notified when the configuration
   * changes (see {@link org.apache.solr.common.cloud.ZkStateReader#registerClusterPropertiesListener})
   * and that will either create a new instance of the plugin with new configuration using the existing factory (if the factory
   * class has not changed - we need to keep track of this one) of create a new factory altogether (then a new plugin instance).</p>
   */
  @SuppressWarnings({"unchecked"})
  public static PlacementPlugin getPlacementPlugin(SolrCloudManager solrCloudManager) {
    Map<String, Object> props = solrCloudManager.getClusterStateProvider().getClusterProperties();
    Map<String, Object> pluginConfigMap = (Map<String, Object>) props.get(PLACEMENT_PLUGIN_CONFIG_KEY);

    if (pluginConfigMap == null) {
      return null;
    }

    String pluginFactoryClassName = (String) pluginConfigMap.get(CONFIG_CLASS);

    // Get the configured plugin factory class. Is there a way to load a resource in Solr without being in the context of
    // CoreContainer? Here the placement code is unrelated to the presence of cores (and one can imagine it running on
    // specialized nodes not having a CoreContainer). I guess the loading code below is not totally satisfying (although
    // it's not the only place in Solr doing it that way), but I didn't find more satisfying alternatives. Open to suggestions.
    PlacementPluginFactory placementPluginFactory;
    try {
      Class<? extends PlacementPluginFactory> factoryClazz =
              Class.forName(pluginFactoryClassName, true, PlacementPluginConfigImpl.class.getClassLoader())
                      .asSubclass(PlacementPluginFactory.class);

      placementPluginFactory = factoryClazz.getConstructor().newInstance(); // no args constructor - that's why we introduced a factory...
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,  "Unable to instantiate placement-plugin factory: " + Utils.toJSONString(pluginConfigMap), e);
    }

    // Translate the config from the properties where they are defined into the abstraction seen by the plugin
    PlacementPluginConfig pluginConfig = createConfigFromProperties(pluginConfigMap);

    PlacementPlugin placementPlugin = placementPluginFactory.createPluginInstance(pluginConfig);

    return placementPlugin;
  }
}
