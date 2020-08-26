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
import java.util.Set;

import org.apache.solr.cluster.placement.PlacementPluginConfig;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.PluginInfo;

/**
 * <p>This concrete class implementing the config as visible by the placement plugins contains the code transforming the
 * {@link PluginInfo} built by {@link org.apache.solr.core.SolrXmlConfig} from the content of {@code solr.xml} into a
 * strongly typed abstraction.
 */
public class PlacementPluginConfigImpl implements PlacementPluginConfig {

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
  public Boolean getBooleanConfig(String configName) {
    return boolConfigs.get(configName);
  }

  @Override
  public Integer getIntegerConfig(String configName) {
    return intConfigs.get(configName);
  }

  @Override
  public Long getLongConfig(String configName) {
    return longConfigs.get(configName);
  }

  @Override
  public Float getFloatConfig(String configName) {
    return floatConfigs.get(configName);
  }

  @Override
  public Double getDoubleConfig(String configName) {
    return doubleConfigs.get(configName);
  }


  /**
   * <p>Parses the {@link PluginInfo} and extracts values to the appropriate maps for strongly typed access by plugin code.
   *
   * <p>Values originally come from elements inside the {@code <placementPluginFactory>} element in {@code solr.xml}
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  static PlacementPluginConfig createConfigFromInfo(PluginInfo info) {

    final Map<String, String> stringConfigs = new HashMap<>();
    final Map<String, Integer> intConfigs = new HashMap<>();
    final Map<String, Long> longConfigs = new HashMap<>();
    final Map<String, Boolean> boolConfigs = new HashMap<>();
    final Map<String, Float> floatConfigs = new HashMap<>();
    final Map<String, Double> doubleConfigs = new HashMap<>();

    if (info.initArgs != null) {
      // Some ugliness due to the lack of typing of NamedList in PluginInfo. But at least it's concentrated in a single
      // place rather than spread all over the code.
      for (Map.Entry<String, Object> e : (Set<Map.Entry<String, Object>>) info.initArgs.asShallowMap().entrySet()) {
        String key = e.getKey();
        if (key == null) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Missing config name attribute in parameter for " + info.className);
        }

        Object value = e.getValue();

        if (value == null) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Missing config value for parameter " + key + " for " + info.className);
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
              " for parameter " + key + " for " + info.className);
        }
      }
    }

    return new PlacementPluginConfigImpl(stringConfigs, intConfigs, longConfigs, boolConfigs, floatConfigs, doubleConfigs);
  }
}
