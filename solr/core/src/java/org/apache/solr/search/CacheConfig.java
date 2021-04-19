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
package org.apache.solr.search;

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.solr.common.ConfigNode;
import org.apache.solr.common.MapSerializable;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.NAME;

/**
 * Contains the knowledge of how cache config is
 * stored in the solrconfig.xml file, and implements a
 * factory to create caches.
 */
public class CacheConfig implements MapSerializable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private String nodeName;


  /**
   * When this object is created, the core is not yet available . So, if the class is to be
   * loaded from a package we should have a corresponding core
   *
   */
  @SuppressWarnings({"rawtypes"})
  private Supplier<Class<? extends SolrCache>> clazz;
  private Map<String,String> args;
  private CacheRegenerator regenerator;

  private String cacheImpl;

  private Object[] persistence = new Object[1];

  private String regenImpl;

  public CacheConfig() {
  }

  @SuppressWarnings({"rawtypes"})
  public CacheConfig(Class<? extends SolrCache> clazz, Map<String,String> args, CacheRegenerator regenerator) {
    this.clazz = () -> clazz;
    this.args = args;
    this.regenerator = regenerator;
    this.nodeName = args.get(NAME);
  }

  public CacheRegenerator getRegenerator() {
    return regenerator;
  }

  public void setRegenerator(CacheRegenerator regenerator) {
    this.regenerator = regenerator;
  }

  public static Map<String, CacheConfig> getMultipleConfigs(SolrConfig solrConfig, String configPath, List<ConfigNode> nodes) {
    if (nodes == null || nodes.size() == 0) return new LinkedHashMap<>();
    Map<String, CacheConfig> result = new HashMap<>(nodes.size());
    for (int i = 0; i < nodes.size(); i++) {
      ConfigNode node = nodes.get(i);
      if (node.boolAttr("enabled", true)) {
        CacheConfig config = getConfig(solrConfig, node.name(), node.attributes().asMap(), configPath);
        result.put(config.args.get(NAME), config);
      }
    }
    return result;
  }


  @SuppressWarnings({"unchecked"})
  public static CacheConfig getConfig(SolrConfig solrConfig, ConfigNode node, String xpath) {
//    Node node = solrConfig.getNode(xpath, false);
    if (!node.exists() || !"true".equals(node.attributes().get("enabled", "true"))) {
      Map<String, String> m = solrConfig.getOverlay().getEditableSubProperties(xpath);
      if (m == null) return null;
      List<String> parts = StrUtils.splitSmart(xpath, '/');
      return getConfig(solrConfig, parts.get(parts.size() - 1), Collections.EMPTY_MAP, xpath);
    }
    return getConfig(solrConfig, node.name(), node.attributes().asMap(), xpath);
  }


  @SuppressWarnings({"unchecked"})
  public static CacheConfig getConfig(SolrConfig solrConfig, String nodeName, Map<String, String> attrs, String xpath) {
    CacheConfig config = new CacheConfig();
    config.nodeName = nodeName;
    @SuppressWarnings({"rawtypes"})
    Map attrsCopy = new LinkedHashMap<>(attrs.size());
    for (Map.Entry<String, String> e : attrs.entrySet()) {
      attrsCopy.put(e.getKey(), String.valueOf(e.getValue()));
    }
    attrs = attrsCopy;
    config.args = attrs;

    Map<String, String> map = xpath == null ? null : solrConfig.getOverlay().getEditableSubProperties(xpath);
    if(map != null){
      HashMap<String, String> mapCopy = new HashMap<>(config.args);
      for (Map.Entry<String, String> e : map.entrySet()) {
        mapCopy.put(e.getKey(),String.valueOf(e.getValue()));
      }
      config.args = mapCopy;
    }
    String nameAttr = config.args.get(NAME);  // OPTIONAL
    if (nameAttr==null) {
      config.args.put(NAME, config.nodeName);
    }

    SolrResourceLoader loader = solrConfig.getResourceLoader();
    config.cacheImpl = config.args.get("class");
    if (config.cacheImpl == null) config.cacheImpl = "solr.CaffeineCache";
    config.clazz = new Supplier() {
      @SuppressWarnings("rawtypes")
      Class<SolrCache> loadedClass;

      @Override
      @SuppressWarnings("rawtypes")
      public Class<? extends SolrCache> get() {
        if (loadedClass != null) return loadedClass;
        return loadedClass = (Class<SolrCache>) loader.findClass(
            new PluginInfo("cache", Collections.singletonMap("class", config.cacheImpl)),
            SolrCache.class, true);
      }
    };
    config.regenImpl = config.args.get("regenerator");
    if (config.regenImpl != null) {
      config.regenerator = loader.newInstance(config.regenImpl, CacheRegenerator.class);
    }

    return config;
  }

  @SuppressWarnings({"rawtypes"})
  public SolrCache newInstance() {
    try {
      SolrCache cache = clazz.get().getConstructor().newInstance();
      persistence[0] = cache.init(args, persistence[0], regenerator);
      return cache;
    } catch (Exception e) {
      SolrException.log(log,"Error instantiating cache",e);
      // we can carry on without a cache... but should we?
      // in some cases (like an OOM) we probably should try to continue.
      return null;
    }
  }

  @Override
  public Map<String, Object> toMap(Map<String, Object> map) {
    return new HashMap<>(args);
  }

  public String getNodeName() {
    return nodeName;
  }


}
