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

import javax.xml.xpath.XPathConstants;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.ConfigOverlay;
import org.apache.solr.core.MemClassLoader;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.RuntimeLib;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.util.DOMUtil;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import static org.apache.solr.common.params.CommonParams.NAME;

public class CacheConfig implements MapWriter {
  final PluginInfo args;
  private CacheRegenerator defRegen;
  private final String name;
  private String cacheImpl, regenImpl;
  Object[] persistence = new Object[1];


  public CacheConfig(Map<String, String> args, String path) {
    this.args = new PluginInfo(SolrCache.TYPE, (Map) copyValsAsString(args));
    this.name = args.get(NAME);
    this.cacheImpl = args.getOrDefault("class", "solr.LRUCache");
    this.regenImpl = args.get("regenerator");
    this.args.pathInConfig = StrUtils.splitSmart(path, '/', true);
  }

  static Map<String, String> copyValsAsString(Map m) {
    Map<String, String> copy = new LinkedHashMap(m.size());
    m.forEach((k, v) -> copy.put(String.valueOf(k), String.valueOf(v)));
    return copy;
  }

  public static CacheConfig getConfig(SolrConfig solrConfig, String xpath) {
    Node node = solrConfig.getNode(xpath, false);
    if (node == null || !"true".equals(DOMUtil.getAttrOrDefault(node, "enabled", "true"))) {
      Map<String, String> m = solrConfig.getOverlay().getEditableSubProperties(xpath);
      if (m == null) return null;
      List<String> pieces = StrUtils.splitSmart(xpath, '/');
      String name = pieces.get(pieces.size() - 1);
      m = Utils.getDeepCopy(m, 2);
      m.put(NAME, name);
      return new CacheConfig(m, xpath);
    } else {
      Map<String, String> attrs = DOMUtil.toMap(node.getAttributes());
      attrs.put(NAME, node.getNodeName());
      return new CacheConfig(applyOverlay(xpath, solrConfig.getOverlay(), attrs), xpath);

    }


  }

  private static Map applyOverlay(String xpath, ConfigOverlay overlay, Map args) {
    Map<String, String> map = xpath == null ? null : overlay.getEditableSubProperties(xpath);
    if (map != null) {
      HashMap<String, String> mapCopy = new HashMap<>(args);
      for (Map.Entry<String, String> e : map.entrySet()) {
        mapCopy.put(e.getKey(), String.valueOf(e.getValue()));
      }
      return mapCopy;
    }
    return args;
  }

  public static Map<String, CacheConfig> getConfigs(SolrConfig solrConfig, String configPath) {
    NodeList nodes = (NodeList) solrConfig.evaluate(configPath, XPathConstants.NODESET);
    if (nodes == null || nodes.getLength() == 0) return new LinkedHashMap<>();
    Map<String, CacheConfig> result = new HashMap<>(nodes.getLength());
    for (int i = 0; i < nodes.getLength(); i++) {
      Map<String, String> args = DOMUtil.toMap(nodes.item(i).getAttributes());
      result.put(args.get(NAME), new CacheConfig(args, configPath+"/"+args.get(NAME)));
    }
    return result;
  }

  public String getName() {
    return name;
  }


  public <K, V> SolrCacheHolder<K, V> newInstance(SolrCore core) {
    return new SolrCacheHolder(new CacheInfo(this, core));
  }

  static class CacheInfo {
    final CacheConfig cfg;
    SolrCore core;
    SolrCache cache = null;
    String pkg;
    RuntimeLib runtimeLib;
    CacheRegenerator regen = null;


    CacheInfo(CacheConfig cfg, SolrCore core) {
      this.core = core;
      this.cfg = cfg;
      pkg = cfg.args.attributes.get(CommonParams.PACKAGE);
      ResourceLoader loader = pkg == null ? core.getResourceLoader() :
          core.getCoreContainer().getPackageManager().getResourceLoader(pkg);

      try {
        cache = loader.findClass(cfg.cacheImpl, SolrCache.class).getConstructor().newInstance();
        regen = null;
        if (cfg.regenImpl != null) {
          regen = loader.findClass(cfg.regenImpl, CacheRegenerator.class).getConstructor().newInstance();
        }
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error loading cache " + cfg.jsonStr(), e);
      }
      if (regen == null && cfg.defRegen != null) regen = cfg.defRegen;
      cfg.persistence[0] = cache.init(cfg.args.attributes, cfg.persistence[0], regen);
      if (pkg!=null && loader instanceof MemClassLoader) {
        MemClassLoader memClassLoader = (MemClassLoader) loader;
        runtimeLib = core.getCoreContainer().getPackageManager().getLib(pkg);
      }

    }
  }


  public void setDefaultRegenerator(CacheRegenerator regen) {
    this.defRegen = regen;
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    args.attributes.forEach(ew.getBiConsumer());
  }
}
