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
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.ConfigOverlay;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.util.DOMUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import static org.apache.solr.common.params.CommonParams.NAME;

public class SolrCacheHolder<K, V> implements SolrCache<K,V> {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


  private final Factory factory;
  protected volatile SolrCache<K, V> delegate;

  public SolrCacheHolder(SolrCache<K, V> delegate, Factory factory) {
    this.delegate = delegate;
    this.factory = factory;
  }

  public int size() {
    return delegate.size();
  }

  public V put(K key, V value) {
    return delegate.put(key, value);

  }

  public V get(K key) {
    return delegate.get(key);
  }

  public void clear() {
    delegate.clear();
  }

  @Override
  public void setState(State state) {
    delegate.setState(state);
  }

  @Override
  public State getState() {
    return delegate.getState();
  }

  @Override
  public void warm(SolrIndexSearcher searcher, SolrCache<K, V> old) {
    if (old instanceof SolrCacheHolder) old = ((SolrCacheHolder) old).get();
    delegate.warm(searcher, old);

  }

  public SolrCache<K, V> get() {
    return delegate;
  }

  public void close() {
    delegate.close();
  }

  @Override
  public Map<String, Object> getResourceLimits() {
    return delegate.getResourceLimits();
  }

  @Override
  public void setResourceLimit(String limitName, Object value) throws Exception {
    delegate.setResourceLimit(limitName, value);

  }


  public void warm(SolrIndexSearcher searcher, SolrCacheHolder src) {
    delegate.warm(searcher, src.get());
  }

  @Override
  public Object init(Map args, Object persistence, CacheRegenerator regenerator) {
    return null;
  }

  @Override
  public String name() {
    return delegate.name();
  }


  @Override
  public String getName() {
    return delegate.getName();
  }

  @Override
  public String getDescription() {
    return delegate.getDescription();
  }

  @Override
  public Category getCategory() {
    return delegate.getCategory();
  }

  @Override
  public void initializeMetrics(SolrMetricManager manager, String registry, String tag, String scope) {
    log.debug("Going to register cachemetrics " + Utils.toJSONString(factory));

    delegate.initializeMetrics(manager, registry, tag,scope);

  }

  public static class Factory implements MapWriter {
    final Map<String, String> args;
    private CacheRegenerator defRegen;
    private final String name;
    private String cacheImpl, regenImpl;
    private Object[] persistence = new Object[1];


    public Factory(Map<String, String> args) {
      this.args = copyValsAsString(args);
      this.name = args.get(NAME);
      this.cacheImpl = args.getOrDefault("class", "solr.LRUCache");
      this.regenImpl = args.get("regenerator");
    }
    static Map<String,String> copyValsAsString(Map m){
      Map<String,String> copy = new LinkedHashMap(m.size());
      m.forEach((k, v) -> copy.put(String.valueOf(k), String.valueOf(v)));
      return copy;
    }

    public static Factory create(SolrConfig solrConfig, String xpath) {
      Node node = solrConfig.getNode(xpath, false);
      if (node == null || !"true".equals(DOMUtil.getAttrOrDefault(node, "enabled", "true"))) {
        Map<String, String> m = solrConfig.getOverlay().getEditableSubProperties(xpath);
        if (m == null) return null;
        return new Factory(m);
      } else {
        Map<String, String> attrs = DOMUtil.toMap(node.getAttributes());
        attrs.put(NAME, node.getNodeName());
        return new Factory(applyOverlay(xpath, solrConfig.getOverlay(), attrs));

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

    public static Map<String, Factory> create(SolrConfig solrConfig, String configPath, boolean multiple) {
      NodeList nodes = (NodeList) solrConfig.evaluate(configPath, XPathConstants.NODESET);
      if (nodes == null || nodes.getLength() == 0) return new LinkedHashMap<>();
      Map<String, Factory> result = new HashMap<>(nodes.getLength());
      for (int i = 0; i < nodes.getLength(); i++) {
        Map<String, String> args = DOMUtil.toMap(nodes.item(i).getAttributes());
        result.put(args.get(NAME), new Factory(args));
      }
      return result;
    }

    public String getName() {
      return name;
    }


    public <K, V> SolrCacheHolder<K, V> create(SolrCore core) {
      ResourceLoader loader = core.getResourceLoader();

      SolrCache inst = null;
      CacheRegenerator regen = null;
      try {
        inst = loader.findClass(cacheImpl, SolrCache.class).getConstructor().newInstance();
        regen = null;
        if (regenImpl != null) {
          regen = loader.findClass(regenImpl, CacheRegenerator.class).getConstructor().newInstance();
        }
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error loading cache " + jsonStr(), e);

      }
      if (regen == null && defRegen != null) regen = defRegen;

      inst.init(args, persistence[0], regen);

      return new SolrCacheHolder<>(inst, this);

    }

    public void setDefaultRegenerator(CacheRegenerator regen) {
      this.defRegen = regen;
    }

    @Override
    public void writeMap(EntryWriter ew) throws IOException {
      args.forEach(ew.getBiConsumer());
    }
  }
}
