/**
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

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.Map;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.DOMUtil;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.Config;

import javax.xml.xpath.XPathConstants;

/**
 * Contains the knowledge of how cache config is
 * stored in the solrconfig.xml file, and implements a
 * factory to create caches.
 *
 * @version $Id$
 */
public class CacheConfig {
  private String nodeName;
  private Map args;

  private String cacheImpl;
  private Class clazz;

  private Object[] persistence = new Object[1];

  private String regenImpl;
  private CacheRegenerator regenerator;

  public CacheRegenerator getRegenerator() {
    return regenerator;
  }

  public void setRegenerator(CacheRegenerator regenerator) {
    this.regenerator = regenerator;
  }

  public static CacheConfig[] getMultipleConfigs(SolrConfig solrConfig, String configPath) {
    NodeList nodes = (NodeList)solrConfig.evaluate(configPath, XPathConstants.NODESET);
    if (nodes==null || nodes.getLength()==0) return null;
    CacheConfig[] configs = new CacheConfig[nodes.getLength()];
    for (int i=0; i<nodes.getLength(); i++) {
      configs[i] = getConfig(nodes.item(i));
    }
    return configs;
  }


  public static CacheConfig getConfig(SolrConfig solrConfig, String xpath) {
    Node node = (Node)solrConfig.getNode(xpath, false);
    return getConfig(node);
  }


  public static CacheConfig getConfig(Node node) {
    if (node==null) return null;
    CacheConfig config = new CacheConfig();
    config.nodeName = node.getNodeName();
    config.args = DOMUtil.toMap(node.getAttributes());
    String nameAttr = (String)config.args.get("name");  // OPTIONAL
    if (nameAttr==null) {
      config.args.put("name",config.nodeName);
    }

    config.cacheImpl = (String)config.args.get("class");
    config.regenImpl = (String)config.args.get("regenerator");
    config.clazz = Config.findClass(config.cacheImpl);
    if (config.regenImpl != null) {
      config.regenerator = (CacheRegenerator) Config.newInstance(config.regenImpl);
    }


    return config;
  }

  public SolrCache newInstance() {
    try {
      SolrCache cache = (SolrCache)clazz.newInstance();
      persistence[0] = cache.init(args, persistence[0], regenerator);
      return cache;
    } catch (Exception e) {
      SolrException.log(SolrCache.log,"Error instantiating cache",e);
      // we can carry on without a cache... but should we?
      // in some cases (like an OOM) we probably should try to continue.
      return null;
    }
  }

}
