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

package org.apache.solr.common.cloud;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.NavigableObject;
import org.apache.solr.common.util.Utils;

/**
 * Fetch lazily and cache a node's system properties
 *
 */
public class LazyNodePropsProvider implements NodePropsProvider {
  private volatile boolean isClosed = false;
  private Map<String ,Map<String, Object>> nodeVsTagsCache = new ConcurrentHashMap<>();
  private final String thisNode;
  private final ZkStateReader zkStateReader;
  private final CloudSolrClient cloudSolrClient;

  public LazyNodePropsProvider(String thisNode, CloudSolrClient cloudSolrClient, ZkStateReader zkStateReader) {
    this.zkStateReader = zkStateReader;
    this.cloudSolrClient = cloudSolrClient;
    this.thisNode = thisNode;
    zkStateReader.registerLiveNodesListener((oldNodes, newNodes) -> {
      for (String n : oldNodes) {
        if(Objects.equals(n, thisNode)) continue;
        if(!newNodes.contains(n)) {
          //this node has gone down, clear data
          nodeVsTagsCache.remove(n);
        }
      }
      return isClosed;
    });

  }

  @Override
  public Map<String, Object> getSysProps(String nodeName, Collection<String> tags) {
    Map<String, Object> cached = nodeVsTagsCache.computeIfAbsent(nodeName, s -> new LinkedHashMap<>());
    Map<String, Object> result = new LinkedHashMap<>();
    for (String tag : tags) {
      if (!cached.containsKey(tag)) {
        return fetchProps(nodeName, tags);
      } else {
        result.put(tag, cached.get(tag));
      }
    }
    return result;
  }

  private Map<String, Object> fetchProps(String nodeName, Collection<String> tags) {
    StringBuilder sb = new StringBuilder(zkStateReader.getBaseUrlForNodeName(nodeName));
    sb.append("?omitHeader=true&wt=javabin");
    LinkedHashMap<String,String> keys= new LinkedHashMap<>();
    for (String tag : tags) {
      String metricsKey = "solr.jvm:system.properties:"+tag;
      keys.put(tag, metricsKey);
      sb.append("&key").append(metricsKey);
    }
    Map<String, Object> result = new LinkedHashMap<>();
    NavigableObject response = (NavigableObject) Utils.executeGET( cloudSolrClient.getHttpClient(),sb.toString(), Utils.JAVABINCONSUMER);
    @SuppressWarnings("unchecked")
    Map<String,Object> metrics = (Map<String, Object>) response._get("metrics", Collections.emptyMap());
    keys.forEach((tag, key) -> result.put(tag, metrics.get(key)));
    nodeVsTagsCache.computeIfAbsent(nodeName, s -> new LinkedHashMap<>()).putAll(result);
    return result;
  }

  @Override
  public void close() {
    isClosed = true;

  }
}
