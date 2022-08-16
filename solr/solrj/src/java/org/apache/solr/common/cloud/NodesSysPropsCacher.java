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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.NavigableObject;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;

/** Fetch lazily and cache a node's system properties */
public class NodesSysPropsCacher implements AutoCloseable {
  private volatile boolean isClosed = false;
  private final Map<String, Map<String, Object>> nodeVsTagsCache = new ConcurrentHashMap<>();
  private ZkStateReader zkStateReader;
  private final SolrClient solrClient;

  public NodesSysPropsCacher(SolrClient solrClient, ZkStateReader zkStateReader) {
    this.zkStateReader = zkStateReader;
    this.solrClient = solrClient;
    zkStateReader.registerLiveNodesListener(
        (oldNodes, newNodes) -> {
          for (String n : oldNodes) {
            if (!newNodes.contains(n)) {
              // this node has gone down, clear data
              nodeVsTagsCache.remove(n);
            }
          }
          return isClosed;
        });
  }

  public Map<String, Object> getSysProps(String nodeName, Collection<String> tags) {
    Map<String, Object> cached =
        nodeVsTagsCache.computeIfAbsent(nodeName, s -> new LinkedHashMap<>());
    HashMap<String, Object> result = new HashMap<>();
    for (String tag : tags) {
      if (!cached.containsKey(tag)) {
        // at least one property is missing. fetch properties from the node
        Map<String, Object> props = fetchProps(nodeName, tags);
        // make a copy
        cached = new LinkedHashMap<>(cached);
        // merge all properties
        cached.putAll(props);
        // update the cache with the new set of properties
        nodeVsTagsCache.put(nodeName, cached);
        return props;
      } else {
        result.put(tag, cached.get(tag));
      }
    }
    return result;
  }

  private Map<String, Object> fetchProps(String nodeName, Collection<String> tags) {
    StringBuilder sb = new StringBuilder(zkStateReader.getBaseUrlForNodeName(nodeName));
    sb.append("/admin/metrics?omitHeader=true&wt=javabin");
    ModifiableSolrParams msp = new ModifiableSolrParams();
    msp.add(CommonParams.OMIT_HEADER, "true");
    LinkedHashMap<String, String> keys = new LinkedHashMap<>();
    for (String tag : tags) {
      String metricsKey = "solr.jvm:system.properties:" + tag;
      keys.put(tag, metricsKey);
      msp.add("key", metricsKey);
    }

    GenericSolrRequest req = new GenericSolrRequest(SolrRequest.METHOD.GET, "/admin/metrics", msp);
    req.setBasePath(zkStateReader.getBaseUrlForNodeName(nodeName));
    try {
      LinkedHashMap<String, Object> result = new LinkedHashMap<>();
      NavigableObject response = solrClient.request(req);
      NavigableObject metrics = (NavigableObject) response._get("metrics", MapWriter.EMPTY);
      keys.forEach((tag, key) -> result.put(tag, metrics._get(key, null)));
      return result;
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  @Override
  public void close() {
    isClosed = true;
  }
}
