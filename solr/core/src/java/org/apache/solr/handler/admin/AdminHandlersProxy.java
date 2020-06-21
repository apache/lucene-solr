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

package org.apache.solr.handler.admin;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Pair;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Static methods to proxy calls to an Admin (GET) API to other nodes in the cluster and return a combined response
 */
public class AdminHandlersProxy {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String PARAM_NODES = "nodes";

  // Proxy this request to a different remote node if 'node' parameter is provided
  public static boolean maybeProxyToNodes(SolrQueryRequest req, SolrQueryResponse rsp, CoreContainer container)
      throws IOException, SolrServerException, InterruptedException {
    String nodeNames = req.getParams().get(PARAM_NODES);
    if (nodeNames == null || nodeNames.isEmpty()) {
      return false; // local request
    }

    if (!container.isZooKeeperAware()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Parameter " + PARAM_NODES + " only supported in Cloud mode");
    }
    
    Set<String> nodes;
    String pathStr = req.getPath();
    
    @SuppressWarnings({"unchecked"})
    Map<String,String> paramsMap = req.getParams().toMap(new HashMap<>());
    paramsMap.remove(PARAM_NODES);
    SolrParams params = new MapSolrParams(paramsMap);
    Set<String> liveNodes = container.getZkController().zkStateReader.getClusterState().getLiveNodes();
    
    if (nodeNames.equals("all")) {
      nodes = liveNodes;
      log.debug("All live nodes requested");
    } else {
      nodes = new HashSet<>(Arrays.asList(nodeNames.split(",")));
      for (String nodeName : nodes) {
        if (!nodeName.matches("^[^/:]+:\\d+_[\\w/]+$")) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Parameter " + PARAM_NODES + " has wrong format");
        }

        if (!liveNodes.contains(nodeName)) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Requested node " + nodeName + " is not part of cluster");
        }
      }       
      log.debug("Nodes requested: {}", nodes);
    }
    if (log.isDebugEnabled()) {
      log.debug("{} parameter {} specified on {} request", PARAM_NODES, nodeNames, pathStr);
    }
    
    Map<String, Pair<Future<NamedList<Object>>, SolrClient>> responses = new HashMap<>();
    for (String node : nodes) {
      responses.put(node, callRemoteNode(node, pathStr, params, container.getZkController()));
    }
    
    for (Map.Entry<String, Pair<Future<NamedList<Object>>, SolrClient>> entry : responses.entrySet()) {
      try {
        NamedList<Object> resp = entry.getValue().first().get(10, TimeUnit.SECONDS);
        entry.getValue().second().close();
        rsp.add(entry.getKey(), resp);
      } catch (ExecutionException ee) {
        log.warn("Exception when fetching result from node {}", entry.getKey(), ee);
      } catch (TimeoutException te) {
        log.warn("Timeout when fetching result from node {}", entry.getKey(), te);
      }
    }
    if (log.isInfoEnabled()) {
      log.info("Fetched response from {} nodes: {}", responses.keySet().size(), responses.keySet());
    }
    return true;
  } 

  /**
   * Makes a remote request and returns a future and the solr client. The caller is responsible for closing the client 
   */
  public static Pair<Future<NamedList<Object>>, SolrClient> callRemoteNode(String nodeName, String endpoint, 
                                                                           SolrParams params, ZkController zkController) 
      throws IOException, SolrServerException {
    log.debug("Proxying {} request to node {}", endpoint, nodeName);
    URL baseUrl = new URL(zkController.zkStateReader.getBaseUrlForNodeName(nodeName));
    HttpSolrClient solr = new HttpSolrClient.Builder(baseUrl.toString()).build();
    @SuppressWarnings({"rawtypes"})
    SolrRequest proxyReq = new GenericSolrRequest(SolrRequest.METHOD.GET, endpoint, params);
    HttpSolrClient.HttpUriRequestResponse proxyResp = solr.httpUriRequest(proxyReq);
    return new Pair<>(proxyResp.future, solr);
  }
}
