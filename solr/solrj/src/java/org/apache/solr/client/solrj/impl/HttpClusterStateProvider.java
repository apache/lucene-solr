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
package org.apache.solr.client.solrj.impl;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient.RemoteSolrException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ClusterState.CollectionRef;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpClusterStateProvider implements ClusterStateProvider {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private String urlScheme;
  volatile Set<String> liveNodes;
  long liveNodesTimestamp = 0;
  volatile Map<String, String> aliases;
  long aliasesTimestamp = 0;

  private int cacheTimeout = 5; // the liveNodes and aliases cache will be invalidated after 5 secs
  final HttpClient httpClient;
  final boolean clientIsInternal;

  public HttpClusterStateProvider(List<String> solrUrls, HttpClient httpClient) throws Exception {
    this.httpClient = httpClient == null? HttpClientUtil.createClient(null): httpClient;
    this.clientIsInternal = httpClient == null;
    for (String solrUrl: solrUrls) {
      urlScheme = solrUrl.startsWith("https")? "https": "http";
      try (SolrClient initialClient = new HttpSolrClient.Builder().withBaseSolrUrl(solrUrl).withHttpClient(httpClient).build()) {
        Set<String> liveNodes = fetchLiveNodes(initialClient); // throws exception if unable to fetch
        this.liveNodes = liveNodes;
        liveNodesTimestamp = System.nanoTime();
        break;
      } catch (IOException e) {
        log.warn("Attempt to fetch live_nodes from " + solrUrl + " failed.", e);
      }
    }

    if (this.liveNodes == null || this.liveNodes.isEmpty()) {
      throw new RuntimeException("Tried fetching live_nodes using Solr URLs provided, i.e. " + solrUrls + ". However, "
          + "succeeded in obtaining the cluster state from none of them."
          + "If you think your Solr cluster is up and is accessible,"
          + " you could try re-creating a new CloudSolrClient using working"
          + " solrUrl(s) or zkHost(s).");
    }
  }

  @Override
  public void close() throws IOException {
    if (this.clientIsInternal && this.httpClient != null) {
      HttpClientUtil.close(httpClient);
    }
  }

  @Override
  public CollectionRef getState(String collection) {
    for (String nodeName: liveNodes) {
      try (HttpSolrClient client = new HttpSolrClient.Builder().
          withBaseSolrUrl(ZkStateReader.getBaseUrlForNodeName(nodeName, urlScheme)).
          withHttpClient(httpClient).build()) {
        ClusterState cs = fetchClusterState(client, collection);
        return cs.getCollectionRef(collection);
      } catch (SolrServerException | RemoteSolrException | IOException e) {
        if (e.getMessage().contains(collection + " not found")) {
          // Cluster state for the given collection was not found.
          // Lets fetch/update our aliases:
          getAliases(true);
          return null;
        }
        log.warn("Attempt to fetch cluster state from " +
            ZkStateReader.getBaseUrlForNodeName(nodeName, urlScheme) + " failed.", e);
      }
    }
    throw new RuntimeException("Tried fetching cluster state using the node names we knew of, i.e. " + liveNodes +". However, "
        + "succeeded in obtaining the cluster state from none of them."
        + "If you think your Solr cluster is up and is accessible,"
        + " you could try re-creating a new CloudSolrClient using working"
        + " solrUrl(s) or zkHost(s).");
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private ClusterState fetchClusterState(SolrClient client, String collection) throws SolrServerException, IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("collection", collection);
    params.set("action", "CLUSTERSTATUS");
    QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    NamedList cluster = (SimpleOrderedMap) client.request(request).get("cluster");
    Map<String, Object> collectionsMap = Collections.singletonMap(collection,
        ((NamedList) cluster.get("collections")).get(collection));
    int znodeVersion = (int)((Map<String, Object>)(collectionsMap).get(collection)).get("znodeVersion");
    Set<String> liveNodes = new HashSet((List<String>)(cluster.get("live_nodes")));
    this.liveNodes = liveNodes;
    liveNodesTimestamp = System.nanoTime();
    ClusterState cs = ClusterState.load(znodeVersion, collectionsMap, liveNodes, ZkStateReader.CLUSTER_STATE);
    return cs;
  }

  @Override
  public Set<String> liveNodes() {
    if (liveNodes == null) {
      throw new RuntimeException("We don't know of any live_nodes to fetch the"
          + " latest live_nodes information from. "
          + "If you think your Solr cluster is up and is accessible,"
          + " you could try re-creating a new CloudSolrClient using working"
          + " solrUrl(s) or zkHost(s).");
    }
    if (TimeUnit.SECONDS.convert((System.nanoTime() - liveNodesTimestamp), TimeUnit.NANOSECONDS) > getCacheTimeout()) {
      for (String nodeName: liveNodes) {
        try (HttpSolrClient client = new HttpSolrClient.Builder().
            withBaseSolrUrl(ZkStateReader.getBaseUrlForNodeName(nodeName, urlScheme)).
            withHttpClient(httpClient).build()) {
          Set<String> liveNodes = fetchLiveNodes(client);
          this.liveNodes = (liveNodes);
          liveNodesTimestamp = System.nanoTime();
          return liveNodes;
        } catch (Exception e) {
          log.warn("Attempt to fetch live_nodes from " +
              ZkStateReader.getBaseUrlForNodeName(nodeName, urlScheme) + " failed.", e);
        }
      }
      throw new RuntimeException("Tried fetching live_nodes using all the node names we knew of, i.e. " + liveNodes +". However, "
          + "succeeded in obtaining the cluster state from none of them."
          + "If you think your Solr cluster is up and is accessible,"
          + " you could try re-creating a new CloudSolrClient using working"
          + " solrUrl(s) or zkHost(s).");
    } else {
      return liveNodes; // cached copy is fresh enough
    }
  }

  private static Set<String> fetchLiveNodes(SolrClient client) throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", "CLUSTERSTATUS");
    QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    NamedList cluster = (SimpleOrderedMap) client.request(request).get("cluster");
    Set<String> liveNodes = new HashSet((List<String>)(cluster.get("live_nodes")));
    return liveNodes;
  }

  @Override
  public String getAlias(String alias) {
    Map<String, String> aliases = getAliases(false);
    return aliases.get(alias);
  }

  private Map<String, String> getAliases(boolean forceFetch) {
    if (this.liveNodes == null) {
      throw new RuntimeException("We don't know of any live_nodes to fetch the"
          + " latest aliases information from. "
          + "If you think your Solr cluster is up and is accessible,"
          + " you could try re-creating a new CloudSolrClient using working"
          + " solrUrl(s) or zkHost(s).");
    }

    if (forceFetch || this.aliases == null ||
        TimeUnit.SECONDS.convert((System.nanoTime() - aliasesTimestamp), TimeUnit.NANOSECONDS) > getCacheTimeout()) {
      for (String nodeName: liveNodes) {
        try (HttpSolrClient client = new HttpSolrClient.Builder().
            withBaseSolrUrl(ZkStateReader.getBaseUrlForNodeName(nodeName, urlScheme)).
            withHttpClient(httpClient).build()) {

          Map<String, String> aliases = new CollectionAdminRequest.ListAliases().process(client).getAliases();
          this.aliases = aliases;
          this.aliasesTimestamp = System.nanoTime();
          return Collections.unmodifiableMap(aliases);
        } catch (SolrServerException | RemoteSolrException | IOException e) {
          // Situation where we're hitting an older Solr which doesn't have LISTALIASES
          if (e instanceof RemoteSolrException && ((RemoteSolrException)e).code()==400) {
            log.warn("LISTALIASES not found, possibly using older Solr server. Aliases won't work"
                + " unless you re-create the CloudSolrClient using zkHost(s) or upgrade Solr server", e);
            this.aliases = Collections.emptyMap();
            this.aliasesTimestamp = System.nanoTime();
            return aliases;
          }
          log.warn("Attempt to fetch cluster state from " +
              ZkStateReader.getBaseUrlForNodeName(nodeName, urlScheme) + " failed.", e);
        }
      }

      throw new RuntimeException("Tried fetching aliases using all the node names we knew of, i.e. " + liveNodes +". However, "
          + "succeeded in obtaining the cluster state from none of them."
          + "If you think your Solr cluster is up and is accessible,"
          + " you could try re-creating a new CloudSolrClient using a working"
          + " solrUrl or zkHost.");
    } else {
      return Collections.unmodifiableMap(this.aliases); // cached copy is fresh enough
    }
  }

  @Override
  public String getCollectionName(String name) {
    Map<String, String> aliases = getAliases(false);
    return aliases.containsKey(name) ? aliases.get(name): name;
  }

  @Override
  public Object getClusterProperty(String propertyName) {
    if (propertyName.equals(ZkStateReader.URL_SCHEME)) {
      return this.urlScheme;
    }
    throw new UnsupportedOperationException("Fetching cluster properties not supported"
        + " using the HttpClusterStateProvider. "
        + "ZkClientClusterStateProvider can be used for this."); // TODO
  }

  @Override
  public Object getClusterProperty(String propertyName, String def) {
    if (propertyName.equals(ZkStateReader.URL_SCHEME)) {
      return this.urlScheme;
    }
    throw new UnsupportedOperationException("Fetching cluster properties not supported"
        + " using the HttpClusterStateProvider. "
        + "ZkClientClusterStateProvider can be used for this."); // TODO
  }

  @Override
  public void connect() {}

  public int getCacheTimeout() {
    return cacheTimeout;
  }

  public void setCacheTimeout(int cacheTimeout) {
    this.cacheTimeout = cacheTimeout;
  }

}
