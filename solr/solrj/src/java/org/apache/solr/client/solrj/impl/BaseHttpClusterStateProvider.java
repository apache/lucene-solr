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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.client.solrj.impl.BaseHttpSolrClient.*;

public abstract class BaseHttpClusterStateProvider implements ClusterStateProvider {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private String urlScheme;
  volatile Set<String> liveNodes;
  long liveNodesTimestamp = 0;
  volatile Map<String, List<String>> aliases;
  volatile Map<String, Map<String, String>> aliasProperties;
  long aliasesTimestamp = 0;

  private int cacheTimeout = 5; // the liveNodes and aliases cache will be invalidated after 5 secs

  public void init(List<String> solrUrls) throws Exception {
    for (String solrUrl: solrUrls) {
      urlScheme = solrUrl.startsWith("https")? "https": "http";
      try (SolrClient initialClient = getSolrClient(solrUrl)) {
        this.liveNodes = fetchLiveNodes(initialClient);
        liveNodesTimestamp = System.nanoTime();
        break;
      } catch (IOException e) {
        log.warn("Attempt to fetch cluster state from {} failed.", solrUrl, e);
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

  protected abstract SolrClient getSolrClient(String baseUrl);

  @Override
  public ClusterState.CollectionRef getState(String collection) {
    for (String nodeName: liveNodes) {
      String baseUrl = Utils.getBaseUrlForNodeName(nodeName, urlScheme);
      try (SolrClient client = getSolrClient(baseUrl)) {
        ClusterState cs = fetchClusterState(client, collection, null);
        return cs.getCollectionRef(collection);
      } catch (SolrServerException | IOException e) {
        log.warn("Attempt to fetch cluster state from " +
            Utils.getBaseUrlForNodeName(nodeName, urlScheme) + " failed.", e);
      } catch (RemoteSolrException e) {
        if ("NOT_FOUND".equals(e.getMetadata("CLUSTERSTATUS"))) {
          return null;
        }
        log.warn("Attempt to fetch cluster state from {} failed.", baseUrl, e);
      } catch (NotACollectionException e) {
        // Cluster state for the given collection was not found, could be an alias.
        // Lets fetch/update our aliases:
        getAliases(true);
        return null;
      }
    }
    throw new RuntimeException("Tried fetching cluster state using the node names we knew of, i.e. " + liveNodes +". However, "
        + "succeeded in obtaining the cluster state from none of them."
        + "If you think your Solr cluster is up and is accessible,"
        + " you could try re-creating a new CloudSolrClient using working"
        + " solrUrl(s) or zkHost(s).");
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private ClusterState fetchClusterState(SolrClient client, String collection, Map<String, Object> clusterProperties) throws SolrServerException, IOException, NotACollectionException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    if (collection != null) {
      params.set("collection", collection);
    }
    params.set("action", "CLUSTERSTATUS");
    QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    NamedList cluster = (SimpleOrderedMap) client.request(request).get("cluster");
    Map<String, Object> collectionsMap;
    if (collection != null) {
      collectionsMap = Collections.singletonMap(collection,
          ((NamedList) cluster.get("collections")).get(collection));
    } else {
      collectionsMap = ((NamedList)cluster.get("collections")).asMap(10);
    }
    int znodeVersion;
    Map<String, Object> collFromStatus = (Map<String, Object>) (collectionsMap).get(collection);
    if (collection != null && collFromStatus == null) {
      throw new NotACollectionException(); // probably an alias
    }
    if (collection != null) { // can be null if alias
      znodeVersion =  (int) collFromStatus.get("znodeVersion");
    } else {
      znodeVersion = -1;
    }
    Set<String> liveNodes = new HashSet((List<String>)(cluster.get("live_nodes")));
    this.liveNodes = liveNodes;
    liveNodesTimestamp = System.nanoTime();
    //TODO SOLR-11877 we don't know the znode path; CLUSTER_STATE is probably wrong leading to bad stateFormat
    ClusterState cs = ClusterState.load(znodeVersion, collectionsMap, liveNodes, ZkStateReader.CLUSTER_STATE);
    if (clusterProperties != null) {
      Map<String, Object> properties = (Map<String, Object>) cluster.get("properties");
      if (properties != null) {
        clusterProperties.putAll(properties);
      }
    }
    return cs;
  }

  @Override
  public Set<String> getLiveNodes() {
    if (liveNodes == null) {
      throw new RuntimeException("We don't know of any live_nodes to fetch the"
          + " latest live_nodes information from. "
          + "If you think your Solr cluster is up and is accessible,"
          + " you could try re-creating a new CloudSolrClient using working"
          + " solrUrl(s) or zkHost(s).");
    }
    if (TimeUnit.SECONDS.convert((System.nanoTime() - liveNodesTimestamp), TimeUnit.NANOSECONDS) > getCacheTimeout()) {
      for (String nodeName: liveNodes) {
        String baseUrl = Utils.getBaseUrlForNodeName(nodeName, urlScheme);
        try (SolrClient client = getSolrClient(baseUrl)) {
          Set<String> liveNodes = fetchLiveNodes(client);
          this.liveNodes = (liveNodes);
          liveNodesTimestamp = System.nanoTime();
          return liveNodes;
        } catch (Exception e) {
          log.warn("Attempt to fetch cluster state from {} failed.", baseUrl, e);
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
    return (Set<String>) new HashSet((List<String>)(cluster.get("live_nodes")));
  }

  @Override
  public List<String> resolveAlias(String aliasName) {
    return resolveAlias(aliasName, false);
  }

  public List<String> resolveAlias(String aliasName, boolean forceFetch) {
    return Aliases.resolveAliasesGivenAliasMap(getAliases(forceFetch), aliasName);
  }

  @Override
  public String resolveSimpleAlias(String aliasName) throws IllegalArgumentException {
    return Aliases.resolveSimpleAliasGivenAliasMap(getAliases(false), aliasName);
  }

  private Map<String, List<String>> getAliases(boolean forceFetch) {
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
        String baseUrl = Utils.getBaseUrlForNodeName(nodeName, urlScheme);
        try (SolrClient client = getSolrClient(baseUrl)) {

          CollectionAdminResponse response = new CollectionAdminRequest.ListAliases().process(client);
          this.aliases = response.getAliasesAsLists();
          this.aliasProperties = response.getAliasProperties(); // side-effect
          this.aliasesTimestamp = System.nanoTime();
          return Collections.unmodifiableMap(this.aliases);
        } catch (SolrServerException | RemoteSolrException | IOException e) {
          // Situation where we're hitting an older Solr which doesn't have LISTALIASES
          if (e instanceof RemoteSolrException && ((RemoteSolrException)e).code()==400) {
            log.warn("LISTALIASES not found, possibly using older Solr server. Aliases won't work"
                + " unless you re-create the CloudSolrClient using zkHost(s) or upgrade Solr server", e);
            this.aliases = Collections.emptyMap();
            this.aliasProperties = Collections.emptyMap();
            this.aliasesTimestamp = System.nanoTime();
            return aliases;
          }
          log.warn("Attempt to fetch cluster state from {} failed.", baseUrl, e);
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
  public Map<String, String> getAliasProperties(String alias) {
    getAliases(false);
    return Collections.unmodifiableMap(aliasProperties.getOrDefault(alias, Collections.emptyMap()));
  }

  @Override
  @SuppressWarnings("deprecation")
  public ClusterState getClusterState() throws IOException {
    for (String nodeName: liveNodes) {
      String baseUrl = Utils.getBaseUrlForNodeName(nodeName, urlScheme);
      try (SolrClient client = getSolrClient(baseUrl)) {
        return fetchClusterState(client, null, null);
      } catch (SolrServerException | HttpSolrClient.RemoteSolrException | IOException e) {
        log.warn("Attempt to fetch cluster state from {} failed.", baseUrl, e);
      } catch (NotACollectionException e) {
        // not possible! (we passed in null for collection so it can't be an alias)
        throw new RuntimeException("null should never cause NotACollectionException in " +
            "fetchClusterState() Please report this as a bug!");
      }
    }
    throw new RuntimeException("Tried fetching cluster state using the node names we knew of, i.e. " + liveNodes +". However, "
        + "succeeded in obtaining the cluster state from none of them."
        + "If you think your Solr cluster is up and is accessible,"
        + " you could try re-creating a new CloudSolrClient using working"
        + " solrUrl(s) or zkHost(s).");
  }

  @Override
  @SuppressWarnings("deprecation")
  public Map<String, Object> getClusterProperties() {
    for (String nodeName : liveNodes) {
      String baseUrl = Utils.getBaseUrlForNodeName(nodeName, urlScheme);
      try (SolrClient client = getSolrClient(baseUrl)) {
        Map<String, Object> clusterProperties = new HashMap<>();
        fetchClusterState(client, null, clusterProperties);
        return clusterProperties;
      } catch (SolrServerException | HttpSolrClient.RemoteSolrException | IOException e) {
        log.warn("Attempt to fetch cluster state from {} failed.", baseUrl, e);
      } catch (NotACollectionException e) {
        // not possible! (we passed in null for collection so it can't be an alias)
        throw new RuntimeException("null should never cause NotACollectionException in " +
            "fetchClusterState() Please report this as a bug!");
      }
    }
    throw new RuntimeException("Tried fetching cluster state using the node names we knew of, i.e. " + liveNodes + ". However, "
        + "succeeded in obtaining the cluster state from none of them."
        + "If you think your Solr cluster is up and is accessible,"
        + " you could try re-creating a new CloudSolrClient using working"
        + " solrUrl(s) or zkHost(s).");
  }

  @Override
  public String getPolicyNameByCollection(String coll) {
    throw new UnsupportedOperationException("Fetching cluster properties not supported"
        + " using the HttpClusterStateProvider. "
        + "ZkClientClusterStateProvider can be used for this."); // TODO
  }

  @Override
  public Object getClusterProperty(String propertyName) {
    if (propertyName.equals(ZkStateReader.URL_SCHEME)) {
      return this.urlScheme;
    }
    return getClusterProperties().get(propertyName);
  }

  @Override
  public void connect() {}

  public int getCacheTimeout() {
    return cacheTimeout;
  }

  public void setCacheTimeout(int cacheTimeout) {
    this.cacheTimeout = cacheTimeout;
  }

  // This exception is not meant to escape this class it should be caught and wrapped.
  private class NotACollectionException extends Exception {
  }
}
