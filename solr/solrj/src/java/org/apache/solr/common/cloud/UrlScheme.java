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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang.StringUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.URL_SCHEME;

/**
 * Singleton access to global vars in persisted state, such as the urlScheme, which although is stored in ZK as a cluster property
 * really should be treated like a static global that is set at initialization and not altered after.
 */
public enum UrlScheme implements LiveNodesListener, ClusterPropertiesListener {
  INSTANCE;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String HTTP = "http";
  public static final String HTTPS = "https";
  public static final String HTTPS_PORT_PROP = "solr.jetty.https.port";
  public static final String USE_LIVENODES_URL_SCHEME = "ext.useLiveNodesUrlScheme";

  private volatile String urlScheme = HTTP;
  private volatile boolean useLiveNodesUrlScheme = false;
  private volatile SortedSet<String> liveNodes = null;
  private volatile SolrZkClient zkClient = null;

  private final ConcurrentMap<String,String> nodeSchemeCache = new ConcurrentHashMap<>();

  /**
   * Called during ZkController initialization to set the urlScheme based on cluster properties.
   * @param client The SolrZkClient needed to read cluster properties from ZK.
   * @throws IOException If a connection or other I/O related error occurs while reading from ZK.
   */
  public void initFromClusterProps(final SolrZkClient client) throws IOException {
    this.zkClient = client;

    // Have to go directly to the cluster props b/c this needs to happen before ZkStateReader does its thing
    ClusterProperties clusterProps = new ClusterProperties(client);
    this.useLiveNodesUrlScheme =
      "true".equals(clusterProps.getClusterProperty(UrlScheme.USE_LIVENODES_URL_SCHEME, "false"));
    setUrlSchemeFromClusterProps(clusterProps.getClusterProperties());
  }

  private void setUrlSchemeFromClusterProps(Map<String,Object> props) {
    // Set the global urlScheme from cluster prop or if that is not set, look at the urlScheme sys prop
    final String scheme = (String)props.get(ZkStateReader.URL_SCHEME);
    if (StringUtils.isNotEmpty(scheme)) {
      // track the urlScheme in a global so we can use it during ZK read / write operations for cluster state objects
      this.urlScheme = HTTPS.equals(scheme) ? HTTPS : HTTP;
    } else {
      String urlSchemeFromSysProp = System.getProperty(URL_SCHEME, HTTP);
      if (HTTPS.equals(urlSchemeFromSysProp)) {
        log.warn("Cluster property 'urlScheme' not set but system property is set to 'https'. " +
            "You should set the cluster property and restart all nodes for consistency.");
      }
      // TODO: We may not want this? See: https://issues.apache.org/jira/browse/SOLR-10202
      this.urlScheme = HTTPS.equals(urlSchemeFromSysProp) ? HTTPS : HTTP;
    }
  }

  public boolean isOnServer() {
    return zkClient != null;
  }

  /**
   * Set the global urlScheme variable; ideally this should be immutable once set, but some tests rely on changing
   * the value on-the-fly.
   * @param urlScheme The new URL scheme, either http or https.
   */
  public void setUrlScheme(final String urlScheme) {
    if (!this.urlScheme.equals(urlScheme) && this.zkClient != null) {
      throw new IllegalStateException("Global, immutable 'urlScheme' already set for this node!");
    }

    if (HTTP.equals(urlScheme) || HTTPS.equals(urlScheme)) {
      this.urlScheme = urlScheme;
    } else {
      throw new IllegalArgumentException("Invalid urlScheme: "+urlScheme);
    }
  }

  public boolean useLiveNodesUrlScheme() {
    return useLiveNodesUrlScheme;
  }

  public String getBaseUrlForNodeName(String nodeName) {
    Objects.requireNonNull(nodeName,"node_name should not be null");
    if (nodeName.indexOf('_') == -1) {
      nodeName += '_'; // underscore required to indicate context
    }
    return Utils.getBaseUrlForNodeName(nodeName, getUrlSchemeForNodeName(nodeName));
  }

  public String getUrlSchemeForNodeName(final String nodeName) {
    return useLiveNodesUrlScheme ? getSchemeFromLiveNode(nodeName).orElse(urlScheme) : urlScheme;
  }

  /**
   * Given a URL with a replaceable parameter for the scheme, return a new URL with the correct scheme applied.
   * @param url A URL to change the scheme (http|https)
   * @return A new URL with the correct scheme
   */
  public String applyUrlScheme(final String url) {
    Objects.requireNonNull(url, "URL must not be null!");

    String updatedUrl;
    if (useLiveNodesUrlScheme && liveNodes != null) {
      updatedUrl = applyUrlSchemeFromLiveNodes(url);
      if (updatedUrl != null) {
        return updatedUrl;
      }
    }

    // heal an incorrect scheme if needed, otherwise return null indicating no change
    final int at = url.indexOf("://");
    return (at == -1) ? (urlScheme + "://" + url) : urlScheme + url.substring(at);
  }

  public String getUrlScheme() {
    return urlScheme;
  }

  @Override
  public boolean onChange(SortedSet<String> oldLiveNodes, SortedSet<String> newLiveNodes) {
    if (useLiveNodesUrlScheme) {
      liveNodes = newLiveNodes;
      if (liveNodes != null) {
        // we only need to clear the cached HTTP entries, keep the HTTPS
        // as we don't really support a graceful downgrade from HTTPS -> HTTP
        liveNodes.forEach(n -> {
          if (HTTP.equals(nodeSchemeCache.get(n))) {
            nodeSchemeCache.remove(n, HTTP);
          }
        });
      } else {
        nodeSchemeCache.clear();
      }
    } else {
      nodeSchemeCache.clear();
      liveNodes = null;
    }
    return !useLiveNodesUrlScheme;
  }

  private String applyUrlSchemeFromLiveNodes(final String url) {
    String updatedUrl = null;
    Optional<String> maybeFromLiveNode = getSchemeFromLiveNode(getNodeNameFromUrl(url));
    if (maybeFromLiveNode.isPresent()) {
      final int at = url.indexOf("://");
      // replace the scheme on the url with the one from the matching live node entry
      updatedUrl = maybeFromLiveNode.get() + ((at != -1) ? url.substring(at) : "://" + url);
    }
    return updatedUrl;
  }

  // Gets the urlScheme from the matching live node entry for this URL
  private Optional<String> getSchemeFromLiveNode(final String nodeName) {
    return (liveNodes != null && liveNodes.contains(nodeName)) ? Optional.ofNullable(getSchemeForLiveNode(nodeName)) : Optional.empty();
  }

  private String getNodeNameFromUrl(String url) {
    final int at = url.indexOf("://");
    if (at != -1) {
      url = url.substring(at+3);
    }
    String hostAndPort = url;
    String context = "";
    int slashAt = url.indexOf('/');
    if (slashAt != -1) {
      hostAndPort = url.substring(0, slashAt);
      // has context in url?s
      if (slashAt < url.length()-1) {
        context = url.substring(slashAt + 1);
      }
    }
    if (!context.isEmpty()) {
      context = URLEncoder.encode(trimLeadingAndTrailingSlashes(context), StandardCharsets.UTF_8);
    }
    return hostAndPort + "_" + context;
  }

  private String trimLeadingAndTrailingSlashes(final String in) {
    String out = in;
    if (out.startsWith("/")) {
      out = out.substring(1);
    }
    if (out.endsWith("/")) {
      out = out.substring(0, out.length() - 1);
    }
    return out;
  }

  @Override
  public boolean onChange(Map<String, Object> properties) {
    useLiveNodesUrlScheme = "true".equals(properties.getOrDefault(USE_LIVENODES_URL_SCHEME, "false"));
    if (!useLiveNodesUrlScheme) {
      nodeSchemeCache.clear();
      liveNodes = null;
    }
    setUrlSchemeFromClusterProps(properties);
    return !useLiveNodesUrlScheme; // if not using live nodes, no need to keep watching cluster props
  }

  private String getSchemeForLiveNode(String liveNode) {
    String scheme = nodeSchemeCache.get(liveNode);
    if (scheme == null) {
      final String nodePath = ZkStateReader.LIVE_NODES_ZKNODE + "/" + liveNode;
      try {
        byte[] data = zkClient.getData(nodePath, null, null, true);
        if (data != null) {
          scheme = new String(data, StandardCharsets.UTF_8);
        } else {
          scheme = HTTP;
        }
        nodeSchemeCache.put(liveNode, scheme);
      } catch (KeeperException.NoNodeException e) {
        // safe to ignore ...
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unable to read scheme for liveNode: "+liveNode, e);
      } catch (KeeperException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unable to read scheme for liveNode: "+liveNode, e);
      }
    }
    return scheme;
  }

  /**
   * Resets the global singleton back to initial state; only visible for testing!
   */
  public void reset() {
    this.zkClient = null;
    this.urlScheme = HTTP;
    this.useLiveNodesUrlScheme = false;
    this.liveNodes = null;
    this.nodeSchemeCache.clear();
  }
}
