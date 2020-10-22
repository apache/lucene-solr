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

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;

import static org.apache.solr.common.cloud.ZkStateReader.URL_SCHEME;

/**
 * Singleton access to global vars in persisted state, such as the urlScheme, which although is stored in ZK as a cluster property
 * really should be treated like a static global that is set at initialization and not altered after.
 */
public enum UrlScheme implements LiveNodesListener, ClusterPropertiesListener {
  INSTANCE;

  public static final String parseHostAndPortFromLiveNode(String liveNode) {
    if (liveNode != null) {
      final int at = liveNode.indexOf(NODE_NAME_SCHEME_DELIM);
      if (at != -1) {
        liveNode = liveNode.substring(at+NODE_NAME_SCHEME_DELIM.length());
      }
      liveNode = liveNode.substring(0, liveNode.indexOf('_'));
    }
    return liveNode;
  }

  public static final String NODE_NAME_SCHEME_DELIM = "|";
  public static final String HTTP = "http";
  public static final String HTTPS = "https";
  public static final String SCHEME_VAR = "${scheme}://";
  public static final String HTTPS_PORT_PROP = "solr.jetty.https.port";
  public static final String USE_LIVENODES_URL_SCHEME = "useLiveNodesUrlScheme";

  private String urlScheme = HTTP;
  private boolean useLiveNodesUrlScheme = false;
  private SortedSet<String> liveNodes = null;

  /**
   * Set the global urlScheme variable; ideally this should be immutable once set, but some tests rely on changing
   * the value on-the-fly.
   * @param urlScheme The new URL scheme, either http or https.
   */
  public void setUrlScheme(final String urlScheme) {
    if (HTTP.equals(urlScheme) || HTTPS.equals(urlScheme)) {
      this.urlScheme = urlScheme;
    } else {
      throw new IllegalArgumentException("Invalid urlScheme: "+urlScheme);
    }
  }

  public String generateNodeName(final String host, final String port, final String context) {
    final String schemePrefix = HTTPS.equals(urlScheme) ? HTTPS+NODE_NAME_SCHEME_DELIM : "";
    try {
      return schemePrefix + host + ':' + port + '_' + URLEncoder.encode(trimLeadingAndTrailingSlashes(context), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new Error("JVM does not support UTF-8", e);
    }
  }

  public boolean useLiveNodesUrlScheme() {
    return useLiveNodesUrlScheme;
  }

  public void setUseLiveNodesUrlScheme(boolean useLiveNodesUrlScheme) {
    this.useLiveNodesUrlScheme = useLiveNodesUrlScheme;
  }

  /**
   * Given a URL with a replaceable parameter for the scheme, return a new URL with the correct scheme applied.
   * @param url A URL to change the scheme (http|https)
   * @return A new URL with the correct scheme or null if the supplied url remains unchanged.
   */
  public Optional<String> applyUrlScheme(final String url) {
    if (url == null || url.isEmpty())
      return Optional.empty();

    if (useLiveNodesUrlScheme && liveNodes != null) {
      String updatedUrl = applyUrlSchemeFromLiveNodes(url);
      if (updatedUrl != null) {
        return Optional.of(updatedUrl);
      }
    }

    // not able to resolve the urlScheme using live nodes ... use the global!

    Optional<String> maybeUpdatedUrl;
    if (url.startsWith(SCHEME_VAR)) {
      // replace ${scheme} with actual scheme
      maybeUpdatedUrl = Optional.of(urlScheme + url.substring(SCHEME_VAR.length()-3)); // keep the ://
    } else {
      // heal an incorrect scheme if needed, otherwise return null indicating no change
      final int at = url.indexOf("://");
      maybeUpdatedUrl = (at == -1) || urlScheme.equals(url.substring(0,at)) ? Optional.empty() /* no change needed */
          : Optional.of(urlScheme + url.substring(at));
    }
    return maybeUpdatedUrl;
  }

  public String getUrlScheme() {
    return urlScheme;
  }

  @Override
  public boolean onChange(SortedSet<String> oldLiveNodes, SortedSet<String> newLiveNodes) {
    if (useLiveNodesUrlScheme) {
      this.liveNodes = newLiveNodes;
    }
    return false;
  }

  private String applyUrlSchemeFromLiveNodes(final String url) {
    String updatedUrl = null;
    Optional<String> maybeFromLiveNode = getSchemeFromLiveNodesEntry(getNodeNameFromUrl(url));
    if (maybeFromLiveNode.isPresent()) {
      final int at = url.indexOf("://");
      // replace the scheme on the url with the one from the matching live node entry
      updatedUrl = maybeFromLiveNode.get() + ((at != -1) ? url.substring(at) : "://" + url);
    }
    return updatedUrl;
  }

  // Gets the urlScheme from the matching live node entry for this URL
  private Optional<String> getSchemeFromLiveNodesEntry(final String nodeNameFromUrl) {
    return liveNodes.parallelStream().filter(n -> {
      // strip off the urlScheme on the live node entry for matching ...
      final int at = n.indexOf(NODE_NAME_SCHEME_DELIM);
      return at != -1 && nodeNameFromUrl.equals(n.substring(at + NODE_NAME_SCHEME_DELIM.length()));
    }).map(n -> n.substring(0, n.indexOf(NODE_NAME_SCHEME_DELIM))).findFirst();
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
    try {
      context = URLEncoder.encode(trimLeadingAndTrailingSlashes(context), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
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
    Object prop = properties.get(USE_LIVENODES_URL_SCHEME);
    useLiveNodesUrlScheme = prop != null && "true".equals(prop.toString());
    setUrlScheme((String)properties.getOrDefault(URL_SCHEME, HTTP));
    return false;
  }
}
