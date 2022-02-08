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

import java.util.Objects;

import org.apache.solr.common.util.Utils;

/**
 * Deprecated ~ do not use!
 * Singleton access to global urlScheme, which although is stored in ZK as a cluster property
 * really should be treated like a static global that is set at initialization and not altered after.
 *
 * Client applications should not use this class directly; it is only included in SolrJ because Replica
 * and ZkNodeProps depend on it.
 * @deprecated to be removed in Solr 9.0 (see: SOLR-15587)
 */
@Deprecated
public enum UrlScheme {
  INSTANCE;

  public static final String HTTP = "http";
  public static final String HTTPS = "https";
  public static final String HTTPS_PORT_PROP = "solr.jetty.https.port";

  private volatile String urlScheme = HTTP;

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

  public String getBaseUrlForNodeName(String nodeName) {
    Objects.requireNonNull(nodeName,"node_name must not be null");
    return Utils.getBaseUrlForNodeName(nodeName, urlScheme);
  }

  /**
   * Given a URL string with or without a scheme, return a new URL with the correct scheme applied.
   * @param url A URL to change the scheme (http|https)
   * @return A new URL with the correct scheme
   */
  public String applyUrlScheme(final String url) {
    Objects.requireNonNull(url, "URL must not be null!");

    // heal an incorrect scheme if needed, otherwise return null indicating no change
    final int at = url.indexOf("://");
    return (at == -1) ? (urlScheme + "://" + url) : urlScheme + url.substring(at);
  }

  public String getUrlScheme() {
    return urlScheme;
  }
}
