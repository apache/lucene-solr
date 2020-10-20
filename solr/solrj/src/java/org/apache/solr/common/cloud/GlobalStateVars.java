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

import static org.apache.solr.common.cloud.ZkStateReader.URL_SCHEME;

/**
 * Singleton access to global vars in persisted state, such as the urlScheme, which although is stored in ZK as a cluster property
 * really should be treated like a static global that is set at initialization and not altered after.
 */
public class GlobalStateVars {

  public static final String HTTP = "http";
  public static final String HTTPS = "https";
  public static final String SCHEME_VAR = "${scheme}://";
  public static final String HTTPS_PORT_PROP = "solr.jetty.https.port";

  private static final GlobalStateVars _singleton = new GlobalStateVars();

  public static GlobalStateVars singleton() {
    return _singleton;
  }

  private String urlScheme = System.getProperty(URL_SCHEME, HTTP);

  // no new! you have to use the static singleton!
  private GlobalStateVars() {
  }

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

  /**
   * Given a URL with a replaceable parameter for the scheme, return a new URL with the correct scheme applied.
   * @param url A URL to change the scheme (http|https)
   * @return A new URL with the correct scheme or null if the supplied url remains unchanged.
   */
  public String applyUrlSchemeIfChanged(final String url) {
    if (url == null || url.isEmpty())
      return url;

    if (url.startsWith(SCHEME_VAR)) {
      // replace ${scheme} with actual scheme
      return urlScheme + url.substring(SCHEME_VAR.length()-3); // keep the ://
    } else {
      // heal an incorrect scheme if needed, otherwise return null indicating no change
      final int at = url.indexOf("://");
      return (at == -1) || urlScheme.equals(url.substring(0,at)) ? null /* no change needed */ : urlScheme + url.substring(at);
    }
  }

  public String getUrlScheme() {
    return urlScheme;
  }
}
