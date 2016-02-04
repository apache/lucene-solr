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

import java.util.Collection;

import org.apache.http.cookie.ClientCookie;
import org.apache.http.cookie.Cookie;
import org.apache.http.cookie.CookieOrigin;
import org.apache.http.cookie.CookieSpec;
import org.apache.http.cookie.CookieSpecFactory;
import org.apache.http.cookie.CookieSpecProvider;
import org.apache.http.cookie.MalformedCookieException;
import org.apache.http.cookie.params.CookieSpecPNames;
import org.apache.http.impl.cookie.NetscapeDomainHandler;
import org.apache.http.impl.cookie.NetscapeDraftSpec;
import org.apache.http.params.HttpParams;
import org.apache.http.protocol.HttpContext;

@SuppressWarnings("deprecation")
public class SolrPortAwareCookieSpecFactory implements CookieSpecFactory, CookieSpecProvider {
  public static final String POLICY_NAME = "solr-portaware";
  private final CookieSpec cookieSpec;

  public SolrPortAwareCookieSpecFactory(final String[] datepatterns) {
    super();
    this.cookieSpec = new PortAwareCookieSpec(datepatterns);
  }

  public SolrPortAwareCookieSpecFactory() {
    this(null);
  }

  @Override
  public CookieSpec newInstance(final HttpParams params) {
    if (params != null) {
      String[] patterns = null;
      final Collection<?> param = (Collection<?>) params.getParameter(
          CookieSpecPNames.DATE_PATTERNS);
      if (param != null) {
        patterns = new String[param.size()];
        patterns = param.toArray(patterns);
      }
      return new PortAwareCookieSpec(patterns);
    } else {
      return new PortAwareCookieSpec(null);
    }
  }

  @Override
  public CookieSpec create(final HttpContext context) {
    return this.cookieSpec;
  }

  public static class PortAwareCookieSpec extends NetscapeDraftSpec {
    public PortAwareCookieSpec(String patterns[]) {
      super(patterns);
      super.registerAttribHandler(ClientCookie.DOMAIN_ATTR, new PortAwareDomainHandler());
    }

    public PortAwareCookieSpec() {
      this(null);
    }
  }

  /**
   * A domain handler to validate and match cookies based on the domain and origin.
   * The domain is tested against host and port both, and if it doesn't match, it
   * delegates the handling to the base class' matching/validation logic.
   */
  public static class PortAwareDomainHandler extends NetscapeDomainHandler {

    public void validate(final Cookie cookie, final CookieOrigin origin)
        throws MalformedCookieException {
      if (origin != null && origin.getHost() != null && cookie != null) {
        String hostPort = origin.getHost() + ":" + origin.getPort();
        String domain = cookie.getDomain();

        if (hostPort.equals(domain)) {
          return;
        }
      }
      super.validate(cookie, origin);
    }

    @Override
    public boolean match(final Cookie cookie, final CookieOrigin origin) {
      if (origin != null && origin.getHost() != null && cookie != null) {
        String hostPort = origin.getHost() + ":" + origin.getPort();
        String domain = cookie.getDomain();
        if (hostPort.equals(domain)) {
          return true;
        }
      }
      return super.match(cookie, origin);
    }
  }
}

