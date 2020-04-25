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

import org.apache.http.cookie.CookieAttributeHandler;
import org.apache.http.cookie.CookieOrigin;
import org.apache.http.cookie.MalformedCookieException;
import org.apache.http.impl.cookie.BasicClientCookie;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.Assert;
import org.junit.Test;

// Test cases imported from TestNetscapeCookieAttribHandlers of HttpClient project
public class SolrPortAwareCookieSpecTest {

  @Test
  public void testDomainHostPortValidate() throws Exception {
    final BasicClientCookie cookie = new BasicClientCookie("name", "value");
    final CookieOrigin origin = new CookieOrigin("somehost", 80, "/", false);
    final CookieAttributeHandler h = new SolrPortAwareCookieSpecFactory.PortAwareDomainHandler();

    cookie.setDomain("somehost:80");
    h.validate(cookie, origin);

    cookie.setDomain("somehost:1234");
    SolrTestCaseJ4.expectThrows(MalformedCookieException.class, () -> h.validate(cookie, origin));
  }

  @Test
  public void testDomainHostPortMatch() throws Exception {
    final BasicClientCookie cookie = new BasicClientCookie("name", "value");
    final CookieOrigin origin = new CookieOrigin("myhost", 80, "/", false);
    final CookieAttributeHandler h = new SolrPortAwareCookieSpecFactory.PortAwareDomainHandler();

    cookie.setDomain("myhost");
    SolrTestCaseJ4.expectThrows(IllegalArgumentException.class, () -> h.match(cookie, null));

    cookie.setDomain(null);
    Assert.assertFalse(h.match(cookie, origin));

    cookie.setDomain("otherhost");
    Assert.assertFalse(h.match(cookie, origin));

    cookie.setDomain("myhost");
    Assert.assertTrue(h.match(cookie, origin));

    cookie.setDomain("myhost:80");
    Assert.assertTrue(h.match(cookie, origin));

    cookie.setDomain("myhost:8080");
    Assert.assertFalse(h.match(cookie, origin));
  }

  @Test
  public void testDomainValidate1() throws Exception {
    final BasicClientCookie cookie = new BasicClientCookie("name", "value");
    final CookieOrigin origin = new CookieOrigin("somehost", 80, "/", false);
    final CookieAttributeHandler h = new SolrPortAwareCookieSpecFactory.PortAwareDomainHandler();

    cookie.setDomain("somehost");
    h.validate(cookie, origin);

    cookie.setDomain("otherhost");
    SolrTestCaseJ4.expectThrows(MalformedCookieException.class, () ->  h.validate(cookie, origin));
  }

  @Test
  public void testDomainValidate2() throws Exception {
    final BasicClientCookie cookie = new BasicClientCookie("name", "value");
    final CookieOrigin origin = new CookieOrigin("www.somedomain.com", 80, "/", false);
    final CookieAttributeHandler h = new SolrPortAwareCookieSpecFactory.PortAwareDomainHandler();

    cookie.setDomain(".somedomain.com");
    h.validate(cookie, origin);

    cookie.setDomain(".otherdomain.com");
    SolrTestCaseJ4.expectThrows(MalformedCookieException.class, () ->  h.validate(cookie, origin));

    cookie.setDomain("www.otherdomain.com");
    SolrTestCaseJ4.expectThrows(MalformedCookieException.class, () ->  h.validate(cookie, origin));
  }

  @Test
  public void testDomainValidate3() throws Exception {
    final BasicClientCookie cookie = new BasicClientCookie("name", "value");
    final CookieOrigin origin = new CookieOrigin("www.a.com", 80, "/", false);
    final CookieAttributeHandler h = new SolrPortAwareCookieSpecFactory.PortAwareDomainHandler();

    cookie.setDomain(".a.com");
    h.validate(cookie, origin);

    cookie.setDomain(".com");
    SolrTestCaseJ4.expectThrows(MalformedCookieException.class, () ->  h.validate(cookie, origin));
  }

  @Test
  public void testDomainValidate4() throws Exception {
    final BasicClientCookie cookie = new BasicClientCookie("name", "value");
    final CookieOrigin origin = new CookieOrigin("www.a.b.c", 80, "/", false);
    final CookieAttributeHandler h = new SolrPortAwareCookieSpecFactory.PortAwareDomainHandler();

    cookie.setDomain(".a.b.c");
    h.validate(cookie, origin);

    cookie.setDomain(".b.c");
    SolrTestCaseJ4.expectThrows(MalformedCookieException.class, () ->  h.validate(cookie, origin));
  }

  @Test
  public void testDomainMatch1() throws Exception {
    final BasicClientCookie cookie = new BasicClientCookie("name", "value");
    final CookieOrigin origin = new CookieOrigin("www.somedomain.com", 80, "/", false);
    final CookieAttributeHandler h = new SolrPortAwareCookieSpecFactory.PortAwareDomainHandler();

    cookie.setDomain(null);
    Assert.assertFalse(h.match(cookie, origin));

    cookie.setDomain(".somedomain.com");
    Assert.assertTrue(h.match(cookie, origin));
  }

  @Test
  public void testDomainMatch2() throws Exception {
    final BasicClientCookie cookie = new BasicClientCookie("name", "value");
    final CookieOrigin origin = new CookieOrigin("www.whatever.somedomain.com", 80, "/", false);
    final CookieAttributeHandler h = new SolrPortAwareCookieSpecFactory.PortAwareDomainHandler();

    cookie.setDomain(".somedomain.com");
    Assert.assertTrue(h.match(cookie, origin));
  }

  @Test
  public void testDomainInvalidInput() throws Exception {
    final CookieAttributeHandler h = new SolrPortAwareCookieSpecFactory.PortAwareDomainHandler();
    SolrTestCaseJ4.expectThrows(IllegalArgumentException.class, () -> h.match(null, null));
    SolrTestCaseJ4.expectThrows(IllegalArgumentException.class,
        () -> h.match(new BasicClientCookie("name", "value"), null));
  }

}
