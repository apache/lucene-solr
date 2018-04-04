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
package org.apache.solr.security;

import javax.servlet.FilterChain;
import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletRequest;
import java.security.Principal;
import java.security.PublicKey;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.http.Header;
import org.apache.http.auth.BasicUserPrincipal;
import org.apache.http.message.BasicHttpRequest;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.CryptoKeys;
import org.junit.Test;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestPKIAuthenticationPlugin extends SolrTestCaseJ4 {

  static class MockPKIAuthenticationPlugin extends PKIAuthenticationPlugin {
    SolrRequestInfo solrRequestInfo;

    Map<String, PublicKey> remoteKeys = new HashMap<>();

    public MockPKIAuthenticationPlugin(CoreContainer cores, String node) {
      super(cores, node);
    }

    @Override
    boolean disabled() {
      return false;
    }

    @Override
    SolrRequestInfo getRequestInfo() {
      return solrRequestInfo;
    }

    @Override
    PublicKey getRemotePublicKey(String nodename) {
      return remoteKeys.get(nodename);
    }

    @Override
    boolean isSolrThread() {
      return true;
    }
  }

  public void test() throws Exception {
    assumeWorkingMockito();

    AtomicReference<Principal> principal = new AtomicReference<>();
    String nodeName = "node_x_233";

    final MockPKIAuthenticationPlugin mock = new MockPKIAuthenticationPlugin(null, nodeName);
    LocalSolrQueryRequest localSolrQueryRequest = new LocalSolrQueryRequest(null, new ModifiableSolrParams()) {
      @Override
      public Principal getUserPrincipal() {
        return principal.get();
      }
    };
    PublicKey correctKey = CryptoKeys.deserializeX509PublicKey(mock.getPublicKey());
    mock.remoteKeys.put(nodeName, correctKey);

    principal.set(new BasicUserPrincipal("solr"));
    mock.solrRequestInfo = new SolrRequestInfo(localSolrQueryRequest, new SolrQueryResponse());
    BasicHttpRequest request = new BasicHttpRequest("GET", "http://localhost:56565");
    mock.setHeader(request);
    final AtomicReference<Header> header = new AtomicReference<>();
    header.set(request.getFirstHeader(PKIAuthenticationPlugin.HEADER));
    assertNotNull(header.get());
    assertTrue(header.get().getValue().startsWith(nodeName));
    final AtomicReference<ServletRequest> wrappedRequestByFilter = new AtomicReference<>();
    HttpServletRequest mockReq = createMockRequest(header);
    FilterChain filterChain = (servletRequest, servletResponse) -> wrappedRequestByFilter.set(servletRequest);
    mock.doAuthenticate(mockReq, null, filterChain);

    assertNotNull(wrappedRequestByFilter.get());
    assertEquals("solr", ((HttpServletRequest) wrappedRequestByFilter.get()).getUserPrincipal().getName());

    //test 2
    principal.set(null); // no user
    header.set(null);
    wrappedRequestByFilter.set(null);//
    request = new BasicHttpRequest("GET", "http://localhost:56565");
    mock.setHeader(request);
    assertNull(request.getFirstHeader(PKIAuthenticationPlugin.HEADER));
    mock.doAuthenticate(mockReq, null, filterChain);
    assertNotNull(wrappedRequestByFilter.get());
    assertNull(((HttpServletRequest) wrappedRequestByFilter.get()).getUserPrincipal());

    //test 3 . No user request . Request originated from Solr
    //create pub key in advance because it can take time and it should be
    //created before the header is set
    PublicKey key = new CryptoKeys.RSAKeyPair().getPublicKey();
    mock.solrRequestInfo = null;
    header.set(null);
    wrappedRequestByFilter.set(null);
    request = new BasicHttpRequest("GET", "http://localhost:56565");
    mock.setHeader(request);
    header.set(request.getFirstHeader(PKIAuthenticationPlugin.HEADER));
    assertNotNull(header.get());
    assertTrue(header.get().getValue().startsWith(nodeName));

    mock.doAuthenticate(mockReq, null, filterChain);
    assertNotNull(wrappedRequestByFilter.get());
    assertEquals("$", ((HttpServletRequest) wrappedRequestByFilter.get()).getUserPrincipal().getName());

    /*test4 mock the restart of a node*/
    MockPKIAuthenticationPlugin mock1 = new MockPKIAuthenticationPlugin(null, nodeName) {
      int called = 0;
      @Override
      PublicKey getRemotePublicKey(String nodename) {
        try {
          return called == 0 ? key : correctKey;
        } finally {
          called++;
        }
      }
    };

    mock1.doAuthenticate(mockReq, null,filterChain );
    assertNotNull(wrappedRequestByFilter.get());
    assertEquals("$", ((HttpServletRequest) wrappedRequestByFilter.get()).getUserPrincipal().getName());
    mock1.close();
    mock.close();
  }

  @Test
  public void testGetBaseUrlForNodeNameLocal() {
    final MockPKIAuthenticationPlugin mock = new MockPKIAuthenticationPlugin(null, "myName");
    assertEquals("http://my.host:9876/solr2", mock.getBaseUrlForNodeNameLocal("my.host:9876_solr2"));
    System.setProperty("solr.jetty.keystore", "foo");
    assertEquals("https://my.host:9876/solr2", mock.getBaseUrlForNodeNameLocal("my.host:9876_solr2"));
    System.clearProperty("solr.jetty.keystore");
  }

  @Test
  public void testResolveUrlScheme() {
    System.clearProperty("urlScheme");
    System.clearProperty("solr.jetty.keystore");
    assertEquals("http", MockPKIAuthenticationPlugin.resolveUrlScheme());
    System.setProperty("urlScheme", "http");
    assertEquals("http", MockPKIAuthenticationPlugin.resolveUrlScheme());
    System.setProperty("urlScheme", "https");
    assertEquals("https", MockPKIAuthenticationPlugin.resolveUrlScheme());
    System.setProperty("urlScheme", "ftp");
    System.clearProperty("solr.jetty.keystore");
    assertEquals("http", MockPKIAuthenticationPlugin.resolveUrlScheme());
    System.setProperty("solr.jetty.keystore", "foo");
    assertEquals("https", MockPKIAuthenticationPlugin.resolveUrlScheme());
    System.clearProperty("urlScheme");
    System.clearProperty("solr.jetty.keystore");
  }

  private HttpServletRequest createMockRequest(final AtomicReference<Header> header) {
    HttpServletRequest mockReq = mock(HttpServletRequest.class);
    when(mockReq.getHeader(any(String.class))).then(invocation -> {
      if (PKIAuthenticationPlugin.HEADER.equals(invocation.getArgument(0))) {
        if (header.get() == null) return null;
        return header.get().getValue();
      } else return null;
    });
    when(mockReq.getRequestURI()).thenReturn("/collection1/select");
    return mockReq;
  }
}
