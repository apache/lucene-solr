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
import static org.mockito.Mockito.*;

public class TestPKIAuthenticationPlugin extends SolrTestCaseJ4 {
  HttpServletRequest mockReq;
  FilterChain filterChain;
  final AtomicReference<ServletRequest> wrappedRequestByFilter = new AtomicReference<>();
  final AtomicReference<Header> header = new AtomicReference<>();
  AtomicReference<Principal> principal = new AtomicReference<>();
  BasicHttpRequest request;


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
    request = new BasicHttpRequest("GET", "http://localhost:56565");
    mock.setHeader(request);
    header.set(request.getFirstHeader(PKIAuthenticationPlugin.HEADER));
    assertNotNull(header.get());
    assertTrue(header.get().getValue().startsWith(nodeName));
    mockReq = createMockRequest(header);
    filterChain = (servletRequest, servletResponse) -> wrappedRequestByFilter.set(servletRequest);


    run("solr", () -> {
      mock.doAuthenticate(mockReq, null, filterChain);
    });


    //test 2

    run(null, () -> {
      principal.set(null); // no user
      header.set(null);
      wrappedRequestByFilter.set(null);//
      request = new BasicHttpRequest("GET", "http://localhost:56565");
      mock.setHeader(request);
      assertNull(request.getFirstHeader(PKIAuthenticationPlugin.HEADER));
      mock.doAuthenticate(mockReq, null, filterChain);
    });

    //test 3 . No user request . Request originated from Solr
    run("$", () -> {
      mock.solrRequestInfo = null;
      header.set(null);
      wrappedRequestByFilter.set(null);
      request = new BasicHttpRequest("GET", "http://localhost:56565");
      mock.setHeader(request);
      header.set(request.getFirstHeader(PKIAuthenticationPlugin.HEADER));
      assertNotNull(header.get());
      assertTrue(header.get().getValue().startsWith(nodeName));
      mock.doAuthenticate(mockReq, null, filterChain);
    });

    run("$", () -> {
      mock.solrRequestInfo = null;
      header.set(null);
      wrappedRequestByFilter.set(null);
      request = new BasicHttpRequest("GET", "http://localhost:56565");
      mock.setHeader(request);
      header.set(request.getFirstHeader(PKIAuthenticationPlugin.HEADER));
      assertNotNull(header.get());
      assertTrue(header.get().getValue().startsWith(nodeName));
      MockPKIAuthenticationPlugin mock1 = new MockPKIAuthenticationPlugin(null, nodeName) {
        int called = 0;

        @Override
        PublicKey getRemotePublicKey(String nodename) {
          try {
            return called == 0 ? new CryptoKeys.RSAKeyPair().getPublicKey() : correctKey;
          } finally {
            called++;
          }
        }
      };

      mock1.doAuthenticate(mockReq, null, filterChain);

    });

  }

  interface Runnable {
    void run() throws Exception;
  }

  private void run(String expected, Runnable r) throws Exception {
    int failures = 0;
    for (; ; ) {
      r.run();
      if (expected == null) {
        assertTrue(wrappedRequestByFilter.get() == null || ((HttpServletRequest) wrappedRequestByFilter.get()).getUserPrincipal() == null);
      } else {
        assertNotNull(wrappedRequestByFilter.get());
        if (((HttpServletRequest) wrappedRequestByFilter.get()).getUserPrincipal() == null) {
          //may be timed out
          if (++failures < 3) continue;
          else
            fail("No principal obtained");
        }
        assertEquals(expected, ((HttpServletRequest) wrappedRequestByFilter.get()).getUserPrincipal().getName());
      }
      return;

    }
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
