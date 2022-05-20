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
import javax.servlet.http.HttpServletResponse;
import java.nio.ByteBuffer;
import java.security.Principal;
import java.security.PublicKey;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.time.Instant;

import org.apache.http.HttpHeaders;
import org.apache.solr.common.util.Base64;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.http.Header;
import org.apache.http.auth.BasicUserPrincipal;
import org.apache.http.message.BasicHttpRequest;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.CryptoKeys;
import org.mockito.ArgumentMatchers;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestPKIAuthenticationPlugin extends SolrTestCaseJ4 {

  static class MockPKIAuthenticationPlugin extends PKIAuthenticationPlugin {
    SolrRequestInfo solrRequestInfo;

    Map<String, PublicKey> remoteKeys = new ConcurrentHashMap<>();

    public MockPKIAuthenticationPlugin(String node) {
      super(null, node, new PublicKeyHandler());
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

  String headerKey = PKIAuthenticationPlugin.HEADER;
  final AtomicReference<Header> header = new AtomicReference<>();
  final AtomicReference<ServletRequest> wrappedRequestByFilter = new AtomicReference<>();
  final FilterChain filterChain = (servletRequest, servletResponse) -> wrappedRequestByFilter.set(servletRequest);
  final CryptoKeys.RSAKeyPair aKeyPair = new CryptoKeys.RSAKeyPair();

  public void test() throws Exception {
    assumeWorkingMockito();
    
    AtomicReference<Principal> principal = new AtomicReference<>();
    String nodeName = "node_x_233";

    if (random().nextBoolean()) {
      headerKey = PKIAuthenticationPlugin.HEADER_V2;
      System.setProperty(PKIAuthenticationPlugin.SEND_VERSION, "v2");
    }

    final MockPKIAuthenticationPlugin mock = new MockPKIAuthenticationPlugin(nodeName);
    LocalSolrQueryRequest localSolrQueryRequest = new LocalSolrQueryRequest(null, new ModifiableSolrParams()) {
      @Override
      public Principal getUserPrincipal() {
        return principal.get();
      }
    };
    PublicKey correctKey = CryptoKeys.deserializeX509PublicKey(mock.getPublicKey());
    mock.remoteKeys.put(nodeName, correctKey);

    String username = "solr user"; // with spaces
    principal.set(new BasicUserPrincipal(username));
    mock.solrRequestInfo = new SolrRequestInfo(localSolrQueryRequest, new SolrQueryResponse());
    BasicHttpRequest request = new BasicHttpRequest("GET", "http://localhost:56565");
    mock.setHeader(request);
    header.set(request.getFirstHeader(headerKey));
    assertNotNull(header.get());
    assertTrue(header.get().getValue().startsWith(nodeName));
    HttpServletRequest mockReq = createMockRequest(header);
    assertTrue(mock.authenticate(mockReq, null, filterChain));

    assertNotNull(wrappedRequestByFilter.get());
    assertNotNull(((HttpServletRequest) wrappedRequestByFilter.get()).getUserPrincipal());
    assertEquals(username, ((HttpServletRequest) wrappedRequestByFilter.get()).getUserPrincipal().getName());

    //test 3 . No user request . Request originated from Solr
    //create pub key in advance because it can take time and it should be
    //created before the header is set
    PublicKey key = new CryptoKeys.RSAKeyPair().getPublicKey();
    mock.solrRequestInfo = null;
    header.set(null);
    wrappedRequestByFilter.set(null);
    request = new BasicHttpRequest("GET", "http://localhost:56565");
    mock.setHeader(request);
    header.set(request.getFirstHeader(headerKey));
    assertNotNull(header.get());
    assertTrue(header.get().getValue().startsWith(nodeName));

    assertTrue(mock.authenticate(mockReq, null, filterChain));
    assertNotNull(wrappedRequestByFilter.get());
    assertEquals("$", ((HttpServletRequest) wrappedRequestByFilter.get()).getUserPrincipal().getName());

    /*test4 mock the restart of a node*/
    MockPKIAuthenticationPlugin mock1 = new MockPKIAuthenticationPlugin(nodeName) {
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

    assertTrue(mock1.authenticate(mockReq, null,filterChain));
    assertNotNull(wrappedRequestByFilter.get());
    assertEquals("$", ((HttpServletRequest) wrappedRequestByFilter.get()).getUserPrincipal().getName());
    mock1.close();
    mock.close();

    /* test protocol mismatch */
    wrappedRequestByFilter.set(null);

    System.setProperty(PKIAuthenticationPlugin.SEND_VERSION, "v1");
    System.setProperty(PKIAuthenticationPlugin.ACCEPT_VERSIONS, "v2");
    MockPKIAuthenticationPlugin mock2 = new MockPKIAuthenticationPlugin(nodeName);

    principal.set(new BasicUserPrincipal("solr"));
    mock2.solrRequestInfo = new SolrRequestInfo(localSolrQueryRequest, new SolrQueryResponse());
    mock2.setHeader(request);

    HttpServletResponse response = mock(HttpServletResponse.class);
    assertFalse("Should have failed authentication", mock2.authenticate(mockReq, response, filterChain));

    verify(response).setHeader(HttpHeaders.WWW_AUTHENTICATE, PKIAuthenticationPlugin.HEADER_V2);
    verify(response).sendError(ArgumentMatchers.eq(401), anyString());

    assertNull("Should not have proceeded after authentication failure", wrappedRequestByFilter.get());

    mock2.close();
  }

  public void testParseCipher() {
    for (String validUser: new String[]{"user1", "$", "some user","some 123"}) {
      for (long validTimestamp: new long[]{Instant.now().toEpochMilli(), 99999999999L, 9999999999999L}) {
        String s = validUser + " " + validTimestamp;
        byte[] payload = s.getBytes(UTF_8);
        byte[] payloadCipher = aKeyPair.encrypt(ByteBuffer.wrap(payload));
        String base64Cipher = Base64.byteArrayToBase64(payloadCipher);
        PKIAuthenticationPlugin.PKIHeaderData header = PKIAuthenticationPlugin.parseCipher(base64Cipher, aKeyPair.getPublicKey(), false);
        assertNotNull("Expecting valid header for user " + validUser + " and timestamp " + validTimestamp, header);
        assertEquals(validUser, header.userName);
        assertEquals(validTimestamp, header.timestamp);
      }
    }
  }

  public void testParseCipherInvalidTimestampTooSmall() {
    long timestamp = 999999999L;
    String s = "user1 " + timestamp;

    byte[] payload = s.getBytes(UTF_8);
    byte[] payloadCipher = aKeyPair.encrypt(ByteBuffer.wrap(payload));
    String base64Cipher = Base64.byteArrayToBase64(payloadCipher);
    assertNull(PKIAuthenticationPlugin.parseCipher(base64Cipher, aKeyPair.getPublicKey(), false));
  }

  public void testParseCipherInvalidTimestampTooBig() {
    long timestamp = 10000000000000L;
    String s = "user1 " + timestamp;

    byte[] payload = s.getBytes(UTF_8);
    byte[] payloadCipher = aKeyPair.encrypt(ByteBuffer.wrap(payload));
    String base64Cipher = Base64.byteArrayToBase64(payloadCipher);
    assertNull(PKIAuthenticationPlugin.parseCipher(base64Cipher, aKeyPair.getPublicKey(), false));
  }

  public void testParseCipherInvalidKey() {
    String s = "user1 " + Instant.now().toEpochMilli();
    byte[] payload = s.getBytes(UTF_8);
    byte[] payloadCipher = aKeyPair.encrypt(ByteBuffer.wrap(payload));
    String base64Cipher = Base64.byteArrayToBase64(payloadCipher);
    assertNull(PKIAuthenticationPlugin.parseCipher(base64Cipher, new CryptoKeys.RSAKeyPair().getPublicKey(), false));
  }

  public void testParseCipherNoSpace() {
    String s = "user1" + Instant.now().toEpochMilli(); // missing space

    byte[] payload = s.getBytes(UTF_8);
    byte[] payloadCipher = aKeyPair.encrypt(ByteBuffer.wrap(payload));
    String base64Cipher = Base64.byteArrayToBase64(payloadCipher);
    assertNull(PKIAuthenticationPlugin.parseCipher(base64Cipher, aKeyPair.getPublicKey(), false));
  }

  public void testParseCipherNoTimestamp() {
    String s = "user1 aaaaaaaaaa";

    byte[] payload = s.getBytes(UTF_8);
    byte[] payloadCipher = aKeyPair.encrypt(ByteBuffer.wrap(payload));
    String base64Cipher = Base64.byteArrayToBase64(payloadCipher);
    assertNull(PKIAuthenticationPlugin.parseCipher(base64Cipher, aKeyPair.getPublicKey(), false));
  }

  public void testParseCipherInvalidKeyExample() {
    /*
    This test shows a case with an invalid public key for which the decrypt will return an output that triggers SOLR-15961.
     */
    String base64Cipher = "A8tEkMfmA5m5+wVG9xSI46Lhg8MqDFkjPVqXc6Tf6LT/EVIpW3DUrkIygIjk9tSCCAxhHwSvKfVJeujaBtxr19ajmpWjtZKgZOXkynF5aPbDuI+mnvCiTmhLuZYExvnmeYxag6A4Fu2TpA/Wo97S4cIkRgfyag/ZOYM0pZwVAtNoJgTpmODDGrH4W16BXSZ6xm+EV4vrfUqpuuO7U7YiU5fd1tv22Au0ZaY6lPbxAHjeFyD8WrkPPIkEoM14K0G5vAg4wUxpRF/eVlnzhULoPgKFErz7cKVxuvxSsYpVw5oko+ldzyfsnMrC1brqUKA7NxhpdpJzp7bmd8W8/mvZEw==";
    String publicKey = "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAsJu1O+A/gGikFSeLGYdgNPrz3ef/tqJP1sRqzkVjnBcdyI2oXMmAWF+yDe0Zmya+HevyOI8YN2Yaq6aCLjbHnT364Rno/urhKvR5PmaH/PqXrh3Dl+vn08B74iLVZxZro/v34FGjX8fkiasZggC4AnyLjFkU7POsHhJKSXGslsWe0dq7yaaA2AES/bFwJ3r3FNxUsE+kWEtZG1RKMq8P8wlx/HLDzjYKaGnyApAltBHVx60XHiOC9Oatu5HZb/eKU3jf7sKibrzrRsqwb+iE4ZxxtXkgATuLOl/2ks5Mnkk4u7bPEAgEpEuzQBB4AahMC7r+R5AzRnB4+xx69FP1IwIDAQAB";
    assertNull(PKIAuthenticationPlugin.parseCipher(base64Cipher, CryptoKeys.deserializeX509PublicKey(publicKey), false));
  }

  private HttpServletRequest createMockRequest(final AtomicReference<Header> header) {
    HttpServletRequest mockReq = mock(HttpServletRequest.class);
    when(mockReq.getHeader(any(String.class))).then(invocation -> {
      if (headerKey.equals(invocation.getArgument(0))) {
        if (header.get() == null) return null;
        return header.get().getValue();
      } else return null;
    });
    when(mockReq.getRequestURI()).thenReturn("/collection1/select");
    return mockReq;
  }
}
