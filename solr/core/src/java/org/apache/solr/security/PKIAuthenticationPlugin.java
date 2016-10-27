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
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.security.Principal;
import java.security.PublicKey;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponse;
import org.apache.http.auth.BasicUserPrincipal;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.SolrHttpClientBuilder;
import org.apache.solr.common.util.Base64;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.CryptoKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;


public class PKIAuthenticationPlugin extends AuthenticationPlugin implements HttpClientBuilderPlugin {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final Map<String, PublicKey> keyCache = new ConcurrentHashMap<>();
  private final CryptoKeys.RSAKeyPair keyPair = new CryptoKeys.RSAKeyPair();
  private final CoreContainer cores;
  private final int MAX_VALIDITY = Integer.parseInt(System.getProperty("pkiauth.ttl", "5000"));
  private final String myNodeName;
  private final HttpHeaderClientInterceptor interceptor = new HttpHeaderClientInterceptor();
  private boolean interceptorRegistered = false;

  public void setInterceptorRegistered(){
    this.interceptorRegistered = true;
  }

  public boolean isInterceptorRegistered(){
    return interceptorRegistered;
  }

  public PKIAuthenticationPlugin(CoreContainer cores, String nodeName) {
    this.cores = cores;
    myNodeName = nodeName;
  }

  @Override
  public void init(Map<String, Object> pluginConfig) {
  }


  @SuppressForbidden(reason = "Needs currentTimeMillis to compare against time in header")
  @Override
  public boolean doAuthenticate(ServletRequest request, ServletResponse response, FilterChain filterChain) throws Exception {

    String requestURI = ((HttpServletRequest) request).getRequestURI();
    if (requestURI.endsWith(PATH)) {
      filterChain.doFilter(request, response);
      return true;
    }
    long receivedTime = System.currentTimeMillis();
    String header = ((HttpServletRequest) request).getHeader(HEADER);
    if (header == null) {
      //this must not happen
      log.error("No SolrAuth header present");
      filterChain.doFilter(request, response);
      return true;
    }

    List<String> authInfo = StrUtils.splitWS(header, false);
    if (authInfo.size() < 2) {
      log.error("Invalid SolrAuth Header {}", header);
      filterChain.doFilter(request, response);
      return true;
    }

    String nodeName = authInfo.get(0);
    String cipher = authInfo.get(1);

    PKIHeaderData decipher = decipherHeader(nodeName, cipher);
    if (decipher == null) {
      log.error("Could not decipher a header {} . No principal set", header);
      filterChain.doFilter(request, response);
      return true;
    }
    if ((receivedTime - decipher.timestamp) > MAX_VALIDITY) {
      log.error("Invalid key request timestamp: {} , received timestamp: {} , TTL: {}", decipher.timestamp, receivedTime, MAX_VALIDITY);
        filterChain.doFilter(request, response);
        return true;
    }

    final Principal principal = "$".equals(decipher.userName) ?
        SU :
        new BasicUserPrincipal(decipher.userName);

    filterChain.doFilter(getWrapper((HttpServletRequest) request, principal), response);
    return true;
  }

  private static HttpServletRequestWrapper getWrapper(final HttpServletRequest request, final Principal principal) {
    return new HttpServletRequestWrapper(request) {
      @Override
      public Principal getUserPrincipal() {
        return principal;
      }
    };
  }

  public static class PKIHeaderData {
    String userName;
    long timestamp;
  }

  private PKIHeaderData decipherHeader(String nodeName, String cipherBase64) {
    PublicKey key = keyCache.get(nodeName);
    if (key == null) {
      log.debug("No key available for node : {} fetching now ", nodeName);
      key = getRemotePublicKey(nodeName);
      log.debug("public key obtained {} ", key);
    }

    PKIHeaderData header = parseCipher(cipherBase64, key);
    if (header == null) {
      log.warn("Failed to decrypt header, trying after refreshing the key ");
      key = getRemotePublicKey(nodeName);
      return parseCipher(cipherBase64, key);
    } else {
      return header;
    }
  }

  private static  PKIHeaderData parseCipher(String cipher, PublicKey key) {
    byte[] bytes;
    try {
      bytes = CryptoKeys.decryptRSA(Base64.base64ToByteArray(cipher), key);
    } catch (Exception e) {
      log.error("Decryption failed , key must be wrong", e);
      return null;
    }
    String s = new String(bytes, UTF_8).trim();
    String[] ss = s.split(" ");
    if (ss.length < 2) {
      log.warn("Invalid cipher {} deciphered data {}", cipher, s);
      return null;
    }
    PKIHeaderData headerData = new PKIHeaderData();
    try {
      headerData.timestamp = Long.parseLong(ss[1]);
      headerData.userName = ss[0];
      log.debug("Successfully decrypted header {} {}", headerData.userName, headerData.timestamp);
      return headerData;
    } catch (NumberFormatException e) {
      log.warn("Invalid cipher {}", cipher);
      return null;
    }
  }

  PublicKey getRemotePublicKey(String nodename) {
    String url = cores.getZkController().getZkStateReader().getBaseUrlForNodeName(nodename);
    try {
      String uri = url + PATH + "?wt=json&omitHeader=true";
      log.debug("Fetching fresh public key from : {}",uri);
      HttpResponse rsp = cores.getUpdateShardHandler().getHttpClient()
          .execute(new HttpGet(uri), HttpClientUtil.createNewHttpClientRequestContext());
      byte[] bytes = EntityUtils.toByteArray(rsp.getEntity());
      Map m = (Map) Utils.fromJSON(bytes);
      String key = (String) m.get("key");
      if (key == null) {
        log.error("No key available from " + url + PATH);
        return null;
      } else {
        log.info("New Key obtained from  node: {} / {}", nodename, key);
      }
      PublicKey pubKey = CryptoKeys.deserializeX509PublicKey(key);
      keyCache.put(nodename, pubKey);
      return pubKey;
    } catch (Exception e) {
      log.error("Exception trying to get public key from : " + url, e);
      return null;
    }

  }

  @Override
  public SolrHttpClientBuilder getHttpClientBuilder(SolrHttpClientBuilder builder) {
    HttpClientUtil.addRequestInterceptor(interceptor);
    return builder;
  }

  public SolrRequestHandler getRequestHandler() {
    return new RequestHandlerBase() {
      @Override
      public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
        rsp.add("key", keyPair.getPublicKeyStr());
      }

      @Override
      public String getDescription() {
        return "Return the public key of this server";
      }
    };
  }

  public boolean needsAuthorization(HttpServletRequest req) {
    return req.getUserPrincipal() != SU;
  }

  private class HttpHeaderClientInterceptor implements HttpRequestInterceptor {

    public HttpHeaderClientInterceptor() {
    }

    @Override
    public void process(HttpRequest httpRequest, HttpContext httpContext) throws HttpException, IOException {
      if (disabled()) return;
      setHeader(httpRequest);
    }
  }

  @SuppressForbidden(reason = "Needs currentTimeMillis to set current time in header")
  void setHeader(HttpRequest httpRequest) {
    SolrRequestInfo reqInfo = getRequestInfo();
    String usr;
    if (reqInfo != null) {
      Principal principal = reqInfo.getReq().getUserPrincipal();
      if (principal == null) {
        //this had a request but not authenticated
        //so we don't not need to set a principal
        return;
      } else {
        usr = principal.getName();
      }
    } else {
      if (!isSolrThread()) {
        //if this is not running inside a Solr threadpool (as in testcases)
        // then no need to add any header
        return;
      }
      //this request seems to be originated from Solr itself
      usr = "$"; //special name to denote the user is the node itself
    }

    String s = usr + " " + System.currentTimeMillis();

    byte[] payload = s.getBytes(UTF_8);
    byte[] payloadCipher = keyPair.encrypt(ByteBuffer.wrap(payload));
    String base64Cipher = Base64.byteArrayToBase64(payloadCipher);
    httpRequest.setHeader(HEADER, myNodeName + " " + base64Cipher);
  }

  boolean isSolrThread() {
    return ExecutorUtil.isSolrServerThread();
  }

  SolrRequestInfo getRequestInfo() {
    return SolrRequestInfo.getRequestInfo();
  }

  boolean disabled() {
    return cores.getAuthenticationPlugin() == null ||
        cores.getAuthenticationPlugin() instanceof HttpClientBuilderPlugin;
  }

  @Override
  public void close() throws IOException {
    HttpClientUtil.removeRequestInterceptor(interceptor);
  }

  public String getPublicKey() {
    return keyPair.getPublicKeyStr();
  }

  public static final String HEADER = "SolrAuth";
  public static final String PATH = "/admin/info/key";
  public static final String NODE_IS_USER = "$";
  // special principal to denote the cluster member
  private static final Principal SU = new BasicUserPrincipal("$");
}
