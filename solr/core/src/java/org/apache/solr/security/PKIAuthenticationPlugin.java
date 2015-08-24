package org.apache.solr.security;

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

import javax.servlet.FilterChain;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
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
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.apache.solr.client.solrj.impl.HttpClientConfigurer;
import org.apache.solr.common.params.SolrParams;
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


public class PKIAuthenticationPlugin extends AuthenticationPlugin implements HttpClientInterceptorPlugin {
  static final Logger log = LoggerFactory.getLogger(PKIAuthenticationPlugin.class);
  private final Map<String, PublicKey> keyCache = new ConcurrentHashMap<>();
  private CryptoKeys.RSAKeyPair keyPair = new CryptoKeys.RSAKeyPair();
  private final CoreContainer cores;
  private int maxValidity = 5000;
  private final String myNodeName;

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
  public void doAuthenticate(ServletRequest request, ServletResponse response, FilterChain filterChain) throws Exception {

    String requestURI = ((HttpServletRequest) request).getRequestURI();
    if (requestURI.endsWith(PATH)) {
      filterChain.doFilter(request, response);
      return;
    }
    long receivedTime = System.currentTimeMillis();
    String header = ((HttpServletRequest) request).getHeader(HEADER);
    if (header == null) {
      log.error("No SolrAuth header present");
      filterChain.doFilter(request, response);
      return;
    }

    List<String> authInfo = StrUtils.splitWS(header, false);
    if (authInfo.size() < 2) {
      log.error("Invalid SolrAuth Header");
      return;
    }

    String nodeName = authInfo.get(0);
    String cipher = authInfo.get(1);

    byte[] decipher = decipherData(nodeName, cipher);
    if (decipher == null) {
      return;
    }

    String s = new String(decipher, StandardCharsets.UTF_8).trim();
    List<String> pcs = StrUtils.splitWS(s, false);
    if (pcs.size() < 2) {
      return;
    }

    final String userName = pcs.get(0);
    String timeStr = pcs.get(1);
    try {
      long timeMillis = Long.parseLong(timeStr);
      if ((receivedTime - timeMillis) > maxValidity) {
        log.error("Invalid key ");
        filterChain.doFilter(request, response);
        return;
      }
    } catch (NumberFormatException e) {
      log.error("Invalid time " + timeStr, e);
      return;
    }
    final Principal principal = "$".equals(userName) ?
        SU :
        new BasicUserPrincipal(userName);

    filterChain.doFilter(getWrapper((HttpServletRequest) request, principal), response);
  }

  private static HttpServletRequestWrapper getWrapper(final HttpServletRequest request, final Principal principal) {
    return new HttpServletRequestWrapper(request) {
      @Override
      public Principal getUserPrincipal() {
        return principal;
      }
    };
  }

  private byte[] decipherData(String nodeName, String cipherBase64) {
    boolean freshKey = false;
    PublicKey key = keyCache.get(nodeName);
    if (key == null) {
      key = getRemotePublicKey(nodeName);
      freshKey = true;
    }

    try {
      return CryptoKeys.decryptRSA(Base64.base64ToByteArray(cipherBase64), key);
    } catch (InvalidKeyException e) {
      if (!freshKey) {
        key = getRemotePublicKey(nodeName);
        if (key == null) {
          return null;
        }
        try {
          return CryptoKeys.decryptRSA(Base64.base64ToByteArray(cipherBase64), key);
        } catch (Exception e1) {
          log.error("Error decrypting");
          return null;
        }
      }

    } catch (Exception e) {
      log.error("Error decrypting");
      return null;
    }

    return null;
  }

  PublicKey getRemotePublicKey(String nodename) {
    String url = cores.getZkController().getZkStateReader().getBaseUrlForNodeName(nodename);
    try {
      HttpResponse rsp = cores.getUpdateShardHandler().getHttpClient().execute(new HttpGet(url + PATH + "?wt=json&omitHeader=true"));
      byte[] bytes = EntityUtils.toByteArray(rsp.getEntity());
      Map m = (Map) Utils.fromJSON(bytes);
      String key = (String) m.get("key");
      if (key == null) {
        log.error("No key available from " + url + PATH);
        return null;
      }
      PublicKey pubKey = CryptoKeys.deserializeX509PublicKey(key);
      keyCache.put(nodename, pubKey);
      return pubKey;
    } catch (Exception e) {
      log.error("Exception trying to get public key from : " + url, e);
      return null;
    }

  }

  private HttpHeaderClientConfigurer clientConfigurer = new HttpHeaderClientConfigurer();

  @Override
  public HttpClientConfigurer getClientConfigurer() {
    return clientConfigurer;
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
    if (req.getUserPrincipal() == SU) return false;

    return true;
  }

  private class HttpHeaderClientConfigurer extends HttpClientConfigurer implements
      HttpRequestInterceptor {

    @Override
    public void configure(DefaultHttpClient httpClient, SolrParams config) {
      super.configure(httpClient, config);
      httpClient.addRequestInterceptor(this);
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
    String usr = null;
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

    byte[] payload = s.getBytes(StandardCharsets.UTF_8);
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
        cores.getAuthenticationPlugin() instanceof HttpClientInterceptorPlugin;
  }

  @Override
  public void close() throws IOException {

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
