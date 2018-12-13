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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.entity.ContentType;
import org.apache.http.protocol.HttpContext;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.cloud.SolrCloudAuthTestCase;
import org.apache.solr.common.util.Pair;
import org.jose4j.jwk.PublicJsonWebKey;
import org.jose4j.jwk.RsaJsonWebKey;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwt.JwtClaims;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Validate that JWT token authentication works in a real cluster.
 * TODO: Test also using SolrJ as client. But that requires a way to set Authorization header on request
 */
public class JWTAuthPluginIntegrationTest extends SolrCloudAuthTestCase {
  protected static final int NUM_SERVERS = 2;
  protected static final int NUM_SHARDS = 2;
  protected static final int REPLICATION_FACTOR = 1;
  private static final String COLLECTION = "jwtColl";
  private static String jwtTestToken;
  private static String baseUrl;
  private static AtomicInteger jwtInterceptCount = new AtomicInteger();
  private static AtomicInteger pkiInterceptCount = new AtomicInteger();
  private static final CountInterceptor interceptor = new CountInterceptor();

  @BeforeClass
  public static void setupClass() throws Exception {
    configureCluster(NUM_SERVERS)// nodes
        .withSecurityJson(TEST_PATH().resolve("security").resolve("jwt_plugin_jwk_security.json"))
        .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
    String hostport = cluster.getSolrClient().getClusterStateProvider().getLiveNodes().iterator().next().split("_")[0];
    baseUrl = "http://" + hostport + "/solr/";

    String jwkJSON = "{\n" +
        "  \"kty\": \"RSA\",\n" +
        "  \"d\": \"i6pyv2z3o-MlYytWsOr3IE1olu2RXZBzjPRBNgWAP1TlLNaphHEvH5aHhe_CtBAastgFFMuP29CFhaL3_tGczkvWJkSveZQN2AHWHgRShKgoSVMspkhOt3Ghha4CvpnZ9BnQzVHnaBnHDTTTfVgXz7P1ZNBhQY4URG61DKIF-JSSClyh1xKuMoJX0lILXDYGGcjVTZL_hci4IXPPTpOJHV51-pxuO7WU5M9252UYoiYyCJ56ai8N49aKIMsqhdGuO4aWUwsGIW4oQpjtce5eEojCprYl-9rDhTwLAFoBtjy6LvkqlR2Ae5dKZYpStljBjK8PJrBvWZjXAEMDdQ8PuQ\",\n" +
        "  \"e\": \"AQAB\",\n" +
        "  \"use\": \"sig\",\n" +
        "  \"kid\": \"test\",\n" +
        "  \"alg\": \"RS256\",\n" +
        "  \"n\": \"jeyrvOaZrmKWjyNXt0myAc_pJ1hNt3aRupExJEx1ewPaL9J9HFgSCjMrYxCB1ETO1NDyZ3nSgjZis-jHHDqBxBjRdq_t1E2rkGFaYbxAyKt220Pwgme_SFTB9MXVrFQGkKyjmQeVmOmV6zM3KK8uMdKQJ4aoKmwBcF5Zg7EZdDcKOFgpgva1Jq-FlEsaJ2xrYDYo3KnGcOHIt9_0NQeLsqZbeWYLxYni7uROFncXYV5FhSJCeR4A_rrbwlaCydGxE0ToC_9HNYibUHlkJjqyUhAgORCbNS8JLCJH8NUi5sDdIawK9GTSyvsJXZ-QHqo4cMUuxWV5AJtaRGghuMUfqQ\"\n" +
        "}";

    PublicJsonWebKey jwk = RsaJsonWebKey.Factory.newPublicJwk(jwkJSON);
    JwtClaims claims = JWTAuthPluginTest.generateClaims();
    JsonWebSignature jws = new JsonWebSignature();
    jws.setPayload(claims.toJson());
    jws.setKey(jwk.getPrivateKey());
    jws.setKeyIdHeaderValue(jwk.getKeyId());
    jws.setAlgorithmHeaderValue(AlgorithmIdentifiers.RSA_USING_SHA256);

    jwtTestToken = jws.getCompactSerialization();

    HttpClientUtil.removeRequestInterceptor(interceptor);
    HttpClientUtil.addRequestInterceptor(interceptor);
    
    cluster.waitForAllNodes(10);
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    System.clearProperty("java.security.auth.login.config");
    shutdownCluster();
  }

  @Before
  public void before() throws IOException, InterruptedException {
    jwtInterceptCount.set(0);
    pkiInterceptCount.set(0);
  }

  @Test(expected = IOException.class)
  public void infoRequestWithoutToken() throws Exception {
    get(baseUrl + "admin/info/system", null);
  }

  @Test
  public void infoRequestWithToken() throws IOException {
    Pair<String,Integer> result = get(baseUrl + "admin/info/system", jwtTestToken);
    assertEquals(Integer.valueOf(200), result.second());
    verifyInterRequestHeaderCounts(0,0);
  }

  @Test
  public void createCollectionUpdateAndQueryDistributed() throws Exception {
    // Admin request will use PKI inter-node auth from Overseer, and succeed
    assertEquals(200, get(baseUrl + "admin/collections?action=CREATE&name=" + COLLECTION + "&numShards=2", jwtTestToken).second().intValue());
    cluster.waitForActiveCollection(COLLECTION, 2, 2);
    
    // Now update three documents
    assertPkiAuthMetricsMinimums(12, 12, 0, 0, 0, 0);
    Pair<String,Integer> result = post(baseUrl + COLLECTION + "/update?commit=true", "[{\"id\" : \"1\"}, {\"id\": \"2\"}, {\"id\": \"3\"}]", jwtTestToken);
    assertEquals(Integer.valueOf(200), result.second());
    verifyInterRequestHeaderCounts(1,1);
    assertAuthMetricsMinimums(3, 0, 0, 0, 0, 0);
    assertPkiAuthMetricsMinimums(13, 13, 0, 0, 0, 0);
    
    // First a non distributed query
    result = get(baseUrl + COLLECTION + "/query?q=*:*&distrib=false", jwtTestToken);
    assertEquals(Integer.valueOf(200), result.second());
    verifyInterRequestHeaderCounts(1,1);
    assertAuthMetricsMinimums(4, 0, 0, 0, 0, 0);

    // Now do a distributed query, using JWTAUth for inter-node
    result = get(baseUrl + COLLECTION + "/query?q=*:*", jwtTestToken);
    assertEquals(Integer.valueOf(200), result.second());
    verifyInterRequestHeaderCounts(5,5);
    assertAuthMetricsMinimums(5, 0, 0, 0, 0, 0);
    
    // Delete
    assertEquals(200, get(baseUrl + "admin/collections?action=DELETE&name=" + COLLECTION, jwtTestToken).second().intValue());
    verifyInterRequestHeaderCounts(5,5);
    assertAuthMetricsMinimums(10, 0, 0, 0, 0, 0);
    assertPkiAuthMetricsMinimums(15, 15, 0, 0, 0, 0);
  }

  private void verifyInterRequestHeaderCounts(int jwt, int pki) {
    assertEquals(jwt, jwtInterceptCount.get());
    assertEquals(pki, jwtInterceptCount.get());
  }

  private Pair<String, Integer> get(String url, String token) throws IOException {
    URL createUrl = new URL(url);
    HttpURLConnection createConn = (HttpURLConnection) createUrl.openConnection();
    if (token != null)
      createConn.setRequestProperty("Authorization", "Bearer " + token);
    BufferedReader br2 = new BufferedReader(new InputStreamReader((InputStream) createConn.getContent(), StandardCharsets.UTF_8));
    String result = br2.lines().collect(Collectors.joining("\n"));
    int code = createConn.getResponseCode();
    createConn.disconnect();
    return new Pair<>(result, code);
  }

  private Pair<String, Integer> post(String url, String json, String token) throws IOException {
    URL createUrl = new URL(url);
    HttpURLConnection con = (HttpURLConnection) createUrl.openConnection();
    con.setRequestMethod("POST");
    con.setRequestProperty(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
    if (token != null)
      con.setRequestProperty("Authorization", "Bearer " + token);

    con.setDoOutput(true);
    OutputStream os = con.getOutputStream();
    os.write(json.getBytes(StandardCharsets.UTF_8));
    os.flush();
    os.close();

    con.connect();
    BufferedReader br2 = new BufferedReader(new InputStreamReader((InputStream) con.getContent(), StandardCharsets.UTF_8));
    String result = br2.lines().collect(Collectors.joining("\n"));
    int code = con.getResponseCode();
    con.disconnect();
    return new Pair<>(result, code);
  }

  private static class CountInterceptor implements HttpRequestInterceptor {
    @Override
    public void process(HttpRequest request, HttpContext context) throws HttpException, IOException {
      Header ah = request.getFirstHeader(HttpHeaders.AUTHORIZATION);
      if (ah != null && ah.getValue().startsWith("Bearer"))
        jwtInterceptCount.addAndGet(1);

      Header ph = request.getFirstHeader(PKIAuthenticationPlugin.HEADER);
      if (ph != null)
        pkiInterceptCount.addAndGet(1);
    }
  }
}
