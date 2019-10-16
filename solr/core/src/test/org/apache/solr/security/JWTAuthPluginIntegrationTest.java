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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.cloud.SolrCloudAuthTestCase;
import org.apache.solr.common.util.Base64;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.TimeOut;
import org.jose4j.jwk.PublicJsonWebKey;
import org.jose4j.jwk.RsaJsonWebKey;
import org.jose4j.jwk.RsaJwkGenerator;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwt.JwtClaims;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Validate that JWT token authentication works in a real cluster.
 * <p> 
 * TODO: Test also using SolrJ as client. But that requires a way to set Authorization header on request, see SOLR-13070<br>
 *       This is also the reason we use {@link org.apache.solr.SolrTestCaseJ4.SuppressSSL} annotation, since we use HttpUrlConnection
 * </p>
 */
@SolrTestCaseJ4.SuppressSSL
public class JWTAuthPluginIntegrationTest extends SolrCloudAuthTestCase {
  protected static final int NUM_SERVERS = 2;
  protected static final int NUM_SHARDS = 2;
  protected static final int REPLICATION_FACTOR = 1;
  private final String COLLECTION = "jwtColl";
  private String jwtTestToken;
  private String baseUrl;
  private JsonWebSignature jws;
  private String jwtTokenWrongSignature;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    
    configureCluster(NUM_SERVERS)// nodes
        .withSecurityJson(TEST_PATH().resolve("security").resolve("jwt_plugin_jwk_security.json"))
        .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .withDefaultClusterProperty("useLegacyReplicaAssignment", "false")
        .configure();
    baseUrl = cluster.getRandomJetty(random()).getBaseUrl().toString();

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
    jws = new JsonWebSignature();
    jws.setPayload(claims.toJson());
    jws.setKey(jwk.getPrivateKey());
    jws.setKeyIdHeaderValue(jwk.getKeyId());
    jws.setAlgorithmHeaderValue(AlgorithmIdentifiers.RSA_USING_SHA256);

    jwtTestToken = jws.getCompactSerialization();
    
    PublicJsonWebKey jwk2 = RsaJwkGenerator.generateJwk(2048);
    jwk2.setKeyId("k2");
    JsonWebSignature jws2 = new JsonWebSignature();
    jws2.setPayload(claims.toJson());
    jws2.setKey(jwk2.getPrivateKey());
    jws2.setKeyIdHeaderValue(jwk2.getKeyId());
    jws2.setAlgorithmHeaderValue(AlgorithmIdentifiers.RSA_USING_SHA256);
    jwtTokenWrongSignature = jws2.getCompactSerialization();

    cluster.waitForAllNodes(10);
  }

  @Override
  @After
  public void tearDown() throws Exception {
    shutdownCluster();
    super.tearDown();
  }

  @Test(expected = IOException.class)
  public void infoRequestWithoutToken() throws Exception {
    get(baseUrl + "/admin/info/system", null);
  }

  @Test
  public void infoRequestValidateXSolrAuthHeaders() throws IOException {
    Map<String, String> headers = getHeaders(baseUrl + "/admin/info/system", null);
    assertEquals("401", headers.get("code"));
    assertEquals("HTTP/1.1 401 Require authentication", headers.get(null));
    assertEquals("Bearer realm=\"my-solr-jwt\"", headers.get("WWW-Authenticate"));
    String authData = new String(Base64.base64ToByteArray(headers.get("X-Solr-AuthData")), UTF_8);
    assertEquals("{\n" +
        "  \"scope\":\"solr:admin\",\n" +
        "  \"redirect_uris\":[],\n" +
        "  \"authorizationEndpoint\":\"http://acmepaymentscorp/oauth/auz/authorize\",\n" +
        "  \"client_id\":\"solr-cluster\"}", authData);
  }

  @Test
  public void testMetrics() throws Exception {
    boolean isUseV2Api = random().nextBoolean();
    String authcPrefix = "/admin/authentication";
    if(isUseV2Api){
      authcPrefix = "/____v2/cluster/security/authentication";
    }
    String baseUrl = cluster.getRandomJetty(random()).getBaseUrl().toString();
    CloseableHttpClient cl = HttpClientUtil.createClient(null);
    
    createCollection(COLLECTION);
    
    // Missing token
    getAndFail(baseUrl + "/" + COLLECTION + "/query?q=*:*", null);
    assertAuthMetricsMinimums(2, 1, 0, 0, 1, 0);
    executeCommand(baseUrl + authcPrefix, cl, "{set-property : { blockUnknown: false}}", jws);
    verifySecurityStatus(cl, baseUrl + authcPrefix, "authentication/blockUnknown", "false", 20, jws);
    // Pass through
    verifySecurityStatus(cl, baseUrl + "/admin/info/key", "key", NOT_NULL_PREDICATE, 20);
    // Now succeeds since blockUnknown=false 
    get(baseUrl + "/" + COLLECTION + "/query?q=*:*", null);
    executeCommand(baseUrl + authcPrefix, cl, "{set-property : { blockUnknown: true}}", null);
    verifySecurityStatus(cl, baseUrl + authcPrefix, "authentication/blockUnknown", "true", 20, jws);

    assertAuthMetricsMinimums(9, 4, 4, 0, 1, 0);
    
    // Wrong Credentials
    getAndFail(baseUrl + "/" + COLLECTION + "/query?q=*:*", jwtTokenWrongSignature);
    assertAuthMetricsMinimums(10, 4, 4, 1, 1, 0);

    // JWT parse error
    getAndFail(baseUrl + "/" + COLLECTION + "/query?q=*:*", "foozzz");
    assertAuthMetricsMinimums(11, 4, 4, 1, 1, 1);
    
    HttpClientUtil.close(cl);
  }

  @Test
  public void createCollectionUpdateAndQueryDistributed() throws Exception {
    // Admin request will use PKI inter-node auth from Overseer, and succeed
    createCollection(COLLECTION);
    
    // Now update three documents
    assertAuthMetricsMinimums(1, 1, 0, 0, 0, 0);
    assertPkiAuthMetricsMinimums(4, 4, 0, 0, 0, 0);
    Pair<String,Integer> result = post(baseUrl + "/" + COLLECTION + "/update?commit=true", "[{\"id\" : \"1\"}, {\"id\": \"2\"}, {\"id\": \"3\"}]", jwtTestToken);
    assertEquals(Integer.valueOf(200), result.second());
    assertAuthMetricsMinimums(4, 4, 0, 0, 0, 0);
    assertPkiAuthMetricsMinimums(4, 4, 0, 0, 0, 0);
    
    // First a non distributed query
    result = get(baseUrl + "/" + COLLECTION + "/query?q=*:*&distrib=false", jwtTestToken);
    assertEquals(Integer.valueOf(200), result.second());
    assertAuthMetricsMinimums(5, 5, 0, 0, 0, 0);

    // Now do a distributed query, using JWTAuth for inter-node
    result = get(baseUrl + "/" + COLLECTION + "/query?q=*:*", jwtTestToken);
    assertEquals(Integer.valueOf(200), result.second());
    assertAuthMetricsMinimums(10, 10, 0, 0, 0, 0);
    
    // Delete
    assertEquals(200, get(baseUrl + "/admin/collections?action=DELETE&name=" + COLLECTION, jwtTestToken).second().intValue());
    assertAuthMetricsMinimums(11, 11, 0, 0, 0, 0);
    assertPkiAuthMetricsMinimums(6, 6, 0, 0, 0, 0);
  }

  private void getAndFail(String url, String token) {
    try {
      get(url, token);
      fail("Request to " + url + " with token " + token + " should have failed");
    } catch(Exception e) { /* Fall through */ }
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

  private Map<String,String> getHeaders(String url, String token) throws IOException {
    URL createUrl = new URL(url);
    HttpURLConnection conn = (HttpURLConnection) createUrl.openConnection();
    if (token != null)
      conn.setRequestProperty("Authorization", "Bearer " + token);
    conn.connect();
    int code = conn.getResponseCode();
    Map<String, String> result = new HashMap<>();
    conn.getHeaderFields().forEach((k,v) -> result.put(k, v.get(0)));
    result.put("code", String.valueOf(code));
    conn.disconnect();
    return result;
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

  private void createCollection(String collectionName) throws IOException {
    assertEquals(200, get(baseUrl + "/admin/collections?action=CREATE&name=" + collectionName + "&numShards=2", jwtTestToken).second().intValue());
    cluster.waitForActiveCollection(collectionName, 2, 2);
  }

  private void executeCommand(String url, HttpClient cl, String payload, JsonWebSignature jws)
    throws Exception {
    
    // HACK: work around for SOLR-13464...
    //
    // note the authz/authn objects in use on each node before executing the command,
    // then wait until we see new objects on every node *after* executing the command
    // before returning...
    final Set<Map.Entry<String,Object>> initialPlugins
      = getAuthPluginsInUseForCluster(url).entrySet();
    
    HttpPost httpPost;
    HttpResponse r;
    httpPost = new HttpPost(url);
    if (jws != null)
      setAuthorizationHeader(httpPost, "Bearer " + jws.getCompactSerialization());
    httpPost.setEntity(new ByteArrayEntity(payload.getBytes(UTF_8)));
    httpPost.addHeader("Content-Type", "application/json; charset=UTF-8");
    r = cl.execute(httpPost);
    String response = IOUtils.toString(r.getEntity().getContent(), StandardCharsets.UTF_8);
    assertEquals("Non-200 response code. Response was " + response, 200, r.getStatusLine().getStatusCode());
    assertFalse("Response contained errors: " + response, response.contains("errorMessages"));
    Utils.consumeFully(r.getEntity());

    // HACK (continued)...
    final TimeOut timeout = new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    timeout.waitFor("core containers never fully updated their auth plugins",
                    () -> {
                      final Set<Map.Entry<String,Object>> tmpSet
                        = getAuthPluginsInUseForCluster(url).entrySet();
                      tmpSet.retainAll(initialPlugins);
                      return tmpSet.isEmpty();
                    });
    
  }
}
