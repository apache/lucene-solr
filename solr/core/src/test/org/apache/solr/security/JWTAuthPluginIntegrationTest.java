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

import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.message.BasicHeader;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.AbstractDistribZkTestBase;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.jose4j.jwk.PublicJsonWebKey;
import org.jose4j.jwk.RsaJsonWebKey;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwt.JwtClaims;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class JWTAuthPluginIntegrationTest extends SolrCloudTestCase {
  protected static final int NUM_SERVERS = 2;
  protected static final int NUM_SHARDS = 2;
  protected static final int REPLICATION_FACTOR = 1;
  private static Header tokenHeader;
  private static String jwkJSON = "{\n" +
      "  \"kty\": \"RSA\",\n" +
      "  \"d\": \"i6pyv2z3o-MlYytWsOr3IE1olu2RXZBzjPRBNgWAP1TlLNaphHEvH5aHhe_CtBAastgFFMuP29CFhaL3_tGczkvWJkSveZQN2AHWHgRShKgoSVMspkhOt3Ghha4CvpnZ9BnQzVHnaBnHDTTTfVgXz7P1ZNBhQY4URG61DKIF-JSSClyh1xKuMoJX0lILXDYGGcjVTZL_hci4IXPPTpOJHV51-pxuO7WU5M9252UYoiYyCJ56ai8N49aKIMsqhdGuO4aWUwsGIW4oQpjtce5eEojCprYl-9rDhTwLAFoBtjy6LvkqlR2Ae5dKZYpStljBjK8PJrBvWZjXAEMDdQ8PuQ\",\n" +
      "  \"e\": \"AQAB\",\n" +
      "  \"use\": \"sig\",\n" +
      "  \"kid\": \"test\",\n" +
      "  \"alg\": \"RS256\",\n" +
      "  \"n\": \"jeyrvOaZrmKWjyNXt0myAc_pJ1hNt3aRupExJEx1ewPaL9J9HFgSCjMrYxCB1ETO1NDyZ3nSgjZis-jHHDqBxBjRdq_t1E2rkGFaYbxAyKt220Pwgme_SFTB9MXVrFQGkKyjmQeVmOmV6zM3KK8uMdKQJ4aoKmwBcF5Zg7EZdDcKOFgpgva1Jq-FlEsaJ2xrYDYo3KnGcOHIt9_0NQeLsqZbeWYLxYni7uROFncXYV5FhSJCeR4A_rrbwlaCydGxE0ToC_9HNYibUHlkJjqyUhAgORCbNS8JLCJH8NUi5sDdIawK9GTSyvsJXZ-QHqo4cMUuxWV5AJtaRGghuMUfqQ\"\n" +
      "}";
  private static PublicJsonWebKey jwk;
  
  @BeforeClass
  public static void setupClass() throws Exception {
    configureCluster(NUM_SERVERS)// nodes
        .withSecurityJson(TEST_PATH().resolve("security").resolve("jwt_plugin_jwk_security.json"))
        .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
    
    jwk = RsaJsonWebKey.Factory.newPublicJwk(jwkJSON);
    JwtClaims claims = JWTAuthPluginTest.generateClaims();
    JsonWebSignature jws = new JsonWebSignature();
    jws.setPayload(claims.toJson());
    jws.setKey(jwk.getPrivateKey());
    jws.setKeyIdHeaderValue(jwk.getKeyId());
    jws.setAlgorithmHeaderValue(AlgorithmIdentifiers.RSA_USING_SHA256);

    String testJwt = jws.getCompactSerialization();
    tokenHeader = new BasicHeader(HttpHeaders.AUTHORIZATION, "Bearer " + testJwt);
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    System.clearProperty("java.security.auth.login.config");
  }

  @Test(expected = HttpSolrClient.RemoteSolrException.class)
  public void testWithoutToken() throws Exception {
    testCollectionCreateSearchDelete();
    // sometimes run a second test e.g. to test collection create-delete-create scenario
    if (random().nextBoolean()) testCollectionCreateSearchDelete();
  }

  @Test
  public void testWithToken() throws Exception {
    enableToken();
    testCollectionCreateSearchDelete();
    // sometimes run a second test e.g. to test collection create-delete-create scenario
    if (random().nextBoolean()) testCollectionCreateSearchDelete();
  }

  private void enableToken() {
    
  }

  protected void testCollectionCreateSearchDelete() throws Exception {
    CloudSolrClient solrClient = cluster.getSolrClient();
    String collectionName = "jwtAuthTestColl";

    // create collection
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName, "conf1",
        NUM_SHARDS, REPLICATION_FACTOR);
    create.process(solrClient);

    solrClient.add(collectionName, new SolrInputDocument("id", "1"));
    solrClient.add(collectionName, new SolrInputDocument("id", "2"));
    solrClient.commit(collectionName);

    SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    QueryResponse rsp = solrClient.query(collectionName, query);
    assertEquals(2, rsp.getResults().getNumFound());

    CollectionAdminRequest.Delete deleteReq = CollectionAdminRequest.deleteCollection(collectionName);
    deleteReq.process(solrClient);
    AbstractDistribZkTestBase.waitForCollectionToDisappear(collectionName,
        solrClient.getZkStateReader(), true, true, 330);
  }

}
