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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.solr.common.SolrException;
import org.jose4j.jwk.JsonWebKeySet;
import org.junit.Before;
import org.junit.Test;
import org.noggit.JSONUtil;

import static org.apache.solr.SolrTestCaseJ4.TEST_PATH;
import static org.apache.solr.security.JWTAuthPluginTest.testJwk;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JWTIssuerConfigTest {
  private JWTIssuerConfig testIssuer;
  private Map<String, Object> testIssuerConfigMap;
  private String testIssuerJson;

  @Before
  public void setUp() throws Exception {
    testIssuer = new JWTIssuerConfig("name")
        .setJwksUrl("https://issuer/path")
        .setIss("issuer")
        .setAud("audience")
        .setClientId("clientid")
        .setWellKnownUrl("wellknown")
        .setAuthorizationEndpoint("https://issuer/authz");

    testIssuerConfigMap = testIssuer.asConfig();

    testIssuerJson = "{\n" +
        "  \"aud\":\"audience\",\n" +
        "  \"wellKnownUrl\":\"wellknown\",\n" +
        "  \"clientId\":\"clientid\",\n" +
        "  \"jwksUrl\":[\"https://issuer/path\"],\n" +
        "  \"name\":\"name\",\n" +
        "  \"iss\":\"issuer\",\n" +
        "  \"authorizationEndpoint\":\"https://issuer/authz\"}";
  }

  @Test
  public void parseConfigMap() {
    // Do a round-trip from map -> object -> map -> json
    JWTIssuerConfig issuerConfig = new JWTIssuerConfig(testIssuerConfigMap);
    issuerConfig.isValid();
    assertEquals(testIssuerJson, JSONUtil.toJSON(issuerConfig.asConfig()));
  }

  @Test(expected = SolrException.class)
  public void parseConfigMapNoName() {
    testIssuerConfigMap.remove("name"); // Will fail validation
    new JWTIssuerConfig(testIssuerConfigMap).isValid();
  }

  @Test
  public void parseJwkSet() throws Exception {
    HashMap<String, Object> testJwks = new HashMap<>();
    List<Map<String, Object>> keys = new ArrayList<>();
    keys.add(testJwk);
    testJwks.put("keys", keys);
    JWTIssuerConfig.parseJwkSet(testJwks);
  }

  @Test
  public void setJwksUrl() {
    JWTIssuerConfig conf = new JWTIssuerConfig("myConf");
    conf.setJwksUrl("http://server/path");
  }

  @Test
  public void asConfig() {
    assertEquals(testIssuerJson, JSONUtil.toJSON(testIssuer.asConfig()));
  }

  @Test
  public void isValid() {
    assertTrue(testIssuer.isValid());
  }

  @Test(expected = SolrException.class)
  public void notValidBothJwksAndJwk() {
    testIssuer.setJsonWebKeySet(new JsonWebKeySet());
    testIssuer.isValid();
  }

  @Test
  public void parseIssuerConfigExplicit() {
    HashMap<String, Object> issuerConfigMap = new HashMap<>();
    issuerConfigMap.put("name", "myName");
    issuerConfigMap.put("iss", "myIss");
    issuerConfigMap.put("jwksUrl", "https://host/jwk");

    JWTIssuerConfig issuerConfig = new JWTIssuerConfig(issuerConfigMap);
    assertEquals("myIss", issuerConfig.getIss());
    assertEquals("myName", issuerConfig.getName());
    assertEquals(1, issuerConfig.getJwksUrls().size());
    assertEquals("https://host/jwk", issuerConfig.getJwksUrls().get(0));
  }

  @Test
  public void wellKnownConfigFromInputstream() throws IOException {
    Path configJson = TEST_PATH().resolve("security").resolve("jwt_well-known-config.json");
    JWTIssuerConfig.WellKnownDiscoveryConfig config = JWTIssuerConfig.WellKnownDiscoveryConfig.parse(Files.newInputStream(configJson));
    assertEquals("https://acmepaymentscorp/oauth/jwks", config.getJwksUrl());
  }

  @Test
  public void wellKnownConfigFromString() throws IOException {
    Path configJson = TEST_PATH().resolve("security").resolve("jwt_well-known-config.json");
    String configString = StringUtils.join(Files.readAllLines(configJson), "\n");
    JWTIssuerConfig.WellKnownDiscoveryConfig config = JWTIssuerConfig.WellKnownDiscoveryConfig.parse(configString, StandardCharsets.UTF_8);
    assertEquals("https://acmepaymentscorp/oauth/jwks", config.getJwksUrl());
    assertEquals("http://acmepaymentscorp", config.getIssuer());
    assertEquals("http://acmepaymentscorp/oauth/auz/authorize", config.getAuthorizationEndpoint());
    assertEquals(Arrays.asList("READ", "WRITE", "DELETE", "openid", "scope", "profile", "email", "address", "phone"), config.getScopesSupported());
    assertEquals(Arrays.asList("code", "code id_token", "code token", "code id_token token", "token", "id_token", "id_token token"), config.getResponseTypesSupported());
  }

  @Test(expected = SolrException.class)
  public void wellKnownConfigNotHttps() {
    JWTIssuerConfig.WellKnownDiscoveryConfig.parse("http://127.0.0.1:45678/.well-known/config");
  }

  @Test(expected = SolrException.class)
  public void wellKnownConfigNotReachable() {
    JWTIssuerConfig.WellKnownDiscoveryConfig.parse("https://127.0.0.1:45678/.well-known/config");
  }
}