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

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.Utils;
import org.apache.solr.security.JWTAuthPlugin.AuthenticationResponse;
import org.jose4j.jwk.RsaJsonWebKey;
import org.jose4j.jwk.RsaJwkGenerator;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.keys.BigEndianBigInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.solr.security.JWTAuthPlugin.AuthenticationResponse.AuthCode.AUTZ_HEADER_PROBLEM;
import static org.apache.solr.security.JWTAuthPlugin.AuthenticationResponse.AuthCode.NO_AUTZ_HEADER;

public class JWTAuthPluginTest extends SolrTestCaseJ4 {
  private static String testHeader;
  private static String slimJwt;
  private static String slimHeader;
  private JWTAuthPlugin plugin;
  private HashMap<String, Object> testJwk;
  private static RsaJsonWebKey rsaJsonWebKey;
  private static String testJwt;
  private HashMap<String, Object> testConfig;

  
  @BeforeClass
  public static void beforeAll() throws Exception {
    // Generate an RSA key pair, which will be used for signing and verification of the JWT, wrapped in a JWK
    rsaJsonWebKey = RsaJwkGenerator.generateJwk(2048);
    rsaJsonWebKey.setKeyId("k1");

    JwtClaims claims = generateClaims();
    JsonWebSignature jws = new JsonWebSignature();
    jws.setPayload(claims.toJson());
    jws.setKey(rsaJsonWebKey.getPrivateKey());
    jws.setKeyIdHeaderValue(rsaJsonWebKey.getKeyId());
    jws.setAlgorithmHeaderValue(AlgorithmIdentifiers.RSA_USING_SHA256);

    testJwt = jws.getCompactSerialization();
    testHeader = "Bearer" + " " + testJwt;

    System.out.println("Header:\n" + testHeader);
    System.out.println("JWK:\n" + rsaJsonWebKey.toJson());
    
    claims.unsetClaim("iss");
    claims.unsetClaim("aud");
    claims.unsetClaim("exp");
    jws.setPayload(claims.toJson());
    slimJwt = jws.getCompactSerialization();
    slimHeader = "Bearer" + " " + slimJwt;
  }

  protected static JwtClaims generateClaims() {
    JwtClaims claims = new JwtClaims();
    claims.setIssuer("IDServer");  // who creates the token and signs it
    claims.setAudience("Solr"); // to whom the token is intended to be sent
    claims.setExpirationTimeMinutesInTheFuture(10); // time when the token will expire (10 minutes from now)
    claims.setGeneratedJwtId(); // a unique identifier for the token
    claims.setIssuedAtToNow();  // when the token was issued/created (now)
    claims.setNotBeforeMinutesInThePast(2); // time before which the token is not yet valid (2 minutes ago)
    claims.setSubject("solruser"); // the subject/principal is whom the token is about
    claims.setClaim("name", "Solr User"); // additional claims/attributes about the subject can be added
    claims.setClaim("customPrincipal", "custom"); // additional claims/attributes about the subject can be added
    claims.setClaim("claim1", "foo"); // additional claims/attributes about the subject can be added
    claims.setClaim("claim2", "bar"); // additional claims/attributes about the subject can be added
    claims.setClaim("claim3", "foo"); // additional claims/attributes about the subject can be added
    List<String> groups = Arrays.asList("group-one", "other-group", "group-three");
    claims.setStringListClaim("groups", groups); // multi-valued claims work too and will end up as a JSON array
    return claims;
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    // Create an auth plugin
    plugin = new JWTAuthPlugin();

    // Create a JWK config for security.json
    testJwk = new HashMap<>();
    testJwk.put("kty", rsaJsonWebKey.getKeyType());
    testJwk.put("e", BigEndianBigInteger.toBase64Url(rsaJsonWebKey.getRsaPublicKey().getPublicExponent()));
    testJwk.put("use", rsaJsonWebKey.getUse());
    testJwk.put("kid", rsaJsonWebKey.getKeyId());
    testJwk.put("alg", rsaJsonWebKey.getAlgorithm());
    testJwk.put("n", BigEndianBigInteger.toBase64Url(rsaJsonWebKey.getRsaPublicKey().getModulus()));

    testConfig = new HashMap<>();
    testConfig.put("class", "org.apache.solr.security.JWTAuthPlugin");
    testConfig.put("jwk", testJwk);
    plugin.init(testConfig);
  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    plugin.close();
  }

  @Test
  public void initWithoutRequired() throws Exception {
    HashMap<String, Object> authConf = new HashMap<String, Object>();
    plugin = new JWTAuthPlugin();
    plugin.init(authConf);
    assertEquals(AUTZ_HEADER_PROBLEM, plugin.authenticate("foo").getAuthCode());
  }

  @Test
  public void initFromSecurityJSONLocalJWK() throws Exception {
    Path securityJson = TEST_PATH().resolve("security").resolve("jwt_plugin_jwk_security.json");
    InputStream is = Files.newInputStream(securityJson);
    Map<String,Object> securityConf = (Map<String, Object>) Utils.fromJSON(is);
    Map<String, Object> authConf = (Map<String, Object>) securityConf.get("authentication");
    plugin.init(authConf);
  }

  @Test
  public void initFromSecurityJSONUrlJwk() throws Exception {
    Path securityJson = TEST_PATH().resolve("security").resolve("jwt_plugin_jwk_url_security.json");
    InputStream is = Files.newInputStream(securityJson);
    Map<String,Object> securityConf = (Map<String, Object>) Utils.fromJSON(is);
    Map<String, Object> authConf = (Map<String, Object>) securityConf.get("authentication");
    plugin.init(authConf);

    AuthenticationResponse resp = plugin.authenticate(testHeader);
    assertEquals(AuthenticationResponse.AuthCode.JWT_VALIDATION_EXCEPTION, resp.getAuthCode());
    assertTrue(resp.getJwtException().getMessage().contains("Connection refused"));
  }

  @Test
  public void initWithJwk() throws Exception {
    HashMap<String, Object> authConf = new HashMap<String, Object>();
    authConf.put("jwk", testJwk);
    plugin = new JWTAuthPlugin();
    plugin.init(authConf);
  }

  @Test
  public void initWithJwkUrl() throws Exception {
    HashMap<String, Object> authConf = new HashMap<String, Object>();
    authConf.put("jwk_url", "https://127.0.0.1:9999/foo.jwk");
    plugin = new JWTAuthPlugin();
    plugin.init(authConf);
  }

  @Test
  public void parseJwkSet() throws Exception {
    plugin.parseJwkSet(testJwk);

    HashMap<String, Object> testJwks = new HashMap<>();
    List<Map<String, Object>> keys = new ArrayList<>();
    keys.add(testJwk);
    testJwks.put("keys", keys);
    plugin.parseJwkSet(testJwks);
  }

  @Test
  public void authenticateOk() throws Exception {
    AuthenticationResponse resp = plugin.authenticate(testHeader);
    assertTrue(resp.isAuthenticated());
    assertEquals("solruser", resp.getPrincipal().getName());
  }

  @Test
  public void authFailedMissingSubject() throws Exception {
    testConfig.put("iss", "NA");
    plugin.init(testConfig);
    AuthenticationResponse resp = plugin.authenticate(testHeader);
    assertFalse(resp.isAuthenticated());
    assertEquals(AuthenticationResponse.AuthCode.JWT_VALIDATION_EXCEPTION, resp.getAuthCode());

    testConfig.put("iss", "IDServer");
    plugin.init(testConfig);
    resp = plugin.authenticate(testHeader);
    assertTrue(resp.isAuthenticated());
  }

  @Test
  public void authFailedMissingAudience() throws Exception {
    testConfig.put("aud", "NA");
    plugin.init(testConfig);
    AuthenticationResponse resp = plugin.authenticate(testHeader);
    assertFalse(resp.isAuthenticated());
    assertEquals(AuthenticationResponse.AuthCode.JWT_VALIDATION_EXCEPTION, resp.getAuthCode());

    testConfig.put("aud", "Solr");
    plugin.init(testConfig);
    resp = plugin.authenticate(testHeader);
    assertTrue(resp.isAuthenticated());
  }

  @Test
  public void authFailedMissingPrincipal() throws Exception {
    testConfig.put("principal_claim", "customPrincipal");
    plugin.init(testConfig);
    AuthenticationResponse resp = plugin.authenticate(testHeader);
    assertTrue(resp.isAuthenticated());

    testConfig.put("principal_claim", "NA");
    plugin.init(testConfig);
    resp = plugin.authenticate(testHeader);
    assertFalse(resp.isAuthenticated());
    assertEquals(AuthenticationResponse.AuthCode.PRINCIPAL_MISSING, resp.getAuthCode());
  }

  @Test
  public void claimMatch() throws Exception {
    // all custom claims match regex
    Map<String, String> shouldMatch = new HashMap<>();
    shouldMatch.put("claim1", "foo");
    shouldMatch.put("claim2", "foo|bar");
    shouldMatch.put("claim3", "f\\w{2}$");
    testConfig.put("claims_match", shouldMatch);
    plugin.init(testConfig);
    AuthenticationResponse resp = plugin.authenticate(testHeader);
    assertTrue(resp.isAuthenticated());

    // Required claim does not exist
    shouldMatch.clear();
    shouldMatch.put("claim9", "NA");
    plugin.init(testConfig);
    resp = plugin.authenticate(testHeader);
    assertEquals(AuthenticationResponse.AuthCode.CLAIM_MISMATCH, resp.getAuthCode());

    // Required claim does not match regex
    shouldMatch.clear();
    shouldMatch.put("claim1", "NA");
    resp = plugin.authenticate(testHeader);
    assertEquals(AuthenticationResponse.AuthCode.CLAIM_MISMATCH, resp.getAuthCode());
  }

  @Test
  public void missingIssAudExp() throws Exception {
    testConfig.put("require_exp", "false");
    testConfig.put("require_sub", "false");
    plugin.init(testConfig);
    AuthenticationResponse resp = plugin.authenticate(slimHeader);
    assertTrue(resp.isAuthenticated());

    // Missing exp header
    testConfig.put("require_exp", true);
    plugin.init(testConfig);
    resp = plugin.authenticate(slimHeader);
    assertEquals(AuthenticationResponse.AuthCode.JWT_VALIDATION_EXCEPTION, resp.getAuthCode());

    // Missing sub header
    testConfig.put("require_sub", true);
    plugin.init(testConfig);
    resp = plugin.authenticate(slimHeader);
    assertEquals(AuthenticationResponse.AuthCode.JWT_VALIDATION_EXCEPTION, resp.getAuthCode());
  }

  @Test
  public void algWhitelist() throws Exception {
    testConfig.put("alg_whitelist", Arrays.asList("PS384", "PS512"));
    plugin.init(testConfig);
    AuthenticationResponse resp = plugin.authenticate(testHeader);
    assertEquals(AuthenticationResponse.AuthCode.JWT_VALIDATION_EXCEPTION, resp.getAuthCode());
    assertTrue(resp.getErrorMessage().contains("not a whitelisted"));
  }

  @Test
  public void roles() throws Exception {
    testConfig.put("roles_claim", "groups");
    plugin.init(testConfig);
    AuthenticationResponse resp = plugin.authenticate(testHeader);
    assertTrue(resp.isAuthenticated());

    Principal principal = resp.getPrincipal();
    assertTrue(principal instanceof VerifiedUserRoles);
    Set<String> roles = ((VerifiedUserRoles)principal).getVerifiedRoles();
    assertEquals(3, roles.size());
    assertTrue(roles.contains("group-one"));

    // Wrong claim ID
    testConfig.put("roles_claim", "NA");
    plugin.init(testConfig);
    resp = plugin.authenticate(testHeader);
    assertEquals(AuthenticationResponse.AuthCode.CLAIM_MISMATCH, resp.getAuthCode());
  }

  @Test
  public void noHeaderBlockUnknown() throws Exception {
    testConfig.put("block_unknown", true);
    plugin.init(testConfig);
    AuthenticationResponse resp = plugin.authenticate(null);
    assertEquals(NO_AUTZ_HEADER, resp.getAuthCode());
  }

  @Test
  public void noHeaderNotBlockUnknown() throws Exception {
    testConfig.put("block_unknown", false);
    plugin.init(testConfig);
    AuthenticationResponse resp = plugin.authenticate(null);
    assertEquals(AuthenticationResponse.AuthCode.PASS_THROUGH, resp.getAuthCode());
  }

  // TODO: Add inter-node request tests
}