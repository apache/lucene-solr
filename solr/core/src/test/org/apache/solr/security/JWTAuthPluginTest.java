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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Principal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.Base64;
import org.apache.solr.common.util.Utils;
import org.jose4j.jwk.RsaJsonWebKey;
import org.jose4j.jwk.RsaJwkGenerator;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.keys.BigEndianBigInteger;
import org.jose4j.lang.JoseException;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.solr.security.JWTAuthPlugin.JWTAuthenticationResponse.AuthCode.AUTZ_HEADER_PROBLEM;
import static org.apache.solr.security.JWTAuthPlugin.JWTAuthenticationResponse.AuthCode.NO_AUTZ_HEADER;
import static org.apache.solr.security.JWTAuthPlugin.JWTAuthenticationResponse.AuthCode.SCOPE_MISSING;

@SuppressWarnings("unchecked")
public class JWTAuthPluginTest extends SolrTestCaseJ4 {
  private static String testHeader;
  private static String slimHeader;
  private JWTAuthPlugin plugin;
  private static RsaJsonWebKey rsaJsonWebKey;
  private HashMap<String, Object> testConfig;
  private HashMap<String, Object> minimalConfig;

  // Shared with other tests
  static HashMap<String, Object> testJwk;

  static {
    // Generate an RSA key pair, which will be used for signing and verification of the JWT, wrapped in a JWK
    try {
      rsaJsonWebKey = RsaJwkGenerator.generateJwk(2048);
      rsaJsonWebKey.setKeyId("k1");

      testJwk = new HashMap<>();
      testJwk.put("kty", rsaJsonWebKey.getKeyType());
      testJwk.put("e", BigEndianBigInteger.toBase64Url(rsaJsonWebKey.getRsaPublicKey().getPublicExponent()));
      testJwk.put("use", rsaJsonWebKey.getUse());
      testJwk.put("kid", rsaJsonWebKey.getKeyId());
      testJwk.put("alg", rsaJsonWebKey.getAlgorithm());
      testJwk.put("n", BigEndianBigInteger.toBase64Url(rsaJsonWebKey.getRsaPublicKey().getModulus()));
    } catch (JoseException e) {
      fail("Failed static initialization: " + e.getMessage());
    }
  }

  @BeforeClass
  public static void beforeAll() throws Exception {
    JwtClaims claims = generateClaims();
    JsonWebSignature jws = new JsonWebSignature();
    jws.setPayload(claims.toJson());
    jws.setKey(rsaJsonWebKey.getPrivateKey());
    jws.setKeyIdHeaderValue(rsaJsonWebKey.getKeyId());
    jws.setAlgorithmHeaderValue(AlgorithmIdentifiers.RSA_USING_SHA256);

    String testJwt = jws.getCompactSerialization();
    testHeader = "Bearer" + " " + testJwt;

    claims.unsetClaim("iss");
    claims.unsetClaim("aud");
    claims.unsetClaim("exp");
    claims.setSubject(null);
    jws.setPayload(claims.toJson());
    String slimJwt = jws.getCompactSerialization();
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
    claims.setStringClaim("scope", "solr:read"); 
    claims.setClaim("name", "Solr User"); // additional claims/attributes about the subject can be added
    claims.setClaim("customPrincipal", "custom"); // additional claims/attributes about the subject can be added
    claims.setClaim("claim1", "foo"); // additional claims/attributes about the subject can be added
    claims.setClaim("claim2", "bar"); // additional claims/attributes about the subject can be added
    claims.setClaim("claim3", "foo"); // additional claims/attributes about the subject can be added
    List<String> roles = Arrays.asList("group-one", "other-group", "group-three");
    claims.setStringListClaim("roles", roles); // multi-valued claims work too and will end up as a JSON array
    return claims;
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();

    // Create an auth plugin
    plugin = new JWTAuthPlugin();

    testConfig = new HashMap<>();
    testConfig.put("class", "org.apache.solr.security.JWTAuthPlugin");
    testConfig.put("principalClaim", "customPrincipal");
    testConfig.put("jwk", testJwk);
    plugin.init(testConfig);
    
    minimalConfig = new HashMap<>();
    minimalConfig.put("class", "org.apache.solr.security.JWTAuthPlugin");
  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    if (null != plugin) {
      plugin.close();
      plugin = null;
    }
  }

  @Test
  public void initWithoutRequired() {
    plugin.init(testConfig);
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

    JWTAuthPlugin.JWTAuthenticationResponse resp = plugin.authenticate(testHeader);
    assertEquals(JWTAuthPlugin.JWTAuthenticationResponse.AuthCode.JWT_VALIDATION_EXCEPTION, resp.getAuthCode());
    assertTrue(resp.getJwtException().getMessage().contains("Connection refused"));
  }

  @Test
  public void initWithJwk() {
    HashMap<String, Object> authConf = new HashMap<>();
    authConf.put("jwk", testJwk);
    plugin = new JWTAuthPlugin();
    plugin.init(authConf);
  }

  @Test
  @Deprecated
  public void initWithJwkUrlForBackwardsCompat() {
    HashMap<String, Object> authConf = new HashMap<>();
    authConf.put("jwkUrl", "https://127.0.0.1:9999/foo.jwk");
    plugin = new JWTAuthPlugin();
    plugin.init(authConf);
    assertEquals(1, plugin.getIssuerConfigs().size());
    assertEquals(1, plugin.getIssuerConfigs().get(0).getJwksUrls().size());
  }

  @Test
  public void initWithJwksUrl() {
    HashMap<String, Object> authConf = new HashMap<>();
    authConf.put("jwksUrl", "https://127.0.0.1:9999/foo.jwk");
    plugin = new JWTAuthPlugin();
    plugin.init(authConf);
    assertEquals(1, plugin.getIssuerConfigs().size());
    assertEquals(1, plugin.getIssuerConfigs().get(0).getJwksUrls().size());
  }

  @Test
  public void initWithJwkUrlArray() {
    HashMap<String, Object> authConf = new HashMap<>();
    authConf.put("jwksUrl", Arrays.asList("https://127.0.0.1:9999/foo.jwk", "https://127.0.0.1:9999/foo2.jwk"));
    authConf.put("iss", "myIssuer");
    plugin = new JWTAuthPlugin();
    plugin.init(authConf);
    assertEquals(1, plugin.getIssuerConfigs().size());
    assertEquals(2, plugin.getIssuerConfigs().get(0).getJwksUrls().size());
  }

  @Test
  public void authenticateOk() {
    JWTAuthPlugin.JWTAuthenticationResponse resp = plugin.authenticate(testHeader);
    assertTrue(resp.isAuthenticated());
    assertEquals("custom", resp.getPrincipal().getName()); // principalClaim = customPrincipal, not sub here
  }

  @Test
  public void authFailedMissingSubject() {
    minimalConfig.put("principalClaim","sub");  // minimalConfig has no subject specified
    plugin.init(minimalConfig);
    JWTAuthPlugin.JWTAuthenticationResponse resp = plugin.authenticate(testHeader);
    assertFalse(resp.isAuthenticated());
    assertEquals(JWTAuthPlugin.JWTAuthenticationResponse.AuthCode.JWT_VALIDATION_EXCEPTION, resp.getAuthCode());

    testConfig.put("principalClaim","sub");  // testConfig has subject = solruser
    plugin.init(testConfig);
    resp = plugin.authenticate(testHeader);
    assertTrue(resp.isAuthenticated());
  }

  @Test
  public void authFailedMissingIssuer() {
    testConfig.put("iss", "NA");
    plugin.init(testConfig);
    JWTAuthPlugin.JWTAuthenticationResponse resp = plugin.authenticate(testHeader);
    assertFalse(resp.isAuthenticated());
    assertEquals(JWTAuthPlugin.JWTAuthenticationResponse.AuthCode.JWT_VALIDATION_EXCEPTION, resp.getAuthCode());

    testConfig.put("iss", "IDServer");
    plugin.init(testConfig);
    resp = plugin.authenticate(testHeader);
    assertTrue(resp.isAuthenticated());
  }

  @Test
  public void authFailedMissingAudience() {
    testConfig.put("aud", "NA");
    plugin.init(testConfig);
    JWTAuthPlugin.JWTAuthenticationResponse resp = plugin.authenticate(testHeader);
    assertFalse(resp.isAuthenticated());
    assertEquals(JWTAuthPlugin.JWTAuthenticationResponse.AuthCode.JWT_VALIDATION_EXCEPTION, resp.getAuthCode());

    testConfig.put("aud", "Solr");
    plugin.init(testConfig);
    resp = plugin.authenticate(testHeader);
    assertTrue(resp.isAuthenticated());
  }

  @Test
  public void authFailedMissingPrincipal() {
    testConfig.put("principalClaim", "customPrincipal");
    plugin.init(testConfig);
    JWTAuthPlugin.JWTAuthenticationResponse resp = plugin.authenticate(testHeader);
    assertTrue(resp.isAuthenticated());

    testConfig.put("principalClaim", "NA");
    plugin.init(testConfig);
    resp = plugin.authenticate(testHeader);
    assertFalse(resp.isAuthenticated());
    assertEquals(JWTAuthPlugin.JWTAuthenticationResponse.AuthCode.PRINCIPAL_MISSING, resp.getAuthCode());
  }

  @Test
  public void claimMatch() {
    // all custom claims match regex
    Map<String, String> shouldMatch = new HashMap<>();
    shouldMatch.put("claim1", "foo");
    shouldMatch.put("claim2", "foo|bar");
    shouldMatch.put("claim3", "f\\w{2}$");
    testConfig.put("claimsMatch", shouldMatch);
    plugin.init(testConfig);
    JWTAuthPlugin.JWTAuthenticationResponse resp = plugin.authenticate(testHeader);
    assertTrue(resp.isAuthenticated());

    // Required claim does not exist
    shouldMatch.clear();
    shouldMatch.put("claim9", "NA");
    plugin.init(testConfig);
    resp = plugin.authenticate(testHeader);
    assertEquals(JWTAuthPlugin.JWTAuthenticationResponse.AuthCode.CLAIM_MISMATCH, resp.getAuthCode());

    // Required claim does not match regex
    shouldMatch.clear();
    shouldMatch.put("claim1", "NA");
    resp = plugin.authenticate(testHeader);
    assertEquals(JWTAuthPlugin.JWTAuthenticationResponse.AuthCode.CLAIM_MISMATCH, resp.getAuthCode());
  }

  @Test
  public void missingIssAudExp() {
    testConfig.put("requireIss", "false");
    testConfig.put("requireExp", "false");
    plugin.init(testConfig);
    JWTAuthPlugin.JWTAuthenticationResponse resp = plugin.authenticate(slimHeader);
    assertTrue(resp.getErrorMessage(), resp.isAuthenticated());

    // Missing exp claim
    testConfig.put("requireExp", true);
    plugin.init(testConfig);
    resp = plugin.authenticate(slimHeader);
    assertEquals(JWTAuthPlugin.JWTAuthenticationResponse.AuthCode.JWT_VALIDATION_EXCEPTION, resp.getAuthCode());
    testConfig.put("requireExp", false);

    // Missing issuer claim
    testConfig.put("requireIss", true);
    plugin.init(testConfig);
    resp = plugin.authenticate(slimHeader);
    assertEquals(JWTAuthPlugin.JWTAuthenticationResponse.AuthCode.JWT_VALIDATION_EXCEPTION, resp.getAuthCode());
  }

  @Test
  public void algWhitelist() {
    testConfig.put("algWhitelist", Arrays.asList("PS384", "PS512"));
    plugin.init(testConfig);
    JWTAuthPlugin.JWTAuthenticationResponse resp = plugin.authenticate(testHeader);
    assertEquals(JWTAuthPlugin.JWTAuthenticationResponse.AuthCode.JWT_VALIDATION_EXCEPTION, resp.getAuthCode());
    assertTrue(resp.getErrorMessage().contains("not a whitelisted"));
  }

  @Test
  public void scope() {
    testConfig.put("scope", "solr:read solr:admin");
    plugin.init(testConfig);
    JWTAuthPlugin.JWTAuthenticationResponse resp = plugin.authenticate(testHeader);
    assertTrue(resp.getErrorMessage(), resp.isAuthenticated());

    // When 'rolesClaim' is not defined in config, then all scopes are registered as roles
    Principal principal = resp.getPrincipal();
    assertTrue(principal instanceof VerifiedUserRoles);
    Set<String> roles = ((VerifiedUserRoles)principal).getVerifiedRoles();
    assertEquals(1, roles.size());
    assertTrue(roles.contains("solr:read"));
  }

  @Test
  public void roles() {
    testConfig.put("rolesClaim", "roles");
    plugin.init(testConfig);
    JWTAuthPlugin.JWTAuthenticationResponse resp = plugin.authenticate(testHeader);
    assertTrue(resp.getErrorMessage(), resp.isAuthenticated());

    // When 'rolesClaim' is defined in config, then roles from that claim are used instead of claims
    Principal principal = resp.getPrincipal();
    assertTrue(principal instanceof VerifiedUserRoles);
    Set<String> roles = ((VerifiedUserRoles)principal).getVerifiedRoles();
    assertEquals(3, roles.size());
    assertTrue(roles.contains("group-one"));
    assertTrue(roles.contains("other-group"));
    assertTrue(roles.contains("group-three"));
  }

  @Test
  public void wrongScope() {
    testConfig.put("scope", "wrong");
    plugin.init(testConfig);
    JWTAuthPlugin.JWTAuthenticationResponse resp = plugin.authenticate(testHeader);
    assertFalse(resp.isAuthenticated());
    assertNull(resp.getPrincipal());
    assertEquals(SCOPE_MISSING, resp.getAuthCode());
  }
  
  @Test
  public void noHeaderBlockUnknown() {
    testConfig.put("blockUnknown", true);
    plugin.init(testConfig);
    JWTAuthPlugin.JWTAuthenticationResponse resp = plugin.authenticate(null);
    assertEquals(NO_AUTZ_HEADER, resp.getAuthCode());
  }

  @Test
  public void noHeaderNotBlockUnknown() {
    testConfig.put("blockUnknown", false);
    plugin.init(testConfig);
    JWTAuthPlugin.JWTAuthenticationResponse resp = plugin.authenticate(null);
    assertEquals(JWTAuthPlugin.JWTAuthenticationResponse.AuthCode.PASS_THROUGH, resp.getAuthCode());
  }

  @Test
  public void minimalConfigPassThrough() {
    minimalConfig.put("blockUnknown", false);
    plugin.init(minimalConfig);
    JWTAuthPlugin.JWTAuthenticationResponse resp = plugin.authenticate(null);
    assertEquals(JWTAuthPlugin.JWTAuthenticationResponse.AuthCode.PASS_THROUGH, resp.getAuthCode());
  }
  
  @Test
  public void wellKnownConfigNoHeaderPassThrough() {
    String wellKnownUrl = TEST_PATH().resolve("security").resolve("jwt_well-known-config.json").toAbsolutePath().toUri().toString();
    testConfig.put("wellKnownUrl", wellKnownUrl);
    testConfig.remove("jwk");
    plugin.init(testConfig);
    JWTAuthPlugin.JWTAuthenticationResponse resp = plugin.authenticate(null);
    assertEquals(JWTAuthPlugin.JWTAuthenticationResponse.AuthCode.PASS_THROUGH, resp.getAuthCode());
  }

  @Test
  public void defaultRealm() {
    String wellKnownUrl = TEST_PATH().resolve("security").resolve("jwt_well-known-config.json").toAbsolutePath().toUri().toString();
    testConfig.put("wellKnownUrl", wellKnownUrl);
    testConfig.remove("jwk");
    plugin.init(testConfig);
    assertEquals("solr-jwt", plugin.realm);
  }

  @Test
  public void configureRealm() {
    String wellKnownUrl = TEST_PATH().resolve("security").resolve("jwt_well-known-config.json").toAbsolutePath().toUri().toString();
    testConfig.put("wellKnownUrl", wellKnownUrl);
    testConfig.remove("jwk");
    testConfig.put("realm", "myRealm");
    plugin.init(testConfig);
    assertEquals("myRealm", plugin.realm);
  }

  @Test(expected = SolrException.class)
  public void bothJwksUrlAndJwkFails() {
    testConfig.put("jwksUrl", "http://127.0.0.1:45678/myJwk");
    plugin.init(testConfig);
  }

  @Test
  public void xSolrAuthDataHeader() {
    testConfig.put("adminUiScope", "solr:admin");
    testConfig.put("authorizationEndpoint", "http://acmepaymentscorp/oauth/auz/authorize");
    testConfig.put("clientId", "solr-cluster");
    plugin.init(testConfig);
    String headerBase64 = plugin.generateAuthDataHeader();
    String headerJson = new String(Base64.base64ToByteArray(headerBase64), StandardCharsets.UTF_8);
    Map<String,String> parsed = (Map<String, String>) Utils.fromJSONString(headerJson);
    assertEquals("solr:admin", parsed.get("scope"));
    assertEquals("http://acmepaymentscorp/oauth/auz/authorize", parsed.get("authorizationEndpoint"));
    assertEquals("solr-cluster", parsed.get("client_id"));
  }

  @Test
  public void initWithTwoIssuers() {
    HashMap<String, Object> authConf = new HashMap<>();
    JWTIssuerConfig iss1 = new JWTIssuerConfig("iss1").setIss("1").setAud("aud1")
        .setJwksUrl("https://127.0.0.1:9999/foo.jwk");
    JWTIssuerConfig iss2 = new JWTIssuerConfig("iss2").setIss("2").setAud("aud2")
        .setJwksUrl(Arrays.asList("https://127.0.0.1:9999/foo.jwk", "https://127.0.0.1:9999/foo2.jwk"));
    authConf.put("issuers", Arrays.asList(iss1.asConfig(), iss2.asConfig()));
    plugin = new JWTAuthPlugin();
    plugin.init(authConf);
    assertEquals(2, plugin.getIssuerConfigs().size());
    assertTrue(plugin.getIssuerConfigs().get(0).usesHttpsJwk());
    assertTrue(plugin.getIssuerConfigs().get(1).usesHttpsJwk());
    JWTIssuerConfig issuer1 = plugin.getIssuerConfigByName("iss1");
    JWTIssuerConfig issuer2 = plugin.getIssuerConfigByName("iss2");
    assertNotNull(issuer1);
    assertNotNull(issuer2);
    assertEquals(2, issuer2.getJwksUrls().size());
    assertEquals("iss1", plugin.getPrimaryIssuer().getName());
    assertEquals("aud1", issuer1.getAud());
  }

  @Test
  public void initWithToplevelAndIssuersCombined() {
    HashMap<String, Object> authConf = new HashMap<>();
    JWTIssuerConfig iss1 = new JWTIssuerConfig("iss1").setIss("1").setAud("aud1")
        .setJwksUrl("https://127.0.0.1:9999/foo.jwk");
    authConf.put("issuers", Collections.singletonList(iss1.asConfig()));
    authConf.put("aud", "aud2");
    authConf.put("jwksUrl", Arrays.asList("https://127.0.0.1:9999/foo.jwk", "https://127.0.0.1:9999/foo2.jwk"));

    plugin = new JWTAuthPlugin();
    plugin.init(authConf);
    assertEquals(2, plugin.getIssuerConfigs().size());
    assertEquals("PRIMARY", plugin.getPrimaryIssuer().getName());
    assertEquals("aud2", plugin.getPrimaryIssuer().getAud());
    // Top-level (name=PRIMARY) issuer config does not need "iss" for back compat
    assertNull(plugin.getPrimaryIssuer().getIss());
  }
}