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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.security.JWTIssuerConfig.HttpsJwksFactory;
import org.jose4j.jwk.HttpsJwks;
import org.jose4j.jwk.JsonWebKey;
import org.jose4j.jwk.RsaJsonWebKey;
import org.jose4j.jwk.RsaJwkGenerator;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.lang.JoseException;
import org.jose4j.lang.UnresolvableKeyException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import static java.util.Arrays.asList;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

/**
 * Tests the multi jwks resolver that can fetch keys from multiple JWKs
 */
public class JWTVerificationkeyResolverTest extends SolrTestCaseJ4 {
  private JWTVerificationkeyResolver resolver;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock
  private HttpsJwks firstJwkList;
  @Mock
  private HttpsJwks secondJwkList;
  @Mock
  private HttpsJwksFactory httpsJwksFactory;

  private KeyHolder k1;
  private KeyHolder k2;
  private KeyHolder k3;
  private KeyHolder k4;
  private KeyHolder k5;
  private List<JsonWebKey> keysToReturnFromSecondJwk;
  private Iterator refreshSequenceForSecondJwk;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    k1 = new KeyHolder("k1");
    k2 = new KeyHolder("k2");
    k3 = new KeyHolder("k3");
    k4 = new KeyHolder("k4");
    k5 = new KeyHolder("k5");

    when(firstJwkList.getJsonWebKeys()).thenReturn(asList(k1.getJwk(), k2.getJwk()));
    doAnswer(invocation -> {
      keysToReturnFromSecondJwk = (List<JsonWebKey>) refreshSequenceForSecondJwk.next();
      System.out.println("Refresh called, next to return is " + keysToReturnFromSecondJwk);
      return null;
    }).when(secondJwkList).refresh();
    when(secondJwkList.getJsonWebKeys()).then(inv -> {
      if (keysToReturnFromSecondJwk == null)
        keysToReturnFromSecondJwk = (List<JsonWebKey>) refreshSequenceForSecondJwk.next();
      return keysToReturnFromSecondJwk;
    });
    when(httpsJwksFactory.createList(anyList())).thenReturn(asList(firstJwkList, secondJwkList));

    JWTIssuerConfig issuerConfig = new JWTIssuerConfig("primary").setIss("foo").setJwksUrl(asList("url1", "url2"));
    issuerConfig.setHttpsJwksFactory(httpsJwksFactory);
    resolver = new JWTVerificationkeyResolver(Arrays.asList(issuerConfig), true);

    assumeWorkingMockito();
  }

  @Test
  public void findKeyFromFirstList() throws JoseException {
    refreshSequenceForSecondJwk = asList(
        asList(k3.getJwk(), k4.getJwk()),
        asList(k5.getJwk())).iterator();
    resolver.resolveKey(k1.getJws(), null);
    resolver.resolveKey(k2.getJws(), null);
    resolver.resolveKey(k3.getJws(), null);
    resolver.resolveKey(k4.getJws(), null);
    // Key k5 is not in cache, so a refresh will be done, which
    resolver.resolveKey(k5.getJws(), null);
  }

  @Test(expected = UnresolvableKeyException.class)
  public void notFoundKey() throws JoseException {
    refreshSequenceForSecondJwk = asList(
        asList(k3.getJwk()),
        asList(k4.getJwk()),
        asList(k5.getJwk())).iterator();
    // Will not find key since first refresh returns k4, and we only try one refresh.
    resolver.resolveKey(k5.getJws(), null);
  }

  public class KeyHolder {
    private final RsaJsonWebKey key;
    private final String kid;

    public KeyHolder(String kid) throws JoseException {
      key = generateKey(kid);
      this.kid = kid;
    }

    public RsaJsonWebKey getRsaKey() {
      return key;
    }

    public JsonWebKey getJwk() throws JoseException {
      JsonWebKey jsonKey = JsonWebKey.Factory.newJwk(key.getRsaPublicKey());
      jsonKey.setKeyId(kid);
      return jsonKey;
    }

    public JsonWebSignature getJws() {
      JsonWebSignature jws = new JsonWebSignature();
      jws.setPayload(JWTAuthPluginTest.generateClaims().toJson());
      jws.setKey(getRsaKey().getPrivateKey());
      jws.setKeyIdHeaderValue(getRsaKey().getKeyId());
      jws.setAlgorithmHeaderValue(AlgorithmIdentifiers.RSA_USING_SHA256);
      return jws;
    }

    private RsaJsonWebKey generateKey(String kid) throws JoseException {
      RsaJsonWebKey rsaJsonWebKey = RsaJwkGenerator.generateJwk(2048);
      rsaJsonWebKey.setKeyId(kid);
      return rsaJsonWebKey;
    }
  }
}