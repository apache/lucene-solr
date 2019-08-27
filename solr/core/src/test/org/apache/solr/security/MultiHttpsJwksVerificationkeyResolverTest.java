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

import org.jose4j.jwk.HttpsJwks;
import org.jose4j.jwk.JsonWebKey;
import org.jose4j.jwk.RsaJsonWebKey;
import org.jose4j.jwk.RsaJwkGenerator;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.lang.JoseException;
import org.jose4j.lang.UnresolvableKeyException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

/**
 * Tests the multi jwks resolver that can fetch keys from multiple JWKs
 */
@RunWith(MockitoJUnitRunner.class)
public class MultiHttpsJwksVerificationkeyResolverTest {
  private MultiHttpsJwksVerificationkeyResolver resolver;

  @Mock
  private HttpsJwks oneTwo;
  @Mock
  private HttpsJwks threeFour;

  private KeyHolder k1;
  private KeyHolder k2;
  private KeyHolder k3;
  private KeyHolder k4;
  private KeyHolder k5;

  @Before
  public void setUp() throws Exception {
    k1 = new KeyHolder("k1");
    k2 = new KeyHolder("k2");
    k3 = new KeyHolder("k3");
    k4 = new KeyHolder("k4");
    k5 = new KeyHolder("k5");

    when(oneTwo.getJsonWebKeys()).thenReturn(Arrays.asList(k1.getJwk(), k2.getJwk()));
    when(threeFour.getJsonWebKeys())
        .thenReturn(Arrays.asList(k3.getJwk(), k4.getJwk()))
        .thenReturn(Arrays.asList(k3.getJwk(), k4.getJwk()))
        .thenReturn(Arrays.asList(k3.getJwk(), k4.getJwk()))
        .thenReturn(Arrays.asList(k3.getJwk(), k4.getJwk()))
        .thenReturn(Arrays.asList(k5.getJwk())); // On fifth invocation, return k5 instead of k3,k4
    resolver = new MultiHttpsJwksVerificationkeyResolver(Arrays.asList(oneTwo, threeFour));
  }

  @Test
  public void findKeyFromFirstList() throws UnresolvableKeyException {
    resolver.resolveKey(k1.getJws(), null);
    resolver.resolveKey(k2.getJws(), null);
    resolver.resolveKey(k3.getJws(), null);
    resolver.resolveKey(k4.getJws(), null);
    resolver.resolveKey(k5.getJws(), null);
  }

  @Test(expected = UnresolvableKeyException.class)
  public void notFoundKey() throws UnresolvableKeyException {
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