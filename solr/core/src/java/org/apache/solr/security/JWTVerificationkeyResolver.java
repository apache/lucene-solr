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
import java.security.Key;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.security.JWTAuthPlugin.HttpsJwksFactory;
import org.apache.solr.security.JWTAuthPlugin.IssuerConfig;
import org.jose4j.jwk.HttpsJwks;
import org.jose4j.jwk.JsonWebKey;
import org.jose4j.jwk.VerificationJwkSelector;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwx.JsonWebStructure;
import org.jose4j.keys.resolvers.VerificationKeyResolver;
import org.jose4j.lang.JoseException;
import org.jose4j.lang.UnresolvableKeyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adaption of {@link org.jose4j.keys.resolvers.HttpsJwksVerificationKeyResolver} to resolve
 * keys from multiple HttpsJwks endpoints, which is sometimes necessary if the IdP
 * does not publish all public keys that may have signed a token through the main JWKs endpoint.
 * Such setups typically have support for multiple signing backends, each serving its own JWKs
 * endpoint for its keys.
 *
 * This implementation collects all keys from all endpoints into a single list and
 * the rest of the implementation is equivalent to that of HttpsJwksVerificationKeyResolver.
 *
 * No attempt is made to keep track of which key came from which JWKs endpoint, and if a
 * key is not found in any cache, all JWKs endpoints are refreshed before a single retry.
 *
 * NOTE: This class can subclass HttpsJwksVerificationKeyResolver once a new version of jose4j is available
 */
public class JWTVerificationkeyResolver implements VerificationKeyResolver {
  private static final Logger log = LoggerFactory.getLogger(JWTVerificationkeyResolver.class);
  private final List<HttpsJwks> httpsJwksList;

  private VerificationJwkSelector verificationJwkSelector = new VerificationJwkSelector();

  private IssuerConfig issuerConfig;

  /**
   * Resolves key from a list of JWKs URLs stored in IssuerConfig
   * @param issuerConfig Configuration object for the issuer
   * @param httpsJwksFactory Factory used to create HttpsJwks objects from URLs
   */
  public JWTVerificationkeyResolver(IssuerConfig issuerConfig, HttpsJwksFactory httpsJwksFactory) {
    this.issuerConfig = issuerConfig;
    this.httpsJwksList = httpsJwksFactory.createList(issuerConfig.getJwksUrl());
  }

  @Override
  public Key resolveKey(JsonWebSignature jws, List<JsonWebStructure> nestingContext) throws UnresolvableKeyException {
    JsonWebKey theChosenOne;
    List<JsonWebKey> jsonWebKeys = new ArrayList<>();


    try {
      // Add all keys into a master list
      for (HttpsJwks hjwks : httpsJwksList) {
        jsonWebKeys.addAll(hjwks.getJsonWebKeys());
      }

      theChosenOne = verificationJwkSelector.select(jws, jsonWebKeys);
      if (theChosenOne == null) {
        log.debug("Refreshing JWKs from all {} locations, as no suitable verification key for JWS w/ header {} was found in {}",
            httpsJwksList.size(), jws.getHeaders().getFullHeaderAsJsonString(), jsonWebKeys);

        jsonWebKeys.clear();
        for (HttpsJwks hjwks : httpsJwksList) {
          hjwks.refresh();
          jsonWebKeys.addAll(hjwks.getJsonWebKeys());
        }
        theChosenOne = verificationJwkSelector.select(jws, jsonWebKeys);
      }
    } catch (JoseException | IOException e) {
      StringBuilder sb = new StringBuilder();
      sb.append("Unable to find a suitable verification key for JWS w/ header ").append(jws.getHeaders().getFullHeaderAsJsonString());
      sb.append(" due to an unexpected exception (").append(e).append(") while obtaining or using keys from JWKS endpoints at ");
      sb.append(issuerConfig.getJwksUrl());
      throw new UnresolvableKeyException(sb.toString(), e);
    }

    if (theChosenOne == null) {
      StringBuilder sb = new StringBuilder();
      sb.append("Unable to find a suitable verification key for JWS w/ header ").append(jws.getHeaders().getFullHeaderAsJsonString());
      sb.append(" from JWKs ").append(jsonWebKeys).append(" obtained from ").append(issuerConfig.getJwksUrl());
      throw new UnresolvableKeyException(sb.toString());
    }

    return theChosenOne.getKey();
  }

  IssuerConfig getIssuerConfig() {
    return issuerConfig;
  }
}
