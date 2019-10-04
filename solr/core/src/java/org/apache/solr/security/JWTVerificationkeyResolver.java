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
import java.lang.invoke.MethodHandles;
import java.security.Key;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrException;
import org.jose4j.jwk.HttpsJwks;
import org.jose4j.jwk.JsonWebKey;
import org.jose4j.jwk.VerificationJwkSelector;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.jwt.MalformedClaimException;
import org.jose4j.jwt.consumer.InvalidJwtException;
import org.jose4j.jwx.JsonWebStructure;
import org.jose4j.keys.resolvers.VerificationKeyResolver;
import org.jose4j.lang.JoseException;
import org.jose4j.lang.UnresolvableKeyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves jws signature verification keys from a set of {@link JWTIssuerConfig} objects, which
 * may represent any valid configuration in Solr's security.json, i.e. static list of JWKs
 * or keys retrieved from HTTPs JWK endpoints.
 *
 * This implementation maintains a map of issuers, each with its own list of {@link JsonWebKey},
 * and resolves correct key from correct issuer similar to HttpsJwksVerificationKeyResolver.
 * If issuer claim is not required, we will select the first IssuerConfig if there is exactly one such config.
 *
 * If a key is not found, and issuer is backed by HTTPsJWKs, we attempt one cache refresh before failing.
 */
public class JWTVerificationkeyResolver implements VerificationKeyResolver {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private VerificationJwkSelector verificationJwkSelector = new VerificationJwkSelector();

  private Map<String, JWTIssuerConfig> issuerConfigs = new HashMap<>();
  private final boolean requireIssuer;

  /**
   * Resolves key from a JWKs from one or more IssuerConfigs
   * @param issuerConfigs Collection of configuration objects for the issuer(s)
   * @param requireIssuer if true, will require 'iss' claim on jws
   */
  public JWTVerificationkeyResolver(Collection<JWTIssuerConfig> issuerConfigs, boolean requireIssuer) {
    this.requireIssuer = requireIssuer;
    issuerConfigs.forEach(ic -> {
      this.issuerConfigs.put(ic.getIss(), ic);
    });
  }

  @Override
  public Key resolveKey(JsonWebSignature jws, List<JsonWebStructure> nestingContext) throws UnresolvableKeyException {
    JsonWebKey theChosenOne;
    List<JsonWebKey> jsonWebKeys = new ArrayList<>();

    String keysSource = "N/A";
    try {
      String tokenIssuer = JwtClaims.parse(jws.getUnverifiedPayload()).getIssuer();
      JWTIssuerConfig issuerConfig;
      if (tokenIssuer == null) {
        if (requireIssuer) {
          throw new UnresolvableKeyException("Token does not contain required issuer claim");
        } else if (issuerConfigs.size() == 1) {
          issuerConfig = issuerConfigs.values().iterator().next();
        } else {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              "Signature verifiction not supported for multiple issuers without 'iss' claim in token.");
        }
      } else {
        issuerConfig = issuerConfigs.get(tokenIssuer);
        if (issuerConfig == null) {
          if (issuerConfigs.size() > 1) {
            throw new UnresolvableKeyException("No issuers configured for iss='" + tokenIssuer + "', cannot validate signature");
          } else if (issuerConfigs.size() == 1) {
            issuerConfig = issuerConfigs.values().iterator().next();
            log.debug("No issuer matching token's iss claim, but exactly one configured, selecting that one");
          } else {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                "Signature verifiction failed due to no configured issuer with id " + tokenIssuer);
          }
        }
      }

      // Add all keys into a master list
      if (issuerConfig.usesHttpsJwk()) {
        keysSource = "[" + String.join(", ", issuerConfig.getJwksUrls()) + "]";
        for (HttpsJwks hjwks : issuerConfig.getHttpsJwks()) {
          jsonWebKeys.addAll(hjwks.getJsonWebKeys());
        }
      } else {
        keysSource = "static list of keys in security.json";
        jsonWebKeys.addAll(issuerConfig.getJsonWebKeySet().getJsonWebKeys());
      }

      theChosenOne = verificationJwkSelector.select(jws, jsonWebKeys);
      if (theChosenOne == null && issuerConfig.usesHttpsJwk()) {
        log.debug("Refreshing JWKs from all {} locations, as no suitable verification key for JWS w/ header {} was found in {}",
            issuerConfig.getHttpsJwks().size(), jws.getHeaders().getFullHeaderAsJsonString(), jsonWebKeys);

        jsonWebKeys.clear();
        for (HttpsJwks hjwks : issuerConfig.getHttpsJwks()) {
          hjwks.refresh();
          jsonWebKeys.addAll(hjwks.getJsonWebKeys());
        }
        theChosenOne = verificationJwkSelector.select(jws, jsonWebKeys);
      }
    } catch (JoseException | IOException | InvalidJwtException | MalformedClaimException e) {
      StringBuilder sb = new StringBuilder();
      sb.append("Unable to find a suitable verification key for JWS w/ header ").append(jws.getHeaders().getFullHeaderAsJsonString());
      sb.append(" due to an unexpected exception (").append(e).append(") while obtaining or using keys from source ");
      sb.append(keysSource);
      throw new UnresolvableKeyException(sb.toString(), e);
    }

    if (theChosenOne == null) {
      StringBuilder sb = new StringBuilder();
      sb.append("Unable to find a suitable verification key for JWS w/ header ").append(jws.getHeaders().getFullHeaderAsJsonString());
      sb.append(" from ").append(jsonWebKeys.size()).append(" keys from source ").append(keysSource);
      throw new UnresolvableKeyException(sb.toString());
    }

    return theChosenOne.getKey();
  }

  Set<JWTIssuerConfig> getIssuerConfigs() {
    return new HashSet<>(issuerConfigs.values());
  }
}
