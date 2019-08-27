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
import java.util.stream.Collectors;

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
 * This implementation simply collects all keys from all endpoints into a single list and
 * the rest of the implementation is equivalent to that of HttpsJwksVerificationKeyResolver.
 *
 * No attempt is made to keep track of which key came from which JWKs endpoint, and if a
 * key is not found in any cache, all JWKs endpoints are refreshed before a single retry.
 */
public class MultiHttpsJwksVerificationkeyResolver implements VerificationKeyResolver {
  private static final Logger log = LoggerFactory.getLogger(MultiHttpsJwksVerificationkeyResolver.class);
  private final List<String> locations;

  private VerificationJwkSelector verificationJwkSelector = new VerificationJwkSelector();

  private boolean disambiguateWithVerifySignature;

  private List<HttpsJwks> httpsJkwsList;

  public MultiHttpsJwksVerificationkeyResolver(List<HttpsJwks> httpsJkwsList) {
    this.httpsJkwsList = httpsJkwsList;
    this.locations = httpsJkwsList.stream().map(HttpsJwks::getLocation).collect(Collectors.toList());
  }

  @Override
  public Key resolveKey(JsonWebSignature jws, List<JsonWebStructure> nestingContext) throws UnresolvableKeyException {
    JsonWebKey theChosenOne;
    List<JsonWebKey> jsonWebKeys = new ArrayList<>();

    try {
      // Add all keys into a master list
      for (HttpsJwks hjwks : httpsJkwsList) {
        jsonWebKeys.addAll(hjwks.getJsonWebKeys());
      }

      theChosenOne = select(jws, jsonWebKeys);
      if (theChosenOne == null) {
        log.debug("Refreshing JWKs from all {} locations, as no suitable verification key for JWS w/ header {} was found in {}",
            httpsJkwsList.size(), jws.getHeaders().getFullHeaderAsJsonString(), jsonWebKeys);

        jsonWebKeys.clear();
        for (HttpsJwks hjwks : httpsJkwsList) {
          jsonWebKeys.addAll(hjwks.getJsonWebKeys());
        }
        theChosenOne = select(jws, jsonWebKeys);
      }
    } catch (JoseException | IOException e) {
      StringBuilder sb = new StringBuilder();
      sb.append("Unable to find a suitable verification key for JWS w/ header ").append(jws.getHeaders().getFullHeaderAsJsonString());
      sb.append(" due to an unexpected exception (").append(e).append(") while obtaining or using keys from JWKS endpoints at ");
      sb.append(locations);
      throw new UnresolvableKeyException(sb.toString(), e);
    }

    if (theChosenOne == null) {
      StringBuilder sb = new StringBuilder();
      sb.append("Unable to find a suitable verification key for JWS w/ header ").append(jws.getHeaders().getFullHeaderAsJsonString());
      sb.append(" from JWKs ").append(jsonWebKeys).append(" obtained from ").append(locations);
      throw new UnresolvableKeyException(sb.toString());
    }

    return theChosenOne.getKey();
  }

  // TODO: This method is copy/pasted from HttpsJwksVerificationKeyResolver since it was private. Else we couldhave
  // TODO: extended HttpsJwksVerificationKeyResolver instead of duplicating here

  private JsonWebKey select(JsonWebSignature jws, List<JsonWebKey> jsonWebKeys) throws JoseException
  {
    if (disambiguateWithVerifySignature)
    {
      return verificationJwkSelector.selectWithVerifySignatureDisambiguate(jws, jsonWebKeys);
    }
    else
    {
      return verificationJwkSelector.select(jws, jsonWebKeys);
    }
  }

  /**
   * Indicates whether or not to use signature verification to try and disambiguate when the normal key selection based on the JWS headers results in more than one key. Default is false.
   * @param disambiguateWithVerifySignature boolean indicating whether or not to use signature verification to disambiguate
   */
  public void setDisambiguateWithVerifySignature(boolean disambiguateWithVerifySignature)
  {
    this.disambiguateWithVerifySignature = disambiguateWithVerifySignature;
  }
}
