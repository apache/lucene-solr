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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrException;
import org.jose4j.jwk.HttpsJwks;
import org.jose4j.jwk.JsonWebKey;
import org.jose4j.jwk.JsonWebKeySet;
import org.jose4j.lang.JoseException;

/**
 * Holds information about an IdP (issuer), such as issuer ID, JWK url(s), keys etc
 */
public class JWTIssuerConfig {
  private static JWTAuthPlugin.HttpsJwksFactory httpsJwksFactory =
      new JWTAuthPlugin.HttpsJwksFactory(3600, 5000);
  private String iss;
  private String aud;
  private JsonWebKeySet jsonWebKeySet;
  private String name;
  private Map<String, Object> configMap = null;
  private List<String> jwksUrl;
  private List<HttpsJwks> httpsJwks;
  private String wellKnownUrl;
  private JWTAuthPlugin.WellKnownDiscoveryConfig wellKnownDiscoveryConfig;
  private String clientId;
  private String authorizationEndpoint;

  /**
   * Create config for further configuration with setters.
   * Once all values are set, call {@link #init()} before further use
   *
   * @param name a unique name for this issuer
   */
  public JWTIssuerConfig(String name) {
    this.name = name;
  }

  /**
   * Initialize issuer config from a generic configuration map
   *
   * @param configMap map of configuration keys anv values
   */
  public JWTIssuerConfig(Map<String, Object> configMap) {
    this.configMap = configMap;
    parseConfigMap(configMap);
  }

  public void init() {
    if (!isValid()) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Configuration is not valid");
    }
    if (wellKnownUrl != null) {
      wellKnownDiscoveryConfig = fetchWellKnown(wellKnownUrl);
      if (iss == null) {
        iss = wellKnownDiscoveryConfig.getIssuer();
      }
      if (jwksUrl == null) {
        jwksUrl = Collections.singletonList(wellKnownDiscoveryConfig.getJwksUrl());
      }
      if (authorizationEndpoint == null) {
        authorizationEndpoint = wellKnownDiscoveryConfig.getAuthorizationEndpoint();
      }
    }
    if (iss == null && !JWTAuthPlugin.PRIMARY_ISSUER.equals(name) && !usesHttpsJwk()) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Missing required config 'iss' for issuer " + getName());
    }
  }

  /**
   * Parses configuration for one IssuerConfig
   */
  @SuppressWarnings("unchecked")
  void parseConfigMap(Map<String, Object> configMap) {
    HashMap<String, Object> conf = new HashMap<>(configMap); // Clone
    setName((String) conf.get(JWTAuthPlugin.PARAM_ISS_NAME));
    setWellKnownUrl((String) conf.get(JWTAuthPlugin.PARAM_WELL_KNOWN_URL));
    setIss((String) conf.get(JWTAuthPlugin.PARAM_ISSUER));
    setClientId((String) conf.get(JWTAuthPlugin.PARAM_CLIENT_ID));
    setAud((String) conf.get(JWTAuthPlugin.PARAM_AUDIENCE));
    setJwksUrl(conf.get(JWTAuthPlugin.PARAM_JWK_URL));
    setJwks(conf.get(JWTAuthPlugin.PARAM_JWK));

    conf.remove(JWTAuthPlugin.PARAM_WELL_KNOWN_URL);
    conf.remove(JWTAuthPlugin.PARAM_ISSUER);
    conf.remove(JWTAuthPlugin.PARAM_ISS_NAME);
    conf.remove(JWTAuthPlugin.PARAM_CLIENT_ID);
    conf.remove(JWTAuthPlugin.PARAM_AUDIENCE);
    conf.remove(JWTAuthPlugin.PARAM_JWK_URL);
    conf.remove(JWTAuthPlugin.PARAM_JWK);

    if (!conf.isEmpty()) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown configuration key " + conf.keySet() + " for issuer " + name);
    }
  }

  @SuppressWarnings("unchecked")
  private void setJwks(Object jwksObject) {
    try {
      if (jwksObject != null) {
        jsonWebKeySet = parseJwkSet((Map<String, Object>) jwksObject);
      }
    } catch (JoseException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Failed parsing parameter 'jwk' for issuer " + getName(), e);
    }
  }

  @SuppressWarnings("unchecked")
  public static JsonWebKeySet parseJwkSet(Map<String, Object> jwkObj) throws JoseException {
    JsonWebKeySet webKeySet = new JsonWebKeySet();
    if (jwkObj.containsKey("keys")) {
      List<Object> jwkList = (List<Object>) jwkObj.get("keys");
      for (Object jwkO : jwkList) {
        webKeySet.addJsonWebKey(JsonWebKey.Factory.newJwk((Map<String, Object>) jwkO));
      }
    } else {
      webKeySet = new JsonWebKeySet(JsonWebKey.Factory.newJwk(jwkObj));
    }
    return webKeySet;
  }

  private JWTAuthPlugin.WellKnownDiscoveryConfig fetchWellKnown(String wellKnownUrl) {
    return JWTAuthPlugin.WellKnownDiscoveryConfig.parse(wellKnownUrl);
  }

  public String getIss() {
    return iss;
  }

  public JWTIssuerConfig setIss(String iss) {
    this.iss = iss;
    return this;
  }

  public String getName() {
    return name;
  }

  public JWTIssuerConfig setName(String name) {
    this.name = name;
    return this;
  }

  public String getWellKnownUrl() {
    return wellKnownUrl;
  }

  public JWTIssuerConfig setWellKnownUrl(String wellKnownUrl) {
    this.wellKnownUrl = wellKnownUrl;
    return this;
  }

  public List<String> getJwksUrls() {
    return jwksUrl;
  }

  public JWTIssuerConfig setJwksUrl(List<String> jwksUrl) {
    this.jwksUrl = jwksUrl;
    return this;
  }

  @SuppressWarnings("unchecked")
  public JWTIssuerConfig setJwksUrl(Object jwksUrlListOrString) {
    if (jwksUrlListOrString instanceof String)
      this.jwksUrl = Collections.singletonList((String) jwksUrlListOrString);
    else if (jwksUrlListOrString instanceof List)
      this.jwksUrl = (List<String>) jwksUrlListOrString;
    else if (jwksUrlListOrString != null)
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Parameter " + JWTAuthPlugin.PARAM_JWK_URL + " must be either List or String");
    return this;
  }

  public List<HttpsJwks> getHttpsJwks() {
    if (httpsJwks == null) {
      httpsJwks = httpsJwksFactory.createList(getJwksUrls());
    }
    return httpsJwks;
  }

  public JWTIssuerConfig setHttpsJwks(List<HttpsJwks> httpsJwks) {
    this.httpsJwks = httpsJwks;
    return this;
  }

  /**
   * Set the factory to use when creating HttpsJwks objects
   * @param httpsJwksFactory factory with custom settings
   */
  public static void setHttpsJwksFactory(JWTAuthPlugin.HttpsJwksFactory httpsJwksFactory) {
    JWTIssuerConfig.httpsJwksFactory = httpsJwksFactory;
  }

  public JsonWebKeySet getJsonWebKeySet() {
    return jsonWebKeySet;
  }

  /**
   * Check if the issuer is backed by HttpsJwk url(s)
   * @return true if keys are fetched over https
   */
  public boolean usesHttpsJwk() {
    return getJwksUrls() != null && !getJwksUrls().isEmpty();
  }

  public Map<String, Object> getConfigMap() {
    return configMap;
  }

  public JWTAuthPlugin.WellKnownDiscoveryConfig getWellKnownDiscoveryConfig() {
    return wellKnownDiscoveryConfig;
  }

  public String getAud() {
    return aud;
  }

  public JWTIssuerConfig setAud(String aud) {
    this.aud = aud;
    return this;
  }

  public JWTIssuerConfig setJwks(JsonWebKeySet jsonWebKeySet) {
    this.jsonWebKeySet = jsonWebKeySet;
    return this;
  }

  public String getClientId() {
    return clientId;
  }

  public JWTIssuerConfig setClientId(String clientId) {
    this.clientId = clientId;
    return this;
  }

  public String getAuthorizationEndpoint() {
    return authorizationEndpoint;
  }

  public JWTIssuerConfig setAuthorizationEndpoint(String authorizationEndpoint) {
    this.authorizationEndpoint = authorizationEndpoint;
    return this;
  }

  public Map<String,Object> asConfig() {
    HashMap<String,Object> config = new HashMap<>();
    putIfNotNull(config, JWTAuthPlugin.PARAM_ISS_NAME, name);
    putIfNotNull(config, JWTAuthPlugin.PARAM_ISSUER, iss);
    putIfNotNull(config, JWTAuthPlugin.PARAM_AUDIENCE, aud);
    putIfNotNull(config, JWTAuthPlugin.PARAM_JWK_URL, jwksUrl);
    putIfNotNull(config, JWTAuthPlugin.PARAM_WELL_KNOWN_URL, wellKnownUrl);
    putIfNotNull(config, JWTAuthPlugin.PARAM_CLIENT_ID, clientId);
    putIfNotNull(config, JWTAuthPlugin.PARAM_AUTHORIZATION_ENDPOINT, authorizationEndpoint);
    if (jsonWebKeySet != null) {
      putIfNotNull(config, JWTAuthPlugin.PARAM_JWK, jsonWebKeySet.getJsonWebKeys());
    }
    return config;
  }

  private void putIfNotNull(HashMap<String, Object> config, String paramName, Object value) {
    if (value != null) {
      config.put(paramName, value);
    }
  }

  /**
   * Validates that this config has a name and either jwksUrl, wellkKownUrl or jwk
   * @return true if a configuration is found and is valid, otherwise false
   * @throws SolrException if configuration is present but wrong
   */
  public boolean isValid() {
    int jwkConfigured = wellKnownUrl != null ? 1 : 0;
    jwkConfigured += jwksUrl != null ? 2 : 0;
    jwkConfigured += jsonWebKeySet != null ? 2 : 0;
    if (jwkConfigured > 3) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "JWTAuthPlugin needs to configure exactly one of " +
          JWTAuthPlugin.PARAM_WELL_KNOWN_URL + ", " + JWTAuthPlugin.PARAM_JWK_URL + " and " + JWTAuthPlugin.PARAM_JWK);
    }
    if (jwkConfigured > 0 && name == null) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Parameter 'name' is required for issuer configurations");
    }
    return jwkConfigured > 0;
  }
}
