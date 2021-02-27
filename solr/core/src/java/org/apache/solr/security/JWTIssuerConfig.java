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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.Utils;
import org.jose4j.jwk.HttpsJwks;
import org.jose4j.jwk.JsonWebKey;
import org.jose4j.jwk.JsonWebKeySet;
import org.jose4j.lang.JoseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds information about an IdP (issuer), such as issuer ID, JWK url(s), keys etc
 */
public class JWTIssuerConfig {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  static final String PARAM_ISS_NAME = "name";
  static final String PARAM_JWKS_URL = "jwksUrl";
  static final String PARAM_JWK = "jwk";
  static final String PARAM_ISSUER = "iss";
  static final String PARAM_AUDIENCE = "aud";
  static final String PARAM_WELL_KNOWN_URL = "wellKnownUrl";
  static final String PARAM_AUTHORIZATION_ENDPOINT = "authorizationEndpoint";
  static final String PARAM_CLIENT_ID = "clientId";

  private static HttpsJwksFactory httpsJwksFactory =
      new HttpsJwksFactory(3600, 5000);
  private String iss;
  private String aud;
  private JsonWebKeySet jsonWebKeySet;
  private String name;
  private List<String> jwksUrl;
  private List<HttpsJwks> httpsJwks;
  private String wellKnownUrl;
  private WellKnownDiscoveryConfig wellKnownDiscoveryConfig;
  private String clientId;
  private String authorizationEndpoint;
  
  public static boolean ALLOW_OUTBOUND_HTTP = Boolean.parseBoolean(System.getProperty("solr.auth.jwt.allowOutboundHttp", "false"));
  public static final String ALLOW_OUTBOUND_HTTP_ERR_MSG = "HTTPS required for IDP communication. Please use SSL or start your nodes with -Dsolr.auth.jwt.allowOutboundHttp=true to allow HTTP for test purposes.";

  /**
   * Create config for further configuration with setters, builder style.
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
    parseConfigMap(configMap);
  }

  /**
   * Call this to validate and initialize an object which is populated with setters.
   * Init will fetch wellKnownUrl if relevant
   * @throws SolrException if issuer is missing
   */
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
    if (iss == null && usesHttpsJwk() && !JWTAuthPlugin.PRIMARY_ISSUER.equals(name)) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Missing required config 'iss' for issuer " + getName());
    }
  }

  /**
   * Parses configuration for one IssuerConfig and sets all variables found
   * @throws SolrException if unknown parameter names found in config
   */
  protected void parseConfigMap(Map<String, Object> configMap) {
    HashMap<String, Object> conf = new HashMap<>(configMap); // Clone
    setName((String) conf.get(PARAM_ISS_NAME));
    setWellKnownUrl((String) conf.get(PARAM_WELL_KNOWN_URL));
    setIss((String) conf.get(PARAM_ISSUER));
    setClientId((String) conf.get(PARAM_CLIENT_ID));
    setAud((String) conf.get(PARAM_AUDIENCE));
    Object confJwksUrl = conf.get(PARAM_JWKS_URL);
    setJwksUrl(confJwksUrl);
    setJsonWebKeySet(conf.get(PARAM_JWK));
    setAuthorizationEndpoint((String) conf.get(PARAM_AUTHORIZATION_ENDPOINT));

    conf.remove(PARAM_WELL_KNOWN_URL);
    conf.remove(PARAM_ISSUER);
    conf.remove(PARAM_ISS_NAME);
    conf.remove(PARAM_CLIENT_ID);
    conf.remove(PARAM_AUDIENCE);
    conf.remove(PARAM_JWKS_URL);
    conf.remove(PARAM_JWK);
    conf.remove(PARAM_AUTHORIZATION_ENDPOINT);

    if (!conf.isEmpty()) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown configuration key " + conf.keySet() + " for issuer " + name);
    }
  }

  /**
   * Setter that takes a jwk config object, parses it into a {@link JsonWebKeySet} and sets it
   * @param jwksObject the config object to parse
   */
  @SuppressWarnings("unchecked")
  protected void setJsonWebKeySet(Object jwksObject) {
    try {
      if (jwksObject != null) {
        jsonWebKeySet = parseJwkSet((Map<String, Object>) jwksObject);
      }
    } catch (JoseException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Failed parsing parameter 'jwk' for issuer " + getName(), e);
    }
  }

  @SuppressWarnings("unchecked")
  protected static JsonWebKeySet parseJwkSet(Map<String, Object> jwkObj) throws JoseException {
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

  private WellKnownDiscoveryConfig fetchWellKnown(String wellKnownUrl) {
    return WellKnownDiscoveryConfig.parse(wellKnownUrl);
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

  /**
   * Setter that converts from String or List into a list
   * @param jwksUrlListOrString object that should be either string or list
   * @return this for builder pattern
   * @throws SolrException if wrong type
   */
  @SuppressWarnings("unchecked")
  public JWTIssuerConfig setJwksUrl(Object jwksUrlListOrString) {
    if (jwksUrlListOrString instanceof String)
      this.jwksUrl = Collections.singletonList((String) jwksUrlListOrString);
    else if (jwksUrlListOrString instanceof List)
      this.jwksUrl = (List<String>) jwksUrlListOrString;
    else if (jwksUrlListOrString != null)
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Parameter " + PARAM_JWKS_URL + " must be either List or String");
    return this;
  }

  public List<HttpsJwks> getHttpsJwks() {
    if (httpsJwks == null) {
      httpsJwks = httpsJwksFactory.createList(getJwksUrls());
    }
    return httpsJwks;
  }

  /**
   * Set the factory to use when creating HttpsJwks objects
   * @param httpsJwksFactory factory with custom settings
   */
  public static void setHttpsJwksFactory(HttpsJwksFactory httpsJwksFactory) {
    JWTIssuerConfig.httpsJwksFactory = httpsJwksFactory;
  }

  public JsonWebKeySet getJsonWebKeySet() {
    return jsonWebKeySet;
  }

  public JWTIssuerConfig setJsonWebKeySet(JsonWebKeySet jsonWebKeySet) {
    this.jsonWebKeySet = jsonWebKeySet;
    return this;
  }

  /**
   * Check if the issuer is backed by HttpsJwk url(s)
   * @return true if keys are fetched over https
   */
  public boolean usesHttpsJwk() {
    return getJwksUrls() != null && !getJwksUrls().isEmpty();
  }

  public WellKnownDiscoveryConfig getWellKnownDiscoveryConfig() {
    return wellKnownDiscoveryConfig;
  }

  public String getAud() {
    return aud;
  }

  public JWTIssuerConfig setAud(String aud) {
    this.aud = aud;
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
    putIfNotNull(config, PARAM_ISS_NAME, name);
    putIfNotNull(config, PARAM_ISSUER, iss);
    putIfNotNull(config, PARAM_AUDIENCE, aud);
    putIfNotNull(config, PARAM_JWKS_URL, jwksUrl);
    putIfNotNull(config, PARAM_WELL_KNOWN_URL, wellKnownUrl);
    putIfNotNull(config, PARAM_CLIENT_ID, clientId);
    putIfNotNull(config, PARAM_AUTHORIZATION_ENDPOINT, authorizationEndpoint);
    if (jsonWebKeySet != null) {
      putIfNotNull(config, PARAM_JWK, jsonWebKeySet.getJsonWebKeys());
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
          PARAM_WELL_KNOWN_URL + ", " + PARAM_JWKS_URL + " and " + PARAM_JWK);
    }
    if (jwkConfigured > 0 && name == null) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Parameter 'name' is required for issuer configurations");
    }
    return jwkConfigured > 0;
  }

  /**
   *
   */
  static class HttpsJwksFactory {
    private final long jwkCacheDuration;
    private final long refreshReprieveThreshold;

    public HttpsJwksFactory(long jwkCacheDuration, long refreshReprieveThreshold) {
      this.jwkCacheDuration = jwkCacheDuration;
      this.refreshReprieveThreshold = refreshReprieveThreshold;
    }
    
    /**
     * While the class name is HttpsJwks, it actually works with plain http formatted url as well.
     * @param url the Url to connect to for JWK details.
     */
    private HttpsJwks create(String url) {
      try {
        URL jwksUrl = new URL(url);
        checkAllowOutboundHttpConnections(PARAM_JWKS_URL, jwksUrl);
        
      } catch (MalformedURLException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Url " + url + " configured in " + PARAM_JWKS_URL + " is not a valid URL");
      }
      HttpsJwks httpsJkws = new HttpsJwks(url);
      httpsJkws.setDefaultCacheDuration(jwkCacheDuration);
      httpsJkws.setRefreshReprieveThreshold(refreshReprieveThreshold);
      return httpsJkws;
    }

    public List<HttpsJwks> createList(List<String> jwkUrls) {
      return jwkUrls.stream().map(this::create).collect(Collectors.toList());
    }
  }

  /**
   * Config object for a OpenId Connect well-known config
   * Typically exposed through /.well-known/openid-configuration endpoint
   */
  public static class WellKnownDiscoveryConfig {
    private Map<String, Object> securityConf;

    WellKnownDiscoveryConfig(Map<String, Object> securityConf) {
      this.securityConf = securityConf;
    }

    public static WellKnownDiscoveryConfig parse(String urlString) {
      try {
        URL url = new URL(urlString);
        if (!Arrays.asList("https", "file", "http").contains(url.getProtocol())) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Well-known config URL must be one of HTTPS or HTTP or file");          
        }
        checkAllowOutboundHttpConnections(PARAM_WELL_KNOWN_URL, url);

        return parse(url.openStream());
      } catch (MalformedURLException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Well-known config URL " + urlString + " is malformed", e);
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Well-known config could not be read from url " + urlString, e);
      }
    }

    public static WellKnownDiscoveryConfig parse(String json, Charset charset) {
      return parse(new ByteArrayInputStream(json.getBytes(charset)));
    }

    @SuppressWarnings("unchecked")
    public static WellKnownDiscoveryConfig parse(InputStream configStream) {
      return new WellKnownDiscoveryConfig((Map<String, Object>) Utils.fromJSON(configStream));
    }


    public String getJwksUrl() {
      return (String) securityConf.get("jwks_uri");
    }

    public String getIssuer() {
      return (String) securityConf.get("issuer");
    }

    public String getAuthorizationEndpoint() {
      return (String) securityConf.get("authorization_endpoint");
    }

    public String getUserInfoEndpoint() {
      return (String) securityConf.get("userinfo_endpoint");
    }

    public String getTokenEndpoint() {
      return (String) securityConf.get("token_endpoint");
    }

    @SuppressWarnings("unchecked")
    public List<String> getScopesSupported() {
      return (List<String>) securityConf.get("scopes_supported");
    }

    @SuppressWarnings("unchecked")
    public List<String> getResponseTypesSupported() {
      return (List<String>) securityConf.get("response_types_supported");
    }
  }

  public static void checkAllowOutboundHttpConnections(String parameterName, URL url) {
    if ("http".equalsIgnoreCase(url.getProtocol())) {
      if (!ALLOW_OUTBOUND_HTTP) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, parameterName + " is using http protocol. " + ALLOW_OUTBOUND_HTTP_ERR_MSG);
      }
    }
  }
 
}
