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

import javax.servlet.FilterChain;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.protocol.HttpContext;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SpecProvider;
import org.apache.solr.common.StringUtils;
import org.apache.solr.common.util.Base64;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.common.util.Utils;
import org.apache.solr.common.util.ValidatingJsonMap;
import org.apache.solr.security.JWTAuthPlugin.JWTAuthenticationResponse.AuthCode;
import org.eclipse.jetty.client.api.Request;
import org.jose4j.jwa.AlgorithmConstraints;
import org.jose4j.jwk.HttpsJwks;
import org.jose4j.jwk.JsonWebKey;
import org.jose4j.jwk.JsonWebKeySet;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.jwt.MalformedClaimException;
import org.jose4j.jwt.consumer.InvalidJwtException;
import org.jose4j.jwt.consumer.JwtConsumer;
import org.jose4j.jwt.consumer.JwtConsumerBuilder;
import org.jose4j.keys.resolvers.HttpsJwksVerificationKeyResolver;
import org.jose4j.keys.resolvers.JwksVerificationKeyResolver;
import org.jose4j.keys.resolvers.VerificationKeyResolver;
import org.jose4j.lang.JoseException;
import org.noggit.JSONUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Authenticaion plugin that finds logged in user by validating the signature of a JWT token
 */
public class JWTAuthPlugin extends AuthenticationPlugin implements SpecProvider, ConfigEditablePlugin {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String PARAM_BLOCK_UNKNOWN = "blockUnknown";
  private static final String PARAM_JWK_URL = "jwkUrl";
  private static final String PARAM_JWK = "jwk";
  private static final String PARAM_ISSUER = "iss";
  private static final String PARAM_AUDIENCE = "aud";
  private static final String PARAM_REQUIRE_SUBJECT = "requireSub";
  private static final String PARAM_PRINCIPAL_CLAIM = "principalClaim";
  private static final String PARAM_REQUIRE_EXPIRATIONTIME = "requireExp";
  private static final String PARAM_ALG_WHITELIST = "algWhitelist";
  private static final String PARAM_JWK_CACHE_DURATION = "jwkCacheDur";
  private static final String PARAM_CLAIMS_MATCH = "claimsMatch";
  private static final String PARAM_SCOPE = "scope";
  private static final String PARAM_ADMINUI_SCOPE = "adminUiScope";
  private static final String PARAM_REDIRECT_URIS = "redirectUris";
  private static final String PARAM_CLIENT_ID = "clientId";
  private static final String PARAM_WELL_KNOWN_URL = "wellKnownUrl";
  private static final String PARAM_AUTHORIZATION_ENDPOINT = "authorizationEndpoint";

  private static final String AUTH_REALM = "solr-jwt";
  private static final String CLAIM_SCOPE = "scope";
  private static final long RETRY_INIT_DELAY_SECONDS = 30;

  private static final Set<String> PROPS = ImmutableSet.of(PARAM_BLOCK_UNKNOWN, PARAM_JWK_URL, PARAM_JWK, PARAM_ISSUER,
      PARAM_AUDIENCE, PARAM_REQUIRE_SUBJECT, PARAM_PRINCIPAL_CLAIM, PARAM_REQUIRE_EXPIRATIONTIME, PARAM_ALG_WHITELIST,
      PARAM_JWK_CACHE_DURATION, PARAM_CLAIMS_MATCH, PARAM_SCOPE, PARAM_CLIENT_ID, PARAM_WELL_KNOWN_URL, 
      PARAM_AUTHORIZATION_ENDPOINT, PARAM_ADMINUI_SCOPE, PARAM_REDIRECT_URIS);

  private JwtConsumer jwtConsumer;
  private String iss;
  private String aud;
  private boolean requireSubject;
  private boolean requireExpirationTime;
  private List<String> algWhitelist;
  private VerificationKeyResolver verificationKeyResolver;
  private String principalClaim;
  private HashMap<String, Pattern> claimsMatchCompiled;
  private boolean blockUnknown;
  private List<String> requiredScopes = new ArrayList<>();
  private String clientId;
  private long jwkCacheDuration;
  private WellKnownDiscoveryConfig oidcDiscoveryConfig;
  private String confIdpConfigUrl;
  private Map<String, Object> pluginConfig;
  private Instant lastInitTime = Instant.now();
  private String authorizationEndpoint;
  private String adminUiScope;
  private List<String> redirectUris;


  /**
   * Initialize plugin
   */
  public JWTAuthPlugin() {}

  @Override
  public void init(Map<String, Object> pluginConfig) {
    List<String> unknownKeys = pluginConfig.keySet().stream().filter(k -> !PROPS.contains(k)).collect(Collectors.toList());
    unknownKeys.remove("class");
    unknownKeys.remove("");
    if (!unknownKeys.isEmpty()) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Invalid JwtAuth configuration parameter " + unknownKeys); 
    }

    blockUnknown = Boolean.parseBoolean(String.valueOf(pluginConfig.getOrDefault(PARAM_BLOCK_UNKNOWN, false)));
    clientId = (String) pluginConfig.get(PARAM_CLIENT_ID);
    requireSubject = Boolean.parseBoolean(String.valueOf(pluginConfig.getOrDefault(PARAM_REQUIRE_SUBJECT, "true")));
    requireExpirationTime = Boolean.parseBoolean(String.valueOf(pluginConfig.getOrDefault(PARAM_REQUIRE_EXPIRATIONTIME, "true")));
    principalClaim = (String) pluginConfig.getOrDefault(PARAM_PRINCIPAL_CLAIM, "sub");
    confIdpConfigUrl = (String) pluginConfig.get(PARAM_WELL_KNOWN_URL);
    Object redirectUrisObj = pluginConfig.get(PARAM_REDIRECT_URIS);
    redirectUris = Collections.emptyList();
    if (redirectUrisObj != null) {
      if (redirectUrisObj instanceof String) {
        redirectUris = Collections.singletonList((String) redirectUrisObj);
      } else if (redirectUrisObj instanceof List) {
        redirectUris = (List<String>) redirectUrisObj;
      }
    } 
    
    if (confIdpConfigUrl != null) {
      log.debug("Initializing well-known oidc config from {}", confIdpConfigUrl);
      oidcDiscoveryConfig = WellKnownDiscoveryConfig.parse(confIdpConfigUrl);
      iss = oidcDiscoveryConfig.getIssuer();
      authorizationEndpoint = oidcDiscoveryConfig.getAuthorizationEndpoint();
    }
    
    if (pluginConfig.containsKey(PARAM_ISSUER)) {
      if (iss != null) {
        log.debug("Explicitly setting required issuer instead of using issuer from well-known config");
      }
      iss = (String) pluginConfig.get(PARAM_ISSUER);
    }

    if (pluginConfig.containsKey(PARAM_AUTHORIZATION_ENDPOINT)) {
      if (authorizationEndpoint != null) {
        log.debug("Explicitly setting authorizationEndpoint instead of using issuer from well-known config");
      }
      authorizationEndpoint = (String) pluginConfig.get(PARAM_AUTHORIZATION_ENDPOINT);
    }
    
    if (pluginConfig.containsKey(PARAM_AUDIENCE)) {
      if (clientId != null) {
        log.debug("Explicitly setting required audience instead of using configured clientId");
      }
      aud = (String) pluginConfig.get(PARAM_AUDIENCE);
    } else {
      aud = clientId;
    }
    
    algWhitelist = (List<String>) pluginConfig.get(PARAM_ALG_WHITELIST);

    String requiredScopesStr = (String) pluginConfig.get(PARAM_SCOPE);
    if (!StringUtils.isEmpty(requiredScopesStr)) {
      requiredScopes = Arrays.asList(requiredScopesStr.split("\\s+"));
    }
    
    adminUiScope = (String) pluginConfig.get(PARAM_ADMINUI_SCOPE);
    if (adminUiScope == null && requiredScopes.size() > 0) {
      adminUiScope = requiredScopes.get(0);
      log.warn("No adminUiScope given, using first scope in 'scope' list as required scope for accessing Admin UI");
    }
    
    if (adminUiScope == null) {
      adminUiScope = "solr";
      log.warn("Warning: No adminUiScope provided, fallback to 'solr' as required scope. If this is not correct, the Admin UI login may not work");
    }
    
    Map<String, String> claimsMatch = (Map<String, String>) pluginConfig.get(PARAM_CLAIMS_MATCH);
    claimsMatchCompiled = new HashMap<>();
    if (claimsMatch != null) {
      for (Map.Entry<String, String> entry : claimsMatch.entrySet()) {
        claimsMatchCompiled.put(entry.getKey(), Pattern.compile(entry.getValue()));
      }
    }

    initJwk(pluginConfig);

    lastInitTime = Instant.now();
  }

  private void initJwk(Map<String, Object> pluginConfig) {
    this.pluginConfig = pluginConfig;
    String confJwkUrl = (String) pluginConfig.get(PARAM_JWK_URL);
    Map<String, Object> confJwk = (Map<String, Object>) pluginConfig.get(PARAM_JWK);
    jwkCacheDuration = Long.parseLong((String) pluginConfig.getOrDefault(PARAM_JWK_CACHE_DURATION, "3600"));

    jwtConsumer = null;
    int jwkConfigured = confIdpConfigUrl != null ? 1 : 0;
    jwkConfigured += confJwkUrl != null ? 1 : 0;
    jwkConfigured += confJwk != null ? 1 : 0;
    if (jwkConfigured > 1) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "JWTAuthPlugin needs to configure exactly one of " +
          PARAM_WELL_KNOWN_URL + ", " + PARAM_JWK_URL + " and " + PARAM_JWK);
    }
    if (jwkConfigured == 0) {
      log.warn("Initialized JWTAuthPlugin without any JWK config. Requests with jwk header will fail.");
    }
    if (oidcDiscoveryConfig != null) {
      String jwkUrl = oidcDiscoveryConfig.getJwksUrl();
      setupJwkUrl(jwkUrl);
    } else if (confJwkUrl != null) {
      setupJwkUrl(confJwkUrl);
    } else if (confJwk != null) {
      try {
        JsonWebKeySet jwks = parseJwkSet(confJwk);
        verificationKeyResolver = new JwksVerificationKeyResolver(jwks.getJsonWebKeys());
      } catch (JoseException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Invalid JWTAuthPlugin configuration, " + PARAM_JWK + " parse error", e);
      }
    }
    initConsumer();
    log.debug("JWK configured");
  }

  private void setupJwkUrl(String url) {
    // The HttpsJwks retrieves and caches keys from a the given HTTPS JWKS endpoint.
    try {
      URL jwkUrl = new URL(url);
      if (!"https".equalsIgnoreCase(jwkUrl.getProtocol())) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, PARAM_JWK_URL + " must be an HTTPS url");
      }
    } catch (MalformedURLException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, PARAM_JWK_URL + " must be a valid URL");
    }
    HttpsJwks httpsJkws = new HttpsJwks(url);
    httpsJkws.setDefaultCacheDuration(jwkCacheDuration);
    verificationKeyResolver = new HttpsJwksVerificationKeyResolver(httpsJkws);
  }

  JsonWebKeySet parseJwkSet(Map<String, Object> jwkObj) throws JoseException {
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

  /**
   * Main authentication method that looks for correct JWT token in the Authorization header
   */
  @Override
  public boolean doAuthenticate(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws Exception {
    HttpServletRequest request = (HttpServletRequest) servletRequest;
    HttpServletResponse response = (HttpServletResponse) servletResponse;
    
    String header = request.getHeader(HttpHeaders.AUTHORIZATION);

    if (jwtConsumer == null) {
      if (header == null && !blockUnknown) {
        log.info("JWTAuth not configured, but allowing anonymous access since {}==false", PARAM_BLOCK_UNKNOWN);
        filterChain.doFilter(request, response);
        numPassThrough.inc();;
        return true;
      }
      // Retry config
      if (lastInitTime.plusSeconds(RETRY_INIT_DELAY_SECONDS).isAfter(Instant.now())) {
        log.info("Retrying JWTAuthPlugin initialization (retry delay={}s)", RETRY_INIT_DELAY_SECONDS);
        init(pluginConfig);
      }
      if (jwtConsumer == null) {
        log.warn("JWTAuth not configured");
        numErrors.mark();
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "JWTAuth plugin not correctly configured");
      }
    }

    JWTAuthenticationResponse authResponse = authenticate(header);
    switch(authResponse.getAuthCode()) {
      case AUTHENTICATED:
        HttpServletRequestWrapper wrapper = new HttpServletRequestWrapper(request) {
          @Override
          public Principal getUserPrincipal() {
            return authResponse.getPrincipal();
          }
        };
        if (!(authResponse.getPrincipal() instanceof JWTPrincipal)) {
          numErrors.mark();
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "JWTAuth plugin says AUTHENTICATED but no token extracted");
        }
        if (log.isDebugEnabled())
          log.debug("Authentication SUCCESS");
        filterChain.doFilter(wrapper, response);
        numAuthenticated.inc();
        return true;

      case PASS_THROUGH:
        if (log.isDebugEnabled())
          log.debug("Unknown user, but allow due to {}=false", PARAM_BLOCK_UNKNOWN);
        filterChain.doFilter(request, response);
        numPassThrough.inc();
        return true;

      case AUTZ_HEADER_PROBLEM:
      case JWT_PARSE_ERROR:
        authenticationFailure(response, authResponse.getAuthCode().getMsg(), HttpServletResponse.SC_BAD_REQUEST, BearerWwwAuthErrorCode.invalid_request);
        numErrors.mark();
        return false;

      case CLAIM_MISMATCH:
      case JWT_EXPIRED:
      case JWT_VALIDATION_EXCEPTION:
      case PRINCIPAL_MISSING:
        if (authResponse.getJwtException() != null) {
          log.warn("Exception: {}", authResponse.getJwtException().getMessage());
        }
        authenticationFailure(response, authResponse.getAuthCode().getMsg(), HttpServletResponse.SC_UNAUTHORIZED, BearerWwwAuthErrorCode.invalid_token);
        numWrongCredentials.inc();
        return false;

      case SCOPE_MISSING:
        authenticationFailure(response, authResponse.getAuthCode().getMsg(), HttpServletResponse.SC_UNAUTHORIZED, BearerWwwAuthErrorCode.insufficient_scope);
        numWrongCredentials.inc();
        return false;
        
      case NO_AUTZ_HEADER:
      default:
        authenticationFailure(response, authResponse.getAuthCode().getMsg(), HttpServletResponse.SC_UNAUTHORIZED, null);
        numMissingCredentials.inc();
        return false;
    }
  }

  /**
   * Testable authentication method
   *
   * @param authorizationHeader the http header "Authentication"
   * @return AuthenticationResponse object
   */
  protected JWTAuthenticationResponse authenticate(String authorizationHeader) {
    if (authorizationHeader != null) {
      StringTokenizer st = new StringTokenizer(authorizationHeader);
      if (st.hasMoreTokens()) {
        String bearer = st.nextToken();
        if (bearer.equalsIgnoreCase("Bearer") && st.hasMoreTokens()) {
          try {
            String jwtCompact = st.nextToken();
            try {
              JwtClaims jwtClaims = jwtConsumer.processToClaims(jwtCompact);
              String principal = jwtClaims.getStringClaimValue(principalClaim);
              if (principal == null || principal.isEmpty()) {
                return new JWTAuthenticationResponse(AuthCode.PRINCIPAL_MISSING, "Cannot identify principal from JWT. Required claim " + principalClaim + " missing. Cannot authenticate");
              }
              if (claimsMatchCompiled != null) {
                for (Map.Entry<String, Pattern> entry : claimsMatchCompiled.entrySet()) {
                  String claim = entry.getKey();
                  if (jwtClaims.hasClaim(claim)) {
                    if (!entry.getValue().matcher(jwtClaims.getStringClaimValue(claim)).matches()) {
                      return new JWTAuthenticationResponse(AuthCode.CLAIM_MISMATCH,
                          "Claim " + claim + "=" + jwtClaims.getStringClaimValue(claim)
                              + " does not match required regular expression " + entry.getValue().pattern());
                    }
                  } else {
                    return new JWTAuthenticationResponse(AuthCode.CLAIM_MISMATCH, "Claim " + claim + " is required but does not exist in JWT");
                  }
                }
              }
              if (!requiredScopes.isEmpty() && !jwtClaims.hasClaim(CLAIM_SCOPE)) {
                // Fail if we require scopes but they don't exist
                return new JWTAuthenticationResponse(AuthCode.CLAIM_MISMATCH, "Claim " + CLAIM_SCOPE + " is required but does not exist in JWT");
              }
              Set<String> scopes = Collections.emptySet();
              Object scopesObj = jwtClaims.getClaimValue(CLAIM_SCOPE);
              if (scopesObj != null) {
                if (scopesObj instanceof String) {
                  scopes = new HashSet<>(Arrays.asList(((String) scopesObj).split("\\s+")));
                } else if (scopesObj instanceof List) {
                  scopes = new HashSet<>(jwtClaims.getStringListClaimValue(CLAIM_SCOPE));
                }
                // Validate that at least one of the required scopes are present in the scope claim 
                if (!requiredScopes.isEmpty()) {
                  if (scopes.stream().noneMatch(requiredScopes::contains)) {
                    return new JWTAuthenticationResponse(AuthCode.SCOPE_MISSING, "Claim " + CLAIM_SCOPE + " does not contain any of the required scopes: " + requiredScopes);
                  }
                }
                final Set<String> finalScopes = new HashSet<>(scopes);
                finalScopes.remove("openid"); // Remove standard scope
                // Pass scopes with principal to signal to any Authorization plugins that user has some verified role claims
                return new JWTAuthenticationResponse(AuthCode.AUTHENTICATED, new JWTPrincipalWithUserRoles(principal, jwtCompact, jwtClaims.getClaimsMap(), finalScopes));
              } else {
                return new JWTAuthenticationResponse(AuthCode.AUTHENTICATED, new JWTPrincipal(principal, jwtCompact, jwtClaims.getClaimsMap()));
              }
            } catch (InvalidJwtException e) {
              // Whether or not the JWT has expired being one common reason for invalidity
              System.out.println("Exception is " + e.getClass().getName() + ", " + e.getMessage() + ", code=" + e.getErrorDetails().get(0).getErrorCode());
              if (e.hasExpired()) {
                return new JWTAuthenticationResponse(AuthCode.JWT_EXPIRED, "Authentication failed due to expired JWT token. Expired at " + e.getJwtContext().getJwtClaims().getExpirationTime());
              }
              if (e.getCause() != null && e.getCause() instanceof JoseException && e.getCause().getMessage().contains("Invalid JOSE Compact Serialization")) {
                return new JWTAuthenticationResponse(AuthCode.JWT_PARSE_ERROR, e.getCause().getMessage());
              }
              return new JWTAuthenticationResponse(AuthCode.JWT_VALIDATION_EXCEPTION, e);
            }
          } catch (MalformedClaimException e) {
            return new JWTAuthenticationResponse(AuthCode.JWT_PARSE_ERROR, "Malformed claim, error was: " + e.getMessage());
          }
        } else {
          return new JWTAuthenticationResponse(AuthCode.AUTZ_HEADER_PROBLEM, "Authorization header is not in correct format");
        }
      } else {
        return new JWTAuthenticationResponse(AuthCode.AUTZ_HEADER_PROBLEM, "Authorization header is not in correct format");
      }
    } else {
      // No Authorization header
      if (blockUnknown) {
        return new JWTAuthenticationResponse(AuthCode.NO_AUTZ_HEADER, "Missing Authorization header");
      } else {
        log.debug("No user authenticated, but blockUnknown=false, so letting request through");
        return new JWTAuthenticationResponse(AuthCode.PASS_THROUGH);
      }
    }
  }

  private void initConsumer() {
    JwtConsumerBuilder jwtConsumerBuilder = new JwtConsumerBuilder()
        .setAllowedClockSkewInSeconds(30); // allow some leeway in validating time based claims to account for clock skew
    if (iss != null)
      jwtConsumerBuilder.setExpectedIssuer(iss); // whom the JWT needs to have been issued by
    if (aud != null) {
      jwtConsumerBuilder.setExpectedAudience(aud); // to whom the JWT is intended for
    } else {
      jwtConsumerBuilder.setSkipDefaultAudienceValidation();
    }
    if (requireSubject)
      jwtConsumerBuilder.setRequireSubject();
    if (requireExpirationTime)
      jwtConsumerBuilder.setRequireExpirationTime();
    if (algWhitelist != null)
      jwtConsumerBuilder.setJwsAlgorithmConstraints( // only allow the expected signature algorithm(s) in the given context
          new AlgorithmConstraints(AlgorithmConstraints.ConstraintType.WHITELIST, algWhitelist.toArray(new String[0])));
    jwtConsumerBuilder.setVerificationKeyResolver(verificationKeyResolver);
    jwtConsumer = jwtConsumerBuilder.build(); // create the JwtConsumer instance
  }

  @Override
  public void close() throws IOException {
    jwtConsumer = null;
  }

  @Override
  public ValidatingJsonMap getSpec() {
    return Utils.getSpec("cluster.security.JwtAuth.Commands").getSpec();
  }

  /**
   * Operate the commands on the latest conf and return a new conf object
   * If there are errors in the commands , throw a SolrException. return a null
   * if no changes are to be made as a result of this edit. It is the responsibility
   * of the implementation to ensure that the returned config is valid . The framework
   * does no validation of the data
   *
   * @param latestConf latest version of config
   * @param commands the list of command operations to perform
   */
  @Override
  public Map<String, Object> edit(Map<String, Object> latestConf, List<CommandOperation> commands) {
    for (CommandOperation command : commands) {
      if (command.name.equals("set-property")) {
        for (Map.Entry<String, Object> e : command.getDataMap().entrySet()) {
          if (PROPS.contains(e.getKey())) {
            latestConf.put(e.getKey(), e.getValue());
            return latestConf;
          } else {
            command.addError("Unknown property " + e.getKey());
          }
        }
      }
    }
    if (!CommandOperation.captureErrors(commands).isEmpty()) return null;
    return latestConf;
  }

  private enum BearerWwwAuthErrorCode { invalid_request, invalid_token, insufficient_scope};

  private void authenticationFailure(HttpServletResponse response, String message, int httpCode, BearerWwwAuthErrorCode responseError) throws IOException {
    List<String> wwwAuthParams = new ArrayList<>();
    wwwAuthParams.add("Bearer realm=\"" + AUTH_REALM + "\"");
    if (responseError != null) {
      wwwAuthParams.add("error=\"" + responseError + "\"");
      wwwAuthParams.add("error_description=\"" + message + "\"");
    }
    response.addHeader(HttpHeaders.WWW_AUTHENTICATE, org.apache.commons.lang.StringUtils.join(wwwAuthParams, ", "));
    response.addHeader(AuthenticationPlugin.HTTP_HEADER_X_SOLR_AUTHDATA, generateAuthDataHeader());
    response.sendError(httpCode, message);
    log.info("JWT Authentication attempt failed: {}", message);
  }

  protected String generateAuthDataHeader() {
    Map<String,Object> data = new HashMap<>();
    data.put(PARAM_AUTHORIZATION_ENDPOINT, authorizationEndpoint);
    data.put("client_id", clientId);
    data.put("scope", adminUiScope);
    data.put("redirect_uris", redirectUris);
    String headerJson = JSONUtil.toJSON(data);
    return Base64.byteArrayToBase64(headerJson.getBytes(StandardCharsets.UTF_8));
  }


  /**
   * Response for authentication attempt
   */
  static class JWTAuthenticationResponse {
    private final Principal principal;
    private String errorMessage;
    private AuthCode authCode;
    private InvalidJwtException jwtException;
  
    enum AuthCode {
      PASS_THROUGH("No user, pass through"),             // Returned when no user authentication but block_unknown=false 
      AUTHENTICATED("Authenticated"),                    // Returned when authentication OK 
      PRINCIPAL_MISSING("No principal in JWT"),          // JWT token does not contain necessary principal (typically sub)  
      JWT_PARSE_ERROR("Invalid JWT"),                    // Problems with parsing the JWT itself 
      AUTZ_HEADER_PROBLEM("Wrong header"),               // The Authorization header exists but is not correct 
      NO_AUTZ_HEADER("Require authentication"),          // The Authorization header is missing 
      JWT_EXPIRED("JWT token expired"),                  // JWT token has expired 
      CLAIM_MISMATCH("Required JWT claim missing"),      // Some required claims are missing or wrong 
      JWT_VALIDATION_EXCEPTION("JWT validation failed"), // The JWT parser failed validation. More details in exception
      SCOPE_MISSING("Required scope missing in JWT");    // None of the required scopes were present in JWT
  
      public String getMsg() {
        return msg;
      }
  
      private final String msg;
  
      AuthCode(String msg) {
        this.msg = msg;
      }
    }
  
    JWTAuthenticationResponse(AuthCode authCode, InvalidJwtException e) {
      this.authCode = authCode;
      this.jwtException = e;
      principal = null;
      this.errorMessage = e.getMessage();
    }
    
    JWTAuthenticationResponse(AuthCode authCode, String errorMessage) {
      this.authCode = authCode;
      this.errorMessage = errorMessage;
      principal = null;
    }
  
    JWTAuthenticationResponse(AuthCode authCode, Principal principal) {
      this.authCode = authCode;
      this.principal = principal;
    }
    
    JWTAuthenticationResponse(AuthCode authCode) {
      this.authCode = authCode;
      principal = null;
    }
  
    boolean isAuthenticated() {
      return authCode.equals(AuthCode.AUTHENTICATED);
    }
  
    public Principal getPrincipal() {
      return principal;
    }
  
    String getErrorMessage() {
      return errorMessage;
    }
  
    InvalidJwtException getJwtException() {
      return jwtException;
    }
  
    AuthCode getAuthCode() {
      return authCode;
    }
  }

  /**
   * Config object for a OpenId Connect well-known config
   * Typically exposed through /.well-known/openid-configuration endpoint 
   */
  public static class WellKnownDiscoveryConfig {
    private static Map<String, Object> securityConf;
  
    WellKnownDiscoveryConfig(Map<String, Object> securityConf) {
      WellKnownDiscoveryConfig.securityConf = securityConf;
    }
  
    public static WellKnownDiscoveryConfig parse(String urlString) {
      try {
        URL url = new URL(urlString);
        if (!Arrays.asList("https", "file").contains(url.getProtocol())) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Well-known config URL must be HTTPS or file");
        }
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
  
    public static WellKnownDiscoveryConfig parse(InputStream configStream) {
      securityConf = (Map<String, Object>) Utils.fromJSON(configStream);
      return new WellKnownDiscoveryConfig(securityConf);
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

    public List<String> getScopesSupported() {
      return (List<String>) securityConf.get("scopes_supported");
    }

    public List<String> getResponseTypesSupported() {
      return (List<String>) securityConf.get("response_types_supported");
    }
  }

  @Override
  protected boolean interceptInternodeRequest(HttpRequest httpRequest, HttpContext httpContext) {
    if (httpContext instanceof HttpClientContext) {
      HttpClientContext httpClientContext = (HttpClientContext) httpContext;
      if (httpClientContext.getUserToken() instanceof JWTPrincipal) {
        JWTPrincipal jwtPrincipal = (JWTPrincipal) httpClientContext.getUserToken();
        httpRequest.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + jwtPrincipal.getToken());
        return true;
      }
    }
    return false;
  }

  @Override
  protected boolean interceptInternodeRequest(Request request) {
    Object userToken = request.getAttributes().get(Http2SolrClient.REQ_PRINCIPAL_KEY);
    if (userToken instanceof JWTPrincipal) {
      JWTPrincipal jwtPrincipal = (JWTPrincipal) userToken;
      request.header(HttpHeaders.AUTHORIZATION, "Bearer " + jwtPrincipal.getToken());
      return true;
    }
    return false;
  }
}
