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
import java.util.Objects;
import java.util.Optional;
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
import org.jose4j.jwt.consumer.InvalidJwtSignatureException;
import org.jose4j.jwt.consumer.JwtConsumer;
import org.jose4j.jwt.consumer.JwtConsumerBuilder;
import org.jose4j.lang.JoseException;
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
  private static final String PARAM_REQUIRE_ISSUER = "requireIss";
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
  private static final String PARAM_ISSUERS = "issuers";
  private static final String PARAM_ISS_NAME = "name";

  private static final String AUTH_REALM = "solr-jwt";
  private static final String CLAIM_SCOPE = "scope";
  private static final long RETRY_INIT_DELAY_SECONDS = 30;
  private static final long DEFAULT_REFRESH_REPRIEVE_THRESHOLD = 5000;

  private static final Set<String> PROPS = ImmutableSet.of(PARAM_BLOCK_UNKNOWN, PARAM_JWK_URL, PARAM_JWK, PARAM_ISSUER,
      PARAM_AUDIENCE, PARAM_REQUIRE_SUBJECT, PARAM_PRINCIPAL_CLAIM, PARAM_REQUIRE_EXPIRATIONTIME, PARAM_ALG_WHITELIST,
      PARAM_JWK_CACHE_DURATION, PARAM_CLAIMS_MATCH, PARAM_SCOPE, PARAM_CLIENT_ID, PARAM_WELL_KNOWN_URL, PARAM_ISS_NAME,
      PARAM_AUTHORIZATION_ENDPOINT, PARAM_ADMINUI_SCOPE, PARAM_REDIRECT_URIS, PARAM_REQUIRE_ISSUER, PARAM_ISSUERS);
  private static final String PRIMARY_ISSUER = "PRIMARY";

  private JwtConsumer jwtConsumer;
  private boolean requireExpirationTime;
  private List<String> algWhitelist;
  private String principalClaim;
  private HashMap<String, Pattern> claimsMatchCompiled;
  private boolean blockUnknown;
  private List<String> requiredScopes = new ArrayList<>();
  private Map<String, Object> pluginConfig;
  private Instant lastInitTime = Instant.now();
  private String adminUiScope;
  private List<String> redirectUris;
  private List<IssuerConfig> issuerConfigs;
  private boolean requireIssuer;
  private JWTVerificationkeyResolver verificationKeyResolver;

  /**
   * Initialize plugin
   */
  public JWTAuthPlugin() {}

  @SuppressWarnings("unchecked")
  @Override
  public void init(Map<String, Object> pluginConfig) {
    this.pluginConfig = pluginConfig;
    this.issuerConfigs = null;
    List<String> unknownKeys = pluginConfig.keySet().stream().filter(k -> !PROPS.contains(k)).collect(Collectors.toList());
    unknownKeys.remove("class");
    unknownKeys.remove("");
    if (!unknownKeys.isEmpty()) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Invalid JwtAuth configuration parameter " + unknownKeys); 
    }

    blockUnknown = Boolean.parseBoolean(String.valueOf(pluginConfig.getOrDefault(PARAM_BLOCK_UNKNOWN, false)));
    requireIssuer = Boolean.parseBoolean(String.valueOf(pluginConfig.getOrDefault(PARAM_REQUIRE_ISSUER, "true")));
    requireExpirationTime = Boolean.parseBoolean(String.valueOf(pluginConfig.getOrDefault(PARAM_REQUIRE_EXPIRATIONTIME, "true")));
    if (pluginConfig.get(PARAM_REQUIRE_SUBJECT) != null) {
      log.warn("Parameter {} is no longer used and may generate error in a later version. A subject claim is now always required",
          PARAM_REQUIRE_SUBJECT);
    }
    principalClaim = (String) pluginConfig.getOrDefault(PARAM_PRINCIPAL_CLAIM, "sub");
    Object redirectUrisObj = pluginConfig.get(PARAM_REDIRECT_URIS);
    redirectUris = Collections.emptyList();
    if (redirectUrisObj != null) {
      if (redirectUrisObj instanceof String) {
        redirectUris = Collections.singletonList((String) redirectUrisObj);
      } else if (redirectUrisObj instanceof List) {
        redirectUris = (List<String>) redirectUrisObj;
      }
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

    long jwkCacheDuration = Long.parseLong((String) pluginConfig.getOrDefault(PARAM_JWK_CACHE_DURATION, "3600"));
    IssuerConfig.setHttpsJwksFactory(new HttpsJwksFactory(jwkCacheDuration, DEFAULT_REFRESH_REPRIEVE_THRESHOLD));

    issuerConfigs = new ArrayList<>();

    // Try to parse an issuer from top level config, and add first (primary issuer)
    Optional<IssuerConfig> topLevelIssuer = parseIssuerFromTopLevelConfig(pluginConfig);
    topLevelIssuer.ifPresent(ic -> {
      issuerConfigs.add(ic);
    });

    // Add issuers from 'issuers' key
    issuerConfigs.addAll(parseIssuers(pluginConfig));
    verificationKeyResolver = new JWTVerificationkeyResolver(issuerConfigs, requireIssuer);

    initConsumer();

    lastInitTime = Instant.now();
  }

  @SuppressWarnings("unchecked")
  private Optional<IssuerConfig> parseIssuerFromTopLevelConfig(Map<String, Object> pluginConfig) {
    try {
      IssuerConfig primary = new IssuerConfig(PRIMARY_ISSUER)
          .setIss((String) pluginConfig.get(PARAM_ISSUER))
          .setAud((String) pluginConfig.get(PARAM_AUDIENCE))
          .setJwksUrl(pluginConfig.get(PARAM_JWK_URL))
          .setAuthorizationEndpoint((String) pluginConfig.get(PARAM_AUTHORIZATION_ENDPOINT))
          .setClientId((String) pluginConfig.get(PARAM_CLIENT_ID))
          .setWellKnownUrl((String) pluginConfig.get(PARAM_WELL_KNOWN_URL));
      if (pluginConfig.get(PARAM_JWK) != null) {
        primary.setJwks(IssuerConfig.parseJwkSet((Map<String, Object>) pluginConfig.get(PARAM_JWK)));
      }
      if (primary.isValid()) {
        log.debug("Found issuer in top level config");
        primary.init();
        return Optional.of(primary);
      } else {
        log.debug("No issuer configured in top level config");
        return Optional.empty();
      }
    } catch (JoseException je) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Failed parsing issuer from top level config", je);
    }
  }

  IssuerConfig getPrimaryIssuer() {
    if (issuerConfigs.size() == 0) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "No issuers configured");
    }
    return issuerConfigs.get(0);
  }

  /**
   * Initialize optional additional issuers configured in 'issuers' condfig map
   * @param pluginConfig the main config object
   * @return a list of parsed {@link IssuerConfig} objects
   */
  @SuppressWarnings("unchecked")
  List<IssuerConfig> parseIssuers(Map<String, Object> pluginConfig) {
    List<IssuerConfig> configs = new ArrayList<>();
    try {
      List<Map<String, Object>> issuers = (List<Map<String, Object>>) pluginConfig.get(PARAM_ISSUERS);
      if (issuers != null) {
        issuers.forEach(issuerConf -> {
          IssuerConfig ic = new IssuerConfig(issuerConf);
          ic.init();
          configs.add(ic);
          log.debug("Found issuer with name {} and issuerId {}", ic.getName(), ic.getIss());
        });
      }
      return configs;
    } catch(ClassCastException cce) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Parameter " + PARAM_ISSUERS + " has wrong format.", cce);
    }
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
        numPassThrough.inc();
        filterChain.doFilter(request, response);
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
    if (AuthCode.SIGNATURE_INVALID.equals(authResponse.getAuthCode())) {
      String issuer = jwtConsumer.processToClaims(header).getIssuer();
      if (issuer != null) {
        Optional<IssuerConfig> issuerConfig = issuerConfigs.stream().filter(ic -> issuer.equals(ic.getIss())).findFirst();
        if (issuerConfig.isPresent() && issuerConfig.get().usesHttpsJwk()) {
          log.warn("Signature validation failed for issuer {}. Refreshing JWKs from IdP before trying again: {}",
              issuer, authResponse.getJwtException() == null ? "" : authResponse.getJwtException().getMessage());
          for (HttpsJwks httpsJwks : issuerConfig.get().getHttpsJwks()) {
            httpsJwks.refresh();
          }
        }
      }
      authResponse = authenticate(header);
    }
    String exceptionMessage = authResponse.getJwtException() != null ? authResponse.getJwtException().getMessage() : "";

    switch (authResponse.getAuthCode()) {
      case AUTHENTICATED:
        final Principal principal = authResponse.getPrincipal();
        HttpServletRequestWrapper wrapper = new HttpServletRequestWrapper(request) {
          @Override
          public Principal getUserPrincipal() {
            return principal;
          }
        };
        if (!(principal instanceof JWTPrincipal)) {
          numErrors.mark();
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "JWTAuth plugin says AUTHENTICATED but no token extracted");
        }
        if (log.isDebugEnabled())
          log.debug("Authentication SUCCESS");
        numAuthenticated.inc();
        filterChain.doFilter(wrapper, response);
        return true;

      case PASS_THROUGH:
        if (log.isDebugEnabled())
          log.debug("Unknown user, but allow due to {}=false", PARAM_BLOCK_UNKNOWN);
        numPassThrough.inc();
        filterChain.doFilter(request, response);
        return true;

      case AUTZ_HEADER_PROBLEM:
      case JWT_PARSE_ERROR:
        log.warn("Authentication failed. {}, {}", authResponse.getAuthCode(), authResponse.getAuthCode().getMsg());
        numErrors.mark();
        authenticationFailure(response, authResponse.getAuthCode().getMsg(), HttpServletResponse.SC_BAD_REQUEST, BearerWwwAuthErrorCode.invalid_request);
        return false;

      case CLAIM_MISMATCH:
      case JWT_EXPIRED:
      case JWT_VALIDATION_EXCEPTION:
      case PRINCIPAL_MISSING:
        log.warn("Authentication failed. {}, {}", authResponse.getAuthCode(), exceptionMessage);
        numWrongCredentials.inc();
        authenticationFailure(response, authResponse.getAuthCode().getMsg(), HttpServletResponse.SC_UNAUTHORIZED, BearerWwwAuthErrorCode.invalid_token);
        return false;

      case SIGNATURE_INVALID:
        log.warn("Signature validation failed: {}", exceptionMessage);
        numWrongCredentials.inc();
        authenticationFailure(response, authResponse.getAuthCode().getMsg(), HttpServletResponse.SC_UNAUTHORIZED, BearerWwwAuthErrorCode.invalid_token);
        return false;

      case SCOPE_MISSING:
        numWrongCredentials.inc();
        authenticationFailure(response, authResponse.getAuthCode().getMsg(), HttpServletResponse.SC_UNAUTHORIZED, BearerWwwAuthErrorCode.insufficient_scope);
        return false;

      case NO_AUTZ_HEADER:
      default:
        numMissingCredentials.inc();
        authenticationFailure(response, authResponse.getAuthCode().getMsg(), HttpServletResponse.SC_UNAUTHORIZED, null);
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
      String jwtCompact = parseAuthorizationHeader(authorizationHeader);
      if (jwtCompact != null) {
        try {
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
          } catch (InvalidJwtSignatureException ise) {
            return new JWTAuthenticationResponse(AuthCode.SIGNATURE_INVALID, ise);
          } catch (InvalidJwtException e) {
            // Whether or not the JWT has expired being one common reason for invalidity
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
      // No Authorization header
      if (blockUnknown) {
        return new JWTAuthenticationResponse(AuthCode.NO_AUTZ_HEADER, "Missing Authorization header");
      } else {
        log.debug("No user authenticated, but blockUnknown=false, so letting request through");
        return new JWTAuthenticationResponse(AuthCode.PASS_THROUGH);
      }
    }
  }

  private String parseAuthorizationHeader(String authorizationHeader) {
    StringTokenizer st = new StringTokenizer(authorizationHeader);
    if (st.hasMoreTokens()) {
      String bearer = st.nextToken();
      if (bearer.equalsIgnoreCase("Bearer") && st.hasMoreTokens()) {
        return st.nextToken();
      }
    }
    return null;
  }

  private void initConsumer() {
    JwtConsumerBuilder jwtConsumerBuilder = new JwtConsumerBuilder()
        .setAllowedClockSkewInSeconds(30); // allow some leeway in validating time based claims to account for clock skew
    String[] issuers = issuerConfigs.stream().map(IssuerConfig::getIss).filter(Objects::nonNull).toArray(String[]::new);
    if (issuers.length > 0) {
      jwtConsumerBuilder.setExpectedIssuers(requireIssuer, issuers); // whom the JWT needs to have been issued by
    }
    String[] audiences = issuerConfigs.stream().map(IssuerConfig::getAud).filter(Objects::nonNull).toArray(String[]::new);
    if (audiences.length > 0) {
      jwtConsumerBuilder.setExpectedAudience(audiences); // to whom the JWT is intended for
    } else {
      jwtConsumerBuilder.setSkipDefaultAudienceValidation();
    }
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

  private enum BearerWwwAuthErrorCode { invalid_request, invalid_token, insufficient_scope}

  private void authenticationFailure(HttpServletResponse response, String message, int httpCode, BearerWwwAuthErrorCode responseError) throws IOException {
    List<String> wwwAuthParams = new ArrayList<>();
    wwwAuthParams.add("Bearer realm=\"" + AUTH_REALM + "\"");
    if (responseError != null) {
      wwwAuthParams.add("error=\"" + responseError + "\"");
      wwwAuthParams.add("error_description=\"" + message + "\"");
    }
    response.addHeader(HttpHeaders.WWW_AUTHENTICATE, String.join(", ", wwwAuthParams));
    response.addHeader(AuthenticationPlugin.HTTP_HEADER_X_SOLR_AUTHDATA, generateAuthDataHeader());
    response.sendError(httpCode, message);
    log.info("JWT Authentication attempt failed: {}", message);
  }

  protected String generateAuthDataHeader() {
    IssuerConfig primaryIssuer = getPrimaryIssuer();
    Map<String,Object> data = new HashMap<>();
    data.put(PARAM_AUTHORIZATION_ENDPOINT, primaryIssuer.getAuthorizationEndpoint());
    data.put("client_id", primaryIssuer.getClientId());
    data.put("scope", adminUiScope);
    data.put("redirect_uris", redirectUris);
    String headerJson = Utils.toJSONString(data);
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
      SCOPE_MISSING("Required scope missing in JWT"),    // None of the required scopes were present in JWT
      SIGNATURE_INVALID("Signature invalid");            // Validation of JWT signature failed

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
    private Map<String, Object> securityConf;
  
    WellKnownDiscoveryConfig(Map<String, Object> securityConf) {
      this.securityConf = securityConf;
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

  /**
   * Holds information about an IdP (issuer), such as issuer ID, JWK url(s), keys etc
   */
  public static class IssuerConfig {
    private static HttpsJwksFactory httpsJwksFactory = new HttpsJwksFactory(3600, DEFAULT_REFRESH_REPRIEVE_THRESHOLD);
    private String iss;
    private String aud;
    private JsonWebKeySet jsonWebKeySet;
    private String name;
    private Map<String, Object> configMap = null;
    private List<String> jwksUrl;
    private List<HttpsJwks> httpsJwks;
    private String wellKnownUrl;
    private WellKnownDiscoveryConfig wellKnownDiscoveryConfig;
    private String clientId;
    private String authorizationEndpoint;

    /**
     * Create config for further configuration with setters.
     * Once all values are set, call {@link #init(Map)} before further use
     *
     * @param name a unique name for this issuer
     */
    public IssuerConfig(String name) {
      this.name = name;
    }

    /**
     * Initialize issuer config from a generic configuration map
     *
     * @param configMap map of configuration keys anv values
     */
    public IssuerConfig(Map<String, Object> configMap) {
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
      if (iss == null && !PRIMARY_ISSUER.equals(name) && !usesHttpsJwk()) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Missing required config 'iss' for issuer " + getName());
      }
    }

    /**
     * Parses configuration for one IssuerConfig
     */
    @SuppressWarnings("unchecked")
    void parseConfigMap(Map<String, Object> configMap) {
      HashMap<String, Object> conf = new HashMap<>(configMap); // Clone
      setName((String) conf.get(PARAM_ISS_NAME));
      setWellKnownUrl((String) conf.get(PARAM_WELL_KNOWN_URL));
      setIss((String) conf.get(PARAM_ISSUER));
      setClientId((String) conf.get(PARAM_CLIENT_ID));
      setAud((String) conf.get(PARAM_AUDIENCE));
      setJwksUrl(conf.get(PARAM_JWK_URL));
      setJwks(conf.get(PARAM_JWK));

      conf.remove(PARAM_WELL_KNOWN_URL);
      conf.remove(PARAM_ISSUER);
      conf.remove(PARAM_ISS_NAME);
      conf.remove(PARAM_CLIENT_ID);
      conf.remove(PARAM_AUDIENCE);
      conf.remove(PARAM_JWK_URL);
      conf.remove(PARAM_JWK);

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

    private WellKnownDiscoveryConfig fetchWellKnown(String wellKnownUrl) {
      return WellKnownDiscoveryConfig.parse(wellKnownUrl);
    }

    public String getIss() {
      return iss;
    }

    public IssuerConfig setIss(String iss) {
      this.iss = iss;
      return this;
    }

    public String getName() {
      return name;
    }

    public IssuerConfig setName(String name) {
      this.name = name;
      return this;
    }

    public String getWellKnownUrl() {
      return wellKnownUrl;
    }

    public IssuerConfig setWellKnownUrl(String wellKnownUrl) {
      this.wellKnownUrl = wellKnownUrl;
      return this;
    }

    public List<String> getJwksUrls() {
      return jwksUrl;
    }

    public IssuerConfig setJwksUrl(List<String> jwksUrl) {
      this.jwksUrl = jwksUrl;
      return this;
    }

    @SuppressWarnings("unchecked")
    public IssuerConfig setJwksUrl(Object jwksUrlListOrString) {
      if (jwksUrlListOrString instanceof String)
        this.jwksUrl = Collections.singletonList((String) jwksUrlListOrString);
      else if (jwksUrlListOrString instanceof List)
        this.jwksUrl = (List<String>) jwksUrlListOrString;
      else if (jwksUrlListOrString != null)
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Parameter " + PARAM_JWK_URL + " must be either List or String");
      return this;
    }

    public List<HttpsJwks> getHttpsJwks() {
      if (httpsJwks == null) {
        httpsJwks = httpsJwksFactory.createList(getJwksUrls());
      }
      return httpsJwks;
    }

    public IssuerConfig setHttpsJwks(List<HttpsJwks> httpsJwks) {
      this.httpsJwks = httpsJwks;
      return this;
    }

    /**
     * Set the factory to use when creating HttpsJwks objects
     * @param httpsJwksFactory factory with custom settings
     */
    public static void setHttpsJwksFactory(HttpsJwksFactory httpsJwksFactory) {
      IssuerConfig.httpsJwksFactory = httpsJwksFactory;
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

    public WellKnownDiscoveryConfig getWellKnownDiscoveryConfig() {
      return wellKnownDiscoveryConfig;
    }

    public String getAud() {
      return aud;
    }

    public IssuerConfig setAud(String aud) {
      this.aud = aud;
      return this;
    }

    public IssuerConfig setJwks(JsonWebKeySet jsonWebKeySet) {
      this.jsonWebKeySet = jsonWebKeySet;
      return this;
    }

    public String getClientId() {
      return clientId;
    }

    public IssuerConfig setClientId(String clientId) {
      this.clientId = clientId;
      return this;
    }

    public String getAuthorizationEndpoint() {
      return authorizationEndpoint;
    }

    public IssuerConfig setAuthorizationEndpoint(String authorizationEndpoint) {
      this.authorizationEndpoint = authorizationEndpoint;
      return this;
    }

    public Map<String,Object> asConfig() {
      HashMap<String,Object> config = new HashMap<>();
      putIfNotNull(config, PARAM_ISS_NAME, name);
      putIfNotNull(config, PARAM_ISSUER, iss);
      putIfNotNull(config, PARAM_AUDIENCE, aud);
      putIfNotNull(config, PARAM_JWK_URL, jwksUrl);
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
            PARAM_WELL_KNOWN_URL + ", " + PARAM_JWK_URL + " and " + PARAM_JWK);
      }
      if (jwkConfigured > 0 && name == null) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Parameter 'name' is required for issuer configurations");
      }
      return jwkConfigured > 0;
    }
  }

  public static class HttpsJwksFactory {
    private final long jwkCacheDuration;
    private final long refreshReprieveThreshold;

    public HttpsJwksFactory(long jwkCacheDuration, long refreshReprieveThreshold) {
      this.jwkCacheDuration = jwkCacheDuration;
      this.refreshReprieveThreshold = refreshReprieveThreshold;
    }

    public HttpsJwks create(String url) {
      try {
        URL jwkUrl = new URL(url);
        if (!"https".equalsIgnoreCase(jwkUrl.getProtocol())) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, PARAM_JWK_URL + " must use HTTPS");
        }
      } catch (MalformedURLException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Url " + url + " configured in " + PARAM_JWK_URL + " is not a valid URL");
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

  public JWTVerificationkeyResolver getVerificationKeyResolver() {
    return verificationKeyResolver;
  }

  public List<IssuerConfig> getIssuerConfigs() {
    return issuerConfigs;
  }

  public Optional<IssuerConfig> getIssuerConfigByName(String name) {
    return issuerConfigs.stream().filter(ic -> name.equals(ic.getName())).findAny();
  }
}
