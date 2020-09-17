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
import java.io.IOException;
import java.lang.invoke.MethodHandles;
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
  private static final String PARAM_REQUIRE_ISSUER = "requireIss";
  private static final String PARAM_PRINCIPAL_CLAIM = "principalClaim";
  private static final String PARAM_ROLES_CLAIM = "rolesClaim";
  private static final String PARAM_REQUIRE_EXPIRATIONTIME = "requireExp";
  private static final String PARAM_ALG_WHITELIST = "algWhitelist";
  private static final String PARAM_JWK_CACHE_DURATION = "jwkCacheDur";
  private static final String PARAM_CLAIMS_MATCH = "claimsMatch";
  private static final String PARAM_SCOPE = "scope";
  private static final String PARAM_ADMINUI_SCOPE = "adminUiScope";
  private static final String PARAM_REDIRECT_URIS = "redirectUris";
  private static final String PARAM_ISSUERS = "issuers";
  private static final String PARAM_REALM = "realm";

  private static final String DEFAULT_AUTH_REALM = "solr-jwt";
  private static final String CLAIM_SCOPE = "scope";
  private static final long RETRY_INIT_DELAY_SECONDS = 30;
  private static final long DEFAULT_REFRESH_REPRIEVE_THRESHOLD = 5000;
  static final String PRIMARY_ISSUER = "PRIMARY";

  private static final Set<String> PROPS = ImmutableSet.of(PARAM_BLOCK_UNKNOWN,
      PARAM_PRINCIPAL_CLAIM, PARAM_REQUIRE_EXPIRATIONTIME, PARAM_ALG_WHITELIST,
      PARAM_JWK_CACHE_DURATION, PARAM_CLAIMS_MATCH, PARAM_SCOPE, PARAM_REALM, PARAM_ROLES_CLAIM,
      PARAM_ADMINUI_SCOPE, PARAM_REDIRECT_URIS, PARAM_REQUIRE_ISSUER, PARAM_ISSUERS,
      // These keys are supported for now to enable PRIMARY issuer config through top-level keys
      JWTIssuerConfig.PARAM_JWK_URL, JWTIssuerConfig.PARAM_JWKS_URL, JWTIssuerConfig.PARAM_JWK, JWTIssuerConfig.PARAM_ISSUER,
      JWTIssuerConfig.PARAM_CLIENT_ID, JWTIssuerConfig.PARAM_WELL_KNOWN_URL, JWTIssuerConfig.PARAM_AUDIENCE,
      JWTIssuerConfig.PARAM_AUTHORIZATION_ENDPOINT);

  private JwtConsumer jwtConsumer;
  private boolean requireExpirationTime;
  private List<String> algWhitelist;
  private String principalClaim;
  private String rolesClaim;
  private HashMap<String, Pattern> claimsMatchCompiled;
  private boolean blockUnknown;
  private List<String> requiredScopes = new ArrayList<>();
  private Map<String, Object> pluginConfig;
  private Instant lastInitTime = Instant.now();
  private String adminUiScope;
  private List<String> redirectUris;
  private List<JWTIssuerConfig> issuerConfigs;
  private boolean requireIssuer;
  private JWTVerificationkeyResolver verificationKeyResolver;
  String realm;

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
    principalClaim = (String) pluginConfig.getOrDefault(PARAM_PRINCIPAL_CLAIM, "sub");

    rolesClaim = (String) pluginConfig.get(PARAM_ROLES_CLAIM);
    algWhitelist = (List<String>) pluginConfig.get(PARAM_ALG_WHITELIST);
    realm = (String) pluginConfig.getOrDefault(PARAM_REALM, DEFAULT_AUTH_REALM);

    Map<String, String> claimsMatch = (Map<String, String>) pluginConfig.get(PARAM_CLAIMS_MATCH);
    claimsMatchCompiled = new HashMap<>();
    if (claimsMatch != null) {
      for (Map.Entry<String, String> entry : claimsMatch.entrySet()) {
        claimsMatchCompiled.put(entry.getKey(), Pattern.compile(entry.getValue()));
      }
    }

    String requiredScopesStr = (String) pluginConfig.get(PARAM_SCOPE);
    if (!StringUtils.isEmpty(requiredScopesStr)) {
      requiredScopes = Arrays.asList(requiredScopesStr.split("\\s+"));
    }

    long jwkCacheDuration = Long.parseLong((String) pluginConfig.getOrDefault(PARAM_JWK_CACHE_DURATION, "3600"));
    JWTIssuerConfig.setHttpsJwksFactory(new JWTIssuerConfig.HttpsJwksFactory(jwkCacheDuration, DEFAULT_REFRESH_REPRIEVE_THRESHOLD));

    issuerConfigs = new ArrayList<>();

    // Try to parse an issuer from top level config, and add first (primary issuer)
    Optional<JWTIssuerConfig> topLevelIssuer = parseIssuerFromTopLevelConfig(pluginConfig);
    topLevelIssuer.ifPresent(ic -> {
      issuerConfigs.add(ic);
      log.warn("JWTAuthPlugin issuer is configured using top-level configuration keys. Please consider using the 'issuers' array instead.");
    });

    // Add issuers from 'issuers' key
    issuerConfigs.addAll(parseIssuers(pluginConfig));
    verificationKeyResolver = new JWTVerificationkeyResolver(issuerConfigs, requireIssuer);

    if (issuerConfigs.size() > 0 && getPrimaryIssuer().getAuthorizationEndpoint() != null) {
      adminUiScope = (String) pluginConfig.get(PARAM_ADMINUI_SCOPE);
      if (adminUiScope == null && requiredScopes.size() > 0) {
        adminUiScope = requiredScopes.get(0);
        log.warn("No adminUiScope given, using first scope in 'scope' list as required scope for accessing Admin UI");
      }

      if (adminUiScope == null) {
        adminUiScope = "solr";
        log.info("No adminUiScope provided, fallback to 'solr' as required scope for Admin UI login may not work");
      }

      Object redirectUrisObj = pluginConfig.get(PARAM_REDIRECT_URIS);
      redirectUris = Collections.emptyList();
      if (redirectUrisObj != null) {
        if (redirectUrisObj instanceof String) {
          redirectUris = Collections.singletonList((String) redirectUrisObj);
        } else if (redirectUrisObj instanceof List) {
          redirectUris = (List<String>) redirectUrisObj;
        }
      }
    }

    initConsumer();

    lastInitTime = Instant.now();
  }

  @SuppressWarnings("unchecked")
  private Optional<JWTIssuerConfig> parseIssuerFromTopLevelConfig(Map<String, Object> conf) {
    try {
      if (conf.get(JWTIssuerConfig.PARAM_JWK_URL) != null) {
        log.warn("Configuration uses deprecated key {}. Please use {} instead", JWTIssuerConfig.PARAM_JWK_URL, JWTIssuerConfig.PARAM_JWKS_URL);
      }
      JWTIssuerConfig primary = new JWTIssuerConfig(PRIMARY_ISSUER)
          .setIss((String) conf.get(JWTIssuerConfig.PARAM_ISSUER))
          .setAud((String) conf.get(JWTIssuerConfig.PARAM_AUDIENCE))
          .setJwksUrl(conf.get(JWTIssuerConfig.PARAM_JWKS_URL) != null ? conf.get(JWTIssuerConfig.PARAM_JWKS_URL) : conf.get(JWTIssuerConfig.PARAM_JWK_URL))
          .setAuthorizationEndpoint((String) conf.get(JWTIssuerConfig.PARAM_AUTHORIZATION_ENDPOINT))
          .setClientId((String) conf.get(JWTIssuerConfig.PARAM_CLIENT_ID))
          .setWellKnownUrl((String) conf.get(JWTIssuerConfig.PARAM_WELL_KNOWN_URL));
      if (conf.get(JWTIssuerConfig.PARAM_JWK) != null) {
        primary.setJsonWebKeySet(JWTIssuerConfig.parseJwkSet((Map<String, Object>) conf.get(JWTIssuerConfig.PARAM_JWK)));
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

  /**
   * Fetch the primary issuer to be used for Admin UI authentication. Callers of this method must ensure that at least
   * one issuer is configured. The primary issuer is defined as the first issuer configured in the list.
   * @return JWTIssuerConfig object for the primary issuer
   */
  JWTIssuerConfig getPrimaryIssuer() {
    if (issuerConfigs.size() == 0) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "No issuers configured");
    }
    return issuerConfigs.get(0);
  }

  /**
   * Initialize optional additional issuers configured in 'issuers' config map
   * @param pluginConfig the main config object
   * @return a list of parsed {@link JWTIssuerConfig} objects
   */
  @SuppressWarnings("unchecked")
  List<JWTIssuerConfig> parseIssuers(Map<String, Object> pluginConfig) {
    List<JWTIssuerConfig> configs = new ArrayList<>();
    try {
      List<Map<String, Object>> issuers = (List<Map<String, Object>>) pluginConfig.get(PARAM_ISSUERS);
      if (issuers != null) {
        issuers.forEach(issuerConf -> {
          JWTIssuerConfig ic = new JWTIssuerConfig(issuerConf);
          ic.init();
          configs.add(ic);
          if (log.isDebugEnabled()) {
            log.debug("Found issuer with name {} and issuerId {}", ic.getName(), ic.getIss());
          }
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
    String exceptionMessage = authResponse.getJwtException() != null ? authResponse.getJwtException().getMessage() : "";
    if (AuthCode.SIGNATURE_INVALID.equals(authResponse.getAuthCode())) {
      String issuer = jwtConsumer.processToClaims(header).getIssuer();
      if (issuer != null) {
        Optional<JWTIssuerConfig> issuerConfig = issuerConfigs.stream().filter(ic -> issuer.equals(ic.getIss())).findFirst();
        if (issuerConfig.isPresent() && issuerConfig.get().usesHttpsJwk()) {
          log.info("Signature validation failed for issuer {}. Refreshing JWKs from IdP before trying again: {}",
              issuer, exceptionMessage);
          for (HttpsJwks httpsJwks : issuerConfig.get().getHttpsJwks()) {
            httpsJwks.refresh();
          }
          authResponse = authenticate(header); // Retry
          exceptionMessage = authResponse.getJwtException() != null ? authResponse.getJwtException().getMessage() : "";
        }
      }
    }

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
        request.setAttribute(AuthenticationPlugin.class.getName(), getPromptHeaders(null, null));
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

            // Find scopes for user
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
            }

            // Determine roles of user, either from 'rolesClaim' or from 'scope' as parsed above
            final Set<String> finalRoles = new HashSet<>();
            if (rolesClaim == null) {
              // Pass scopes with principal to signal to any Authorization plugins that user has some verified role claims
              finalRoles.addAll(scopes);
              finalRoles.remove("openid"); // Remove standard scope
            } else {
              // Pull roles from separate claim, either as whitespace separated list or as JSON array
              Object rolesObj = jwtClaims.getClaimValue(rolesClaim);
              if (rolesObj != null) {
                if (rolesObj instanceof String) {
                  finalRoles.addAll(Arrays.asList(((String) rolesObj).split("\\s+")));
                } else if (rolesObj instanceof List) {
                  finalRoles.addAll(jwtClaims.getStringListClaimValue(rolesClaim));
                }
              }
            }
            if (finalRoles.size() > 0) {
              return new JWTAuthenticationResponse(AuthCode.AUTHENTICATED, new JWTPrincipalWithUserRoles(principal, jwtCompact, jwtClaims.getClaimsMap(), finalRoles));
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
    String[] issuers = issuerConfigs.stream().map(JWTIssuerConfig::getIss).filter(Objects::nonNull).toArray(String[]::new);
    if (issuers.length > 0) {
      jwtConsumerBuilder.setExpectedIssuers(requireIssuer, issuers); // whom the JWT needs to have been issued by
    }
    String[] audiences = issuerConfigs.stream().map(JWTIssuerConfig::getAud).filter(Objects::nonNull).toArray(String[]::new);
    if (audiences.length > 0) {
      jwtConsumerBuilder.setExpectedAudience(audiences); // to whom the JWT is intended for
    } else {
      jwtConsumerBuilder.setSkipDefaultAudienceValidation();
    }
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
    getPromptHeaders(responseError, message).forEach(response::setHeader);
    response.sendError(httpCode, message);
    log.info("JWT Authentication attempt failed: {}", message);
  }

  /**
   * Generate proper response prompt headers
   * @param responseError standardized error code. Set to 'null' to generate WWW-Authenticate header with no error
   * @param message custom message string to return in www-authenticate, or null if no error
   * @return map of headers to add to response
   */
  private Map<String, String> getPromptHeaders(BearerWwwAuthErrorCode responseError, String message) {
    Map<String,String> headers = new HashMap<>();
    List<String> wwwAuthParams = new ArrayList<>();
    wwwAuthParams.add("Bearer realm=\"" + realm + "\"");
    if (responseError != null) {
      wwwAuthParams.add("error=\"" + responseError + "\"");
      wwwAuthParams.add("error_description=\"" + message + "\"");
    }
    headers.put(HttpHeaders.WWW_AUTHENTICATE, String.join(", ", wwwAuthParams));
    headers.put(AuthenticationPlugin.HTTP_HEADER_X_SOLR_AUTHDATA, generateAuthDataHeader());
    return headers;
  }

  protected String generateAuthDataHeader() {
    JWTIssuerConfig primaryIssuer = getPrimaryIssuer();
    Map<String,Object> data = new HashMap<>();
    data.put(JWTIssuerConfig.PARAM_AUTHORIZATION_ENDPOINT, primaryIssuer.getAuthorizationEndpoint());
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

  public List<JWTIssuerConfig> getIssuerConfigs() {
    return issuerConfigs;
  }

  /**
   * Lookup issuer config by its name
   * @param name name property of config
   * @return issuer config object or null if not found
   */
  public JWTIssuerConfig getIssuerConfigByName(String name) {
    return issuerConfigs.stream().filter(ic -> name.equals(ic.getName())).findAny().orElse(null);
  }
}
