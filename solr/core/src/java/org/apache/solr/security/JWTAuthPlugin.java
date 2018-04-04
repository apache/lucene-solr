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
import java.net.MalformedURLException;
import java.net.URL;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.BasicUserPrincipal;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.config.Lookup;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.message.BasicHeader;
import org.apache.solr.client.solrj.impl.SolrHttpClientBuilder;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SpecProvider;
import org.apache.solr.common.util.Utils;
import org.apache.solr.common.util.ValidatingJsonMap;
import org.apache.solr.security.JWTAuthPlugin.AuthenticationResponse.AuthCode;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Authenticaion plugin that finds logged in user by validating the signature of a JWT token
 */
public class JWTAuthPlugin extends AuthenticationPlugin implements HttpClientBuilderPlugin, SpecProvider {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final static ThreadLocal<Header> authHeader = new ThreadLocal<>();
  private static final String PARAM_BLOCK_UNKNOWN = "block_unknown";
  private static final String PARAM_JWK_URL = "jwk_url";
  private static final String PARAM_JWK = "jwk";
  private static final String PARAM_ISSUER = "iss";
  private static final String PARAM_AUDIENCE = "aud";
  private static final String PARAM_REQUIRE_SUBJECT = "require_sub";
  private static final String PARAM_PRINCIPAL_CLAIM = "principal_claim";
  private static final String PARAM_ROLES_CLAIM = "roles_claim";
  private static final String PARAM_REQUIRE_EXPIRATIONTIME = "require_exp";
  private static final String PARAM_ALG_WHITELIST = "alg_whitelist";
  private static final String PARAM_JWK_CACHE_DURATION = "jwk_cache_dur";
  private static final String PARAM_CLAIMS_MATCH = "claims_match";
  private static final String AUTH_REALM = "solr";

  private String jwk_url;
  private Map<String, Object> jwk;
  private JwtConsumer jwtConsumer;
  private String iss;
  private String aud;
  private boolean requireSubject;
  private boolean requireExpirationTime;
  private List<String> algWhitelist;
  private HttpsJwks httpsJkws;
  private long jwkCacheDuration;
  VerificationKeyResolver verificationKeyResolver;
  private JsonWebKeySet jwks;
  private String principalClaim;
  private String rolesClaim;
  private Map<String, String> claimsMatch;
  private HashMap<String, Pattern> claimsMatchCompiled;
  private boolean blockUnknown;

  @Override
  public void init(Map<String, Object> pluginConfig) {
    blockUnknown = Boolean.parseBoolean(String.valueOf(pluginConfig.getOrDefault(PARAM_BLOCK_UNKNOWN, false)));
    jwk_url = (String) pluginConfig.get(PARAM_JWK_URL);
    jwk = (Map<String, Object>) pluginConfig.get(PARAM_JWK);
    iss = (String) pluginConfig.get(PARAM_ISSUER);
    aud = (String) pluginConfig.get(PARAM_AUDIENCE);
    requireSubject = Boolean.parseBoolean(String.valueOf(pluginConfig.getOrDefault(PARAM_REQUIRE_SUBJECT, "true")));
    requireExpirationTime = Boolean.parseBoolean(String.valueOf(pluginConfig.getOrDefault(PARAM_REQUIRE_EXPIRATIONTIME, "true")));
    algWhitelist = (List<String>) pluginConfig.get(PARAM_ALG_WHITELIST);
    jwkCacheDuration = Long.parseLong((String) pluginConfig.getOrDefault(PARAM_JWK_CACHE_DURATION, "3600"));
    principalClaim = (String) pluginConfig.getOrDefault(PARAM_PRINCIPAL_CLAIM, "sub");
    rolesClaim = (String) pluginConfig.get(PARAM_ROLES_CLAIM);
    claimsMatch = (Map<String, String>) pluginConfig.get(PARAM_CLAIMS_MATCH);
    claimsMatchCompiled = new HashMap<>();
    if (claimsMatch != null) {
      for (Map.Entry<String, String> entry : claimsMatch.entrySet()) {
        claimsMatchCompiled.put(entry.getKey(), Pattern.compile(entry.getValue()));
      }
    }

    jwtConsumer = null;
    if (jwk_url != null) {
      // The HttpsJwks retrieves and caches keys from a the given HTTPS JWKS endpoint.
      try {
        URL jwkUrl = new URL(jwk_url);
        if (!"https".equalsIgnoreCase(jwkUrl.getProtocol())) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "jwk_url must be an HTTPS url");
        }
      } catch (MalformedURLException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "jwk_url must be a valid https URL");
      }
      httpsJkws = new HttpsJwks(jwk_url);
      httpsJkws.setDefaultCacheDuration(jwkCacheDuration);
      verificationKeyResolver = new HttpsJwksVerificationKeyResolver(httpsJkws);
      initConsumer();
    } else if (jwk != null) {
      try {
        jwks = parseJwkSet(jwk);
        verificationKeyResolver = new JwksVerificationKeyResolver(jwks.getJsonWebKeys());
        initConsumer();
      } catch (JoseException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Invalid JWTAuthPlugin configuration, jwk field parse error", e);
      }
    } else {
      log.warn("JWTAuthPlugin needs to specify either 'jwk' or 'jwk_url' parameters.");
    }
  }

  protected JsonWebKeySet parseJwkSet(Map<String, Object> jwkObj) throws JoseException {
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

  @Override
  public boolean doAuthenticate(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws Exception {
    HttpServletRequest request = (HttpServletRequest) servletRequest;
    HttpServletResponse response = (HttpServletResponse) servletResponse;

    String header = request.getHeader(HttpHeaders.AUTHORIZATION);

    if (jwtConsumer == null) {
      //
      if (header == null && !blockUnknown) {
        log.info("JWTAuth not configured, but allowing anonymous access since blockUnknown==false");
        filterChain.doFilter(request, response);
        return true;
      }
      log.warn("JWTAuth not configured");
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "JWTAuth plugin not correctly configured");
    }

    if (header != null) {
      // Put the header on the thread for later use in inter-node request
      authHeader.set(new BasicHeader(HttpHeaders.AUTHORIZATION, header));
    }
    AuthenticationResponse authResponse = authenticate(header);
    switch(authResponse.authCode) {
      case AUTHENTICATED:
        HttpServletRequestWrapper wrapper = new HttpServletRequestWrapper(request) {
          @Override
          public Principal getUserPrincipal() {
            return authResponse.getPrincipal();
          }
        };
        filterChain.doFilter(wrapper, response);
        return true;

      case PASS_THROUGH:
        log.debug("Unknown user, but allow due to block_unknown=false");
        filterChain.doFilter(request, response);
        return true;

      case AUTZ_HEADER_PROBLEM:
        log.debug("Authentication failed with reason {}, message {}", authResponse.authCode, authResponse.errorMessage);
        authenticationFailure(response, authResponse.getAuthCode().msg, HttpServletResponse.SC_BAD_REQUEST, BearerWwwAuthErrorCode.invalid_request);
        return false;

      case CLAIM_MISMATCH:
      case JWT_EXPIRED:
      case JWT_PARSE_ERROR:
      case JWT_VALIDATION_EXCEPTION:
      case PRINCIPAL_MISSING:
        log.debug("Authentication failed with reason {}, message {}", authResponse.authCode, authResponse.errorMessage);
        if (authResponse.getJwtException() != null) {
          log.info("Exception: {}", authResponse.getJwtException().getMessage());
        }
        authenticationFailure(response, authResponse.getAuthCode().msg, HttpServletResponse.SC_UNAUTHORIZED, BearerWwwAuthErrorCode.invalid_token);
        return false;

      case NO_AUTZ_HEADER:
      default:
        log.debug("Authentication failed with reason {}, message {}", authResponse.authCode, authResponse.errorMessage);
        authenticationFailure(response, authResponse.getAuthCode().msg);
        return false;
    }
  }

  /**
   * Testable authentication method
   *
   * @param authorizationHeader the http header "Authentication"
   * @return AuthenticationResponse object
   */
  protected AuthenticationResponse authenticate(String authorizationHeader) {
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
                return new AuthenticationResponse(AuthCode.PRINCIPAL_MISSING, "Cannot identify principal from JWT. Required claim " + principalClaim + " missing. Cannot authenticate");
              }
              if (claimsMatchCompiled != null) {
                for (Map.Entry<String, Pattern> entry : claimsMatchCompiled.entrySet()) {
                  String claim = entry.getKey();
                  if (jwtClaims.hasClaim(claim)) {
                    if (!entry.getValue().matcher(jwtClaims.getStringClaimValue(claim)).matches()) {
                      return new AuthenticationResponse(AuthCode.CLAIM_MISMATCH,
                          "Claim " + claim + "=" + jwtClaims.getStringClaimValue(claim)
                              + " does not match required regular expression " + entry.getValue().pattern());
                    }
                  } else {
                    return new AuthenticationResponse(AuthCode.CLAIM_MISMATCH, "Claim " + claim + " is required but does not exist in JWT");
                  }
                }
              }
              Set<String> roles = Collections.emptySet();
              if (rolesClaim != null) {
                if (!jwtClaims.hasClaim(rolesClaim)) {
                  return new AuthenticationResponse(AuthCode.CLAIM_MISMATCH, "Roles claim " + rolesClaim + " is required but does not exist in JWT");
                }
                Object rolesObj = jwtClaims.getClaimValue(rolesClaim);
                if (rolesObj instanceof String) {
                  roles = Collections.singleton((String) rolesObj);
                } else if (rolesObj instanceof List) {
                  roles = new HashSet<>(jwtClaims.getStringListClaimValue(rolesClaim));
                }
                // Pass roles with principal to signal to any Authorization plugins that user has some verified role claims
                return new AuthenticationResponse(AuthCode.AUTHENTICATED, new PrincipalWithUserRoles(principal, roles));
              } else {
                return new AuthenticationResponse(AuthCode.AUTHENTICATED, new BasicUserPrincipal(principal));
              }
            } catch (InvalidJwtException e) {
              // Whether or not the JWT has expired being one common reason for invalidity
              if (e.hasExpired()) {
                return new AuthenticationResponse(AuthCode.JWT_EXPIRED, "Authentication failed due to expired JWT token. Expired at " + e.getJwtContext().getJwtClaims().getExpirationTime());
              }
              return new AuthenticationResponse(AuthCode.JWT_VALIDATION_EXCEPTION, e);
            }
          } catch (MalformedClaimException e) {
            return new AuthenticationResponse(AuthCode.JWT_PARSE_ERROR, "Malformed claim, error was: " + e.getMessage());
          }
        } else {
          return new AuthenticationResponse(AuthCode.AUTZ_HEADER_PROBLEM, "Authorization header is not in correct format");
        }
      } else {
        return new AuthenticationResponse(AuthCode.AUTZ_HEADER_PROBLEM, "Authorization header is not in correct format");
      }
    } else {
      // No Authorization header
      if (blockUnknown) {
        return new AuthenticationResponse(AuthCode.NO_AUTZ_HEADER, "Missing Authorization header");
      } else {
        log.debug("No user authenticated, but block_unknown=false, so letting request through");
        return new AuthenticationResponse(AuthCode.PASS_THROUGH);
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

  }

  @Override
  public void closeRequest() {
    authHeader.remove();
  }

  /**
   * Gets a client builder for inter-node requests
   * @param builder any existing builder or null to create a new one
   * @return Returns an instance of a SolrHttpClientBuilder to be used for configuring the
   * HttpClients for use with SolrJ clients.
   * @lucene.experimental
   */
  @Override
  public SolrHttpClientBuilder getHttpClientBuilder(SolrHttpClientBuilder builder) {
    if (builder == null) {
      builder = SolrHttpClientBuilder.create();
    }
    builder.setAuthSchemeRegistryProvider(() -> {
      Lookup<AuthSchemeProvider> authProviders = RegistryBuilder.<AuthSchemeProvider>create()
          .register("Bearer", new BearerAuthSchemeProvider())
          .build();
      return authProviders;
    });
    builder.setDefaultCredentialsProvider(() -> {
      CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
      // Pull the authorization bearer header from ThreadLocal
      if (authHeader.get() == null) {
        log.warn("Cannot find Authorization header on request thread");
      } else {
        // TODO: Limit AuthScope?
        credentialsProvider.setCredentials(AuthScope.ANY, new TokenCredentials(authHeader.get().getValue()));
      }
      return credentialsProvider;
    });
    return builder;
  }

  // NOCOMMIT: v2 api documentation
  @Override
  public ValidatingJsonMap getSpec() {
    return Utils.getSpec("cluster.security.BasicAuth.Commands").getSpec();
  }

  private void authenticationFailure(HttpServletResponse response, String message) throws IOException {
    authenticationFailure(response, message, HttpServletResponse.SC_UNAUTHORIZED, null);
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
    response.sendError(httpCode, message);
  }


  /**
   * Response for authentication attempt
   */
  static class AuthenticationResponse {
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
      JWT_VALIDATION_EXCEPTION("JWT validation failed"); // The JWT parser failed validation. More details in exception

      private final String msg;

      AuthCode(String msg) {
        this.msg = msg;
      }
    }

    public AuthenticationResponse(AuthCode authCode, InvalidJwtException e) {
      this.authCode = authCode;
      this.jwtException = e;
      principal = null;
      this.errorMessage = e.getMessage();
    }
    
    public AuthenticationResponse(AuthCode authCode, String errorMessage) {
      this.authCode = authCode;
      this.errorMessage = errorMessage;
      principal = null;
    }

    public AuthenticationResponse(AuthCode authCode, Principal principal) {
      this.authCode = authCode;
      this.principal = principal;
    }
    
    public AuthenticationResponse(AuthCode authCode) {
      this.authCode = authCode;
      principal = null;
    }

    public boolean isAuthenticated() {
      return authCode.equals(AuthCode.AUTHENTICATED);
    }

    public Principal getPrincipal() {
      return principal;
    }

    public String getErrorMessage() {
      return errorMessage;
    }

    public InvalidJwtException getJwtException() {
      return jwtException;
    }

    public AuthCode getAuthCode() {
      return authCode;
    }
  }

}
