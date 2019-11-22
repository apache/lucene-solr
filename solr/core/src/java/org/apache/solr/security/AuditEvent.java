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

import javax.servlet.http.HttpServletRequest;
import java.lang.invoke.MethodHandles;
import java.security.Principal;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.servlet.ServletUtils;
import org.apache.solr.servlet.SolrRequestParsers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import static org.apache.solr.security.AuditEvent.EventType.ANONYMOUS;
import static org.apache.solr.security.AuditEvent.EventType.ERROR;

/**
 * Audit event that takes request and auth context as input to be able to audit log custom things.
 * This interface may change in next release and is marked experimental
 * @since 8.1.0
 * @lucene.experimental
 */
public class AuditEvent {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private StringBuffer requestUrl;
  private String nodeName;
  private String message;
  private Level level;
  private Date date;
  private String username;
  private String session;
  private String clientIp;
  private List<String> collections;
  private Map<String, Object> context;
  private Map<String, String> headers;
  private Map<String, List<String>> solrParams = new HashMap<>();
  private String solrHost;
  private int solrPort;
  private String solrIp;
  private String resource;
  private String httpMethod;
  private String httpQueryString;
  private EventType eventType;
  private AuthorizationResponse autResponse;
  private RequestType requestType;
  private double QTime = -1;
  private int status = -1;
  private Throwable exception;
  
  /* Predefined event types. Custom types can be made through constructor */
  public enum EventType {
    AUTHENTICATED("Authenticated", "User successfully authenticated", Level.INFO, -1),
    REJECTED("Rejected", "Authentication request rejected", Level.WARN, 401),
    ANONYMOUS("Anonymous", "Request proceeds with unknown user", Level.INFO, -1),
    ANONYMOUS_REJECTED("AnonymousRejected", "Request from unknown user rejected", Level.WARN, 401),
    AUTHORIZED("Authorized", "Authorization succeeded", Level.INFO, -1),
    UNAUTHORIZED("Unauthorized", "Authorization failed", Level.WARN, 403),
    COMPLETED("Completed", "Request completed", Level.INFO, 200),
    ERROR("Error", "Request was not executed due to an error", Level.ERROR, 500);
    
    public final String message;
    public String explanation;
    public final Level level;
    public int status;

    EventType(String message, String explanation, Level level, int status) {
      this.message = message;
      this.explanation = explanation;
      this.level = level;
      this.status = status;
    }
  }

  /**
   * Empty event, must be filled by user using setters.
   * Message and Loglevel will be initialized from EventType but can
   * be overridden with setters afterwards.
   * @param eventType a predefined or custom EventType
   */
  public AuditEvent(EventType eventType) {
    this.date = new Date();
    this.eventType = eventType;
    this.status = eventType.status;
    this.level = eventType.level;
    this.message = eventType.message;
  }

  public AuditEvent(EventType eventType, HttpServletRequest httpRequest) {
    this(eventType, null, httpRequest);
  }
  
  // Constructor for testing and deserialization only
  protected AuditEvent() { }
  
  /**
   * Event based on an HttpServletRequest, typically used during authentication. 
   * Solr will fill in details such as ip, http method etc from the request, and
   * username if Principal exists on the request.
   * @param eventType a predefined or custom EventType
   * @param httpRequest the request to initialize from
   */
  public AuditEvent(EventType eventType, Throwable exception, HttpServletRequest httpRequest) {
    this(eventType);
    this.solrHost = httpRequest.getLocalName();
    this.solrPort = httpRequest.getLocalPort();
    this.solrIp = httpRequest.getLocalAddr();
    this.clientIp = httpRequest.getRemoteAddr();
    this.httpMethod = httpRequest.getMethod();
    this.httpQueryString = httpRequest.getQueryString();
    this.headers = getHeadersFromRequest(httpRequest);
    this.requestUrl = httpRequest.getRequestURL();
    this.nodeName = MDC.get(ZkStateReader.NODE_NAME_PROP);
    SolrRequestParsers.parseQueryString(httpQueryString).forEach(sp -> {
      this.solrParams.put(sp.getKey(), Arrays.asList(sp.getValue()));
    });

    setResource(ServletUtils.getPathAfterContext(httpRequest));
    setRequestType(findRequestType());

    if (exception != null) setException(exception);

    Principal principal = httpRequest.getUserPrincipal();
    if (principal != null) {
      this.username = httpRequest.getUserPrincipal().getName();
    } else if (eventType.equals(EventType.AUTHENTICATED)) {
      this.eventType = ANONYMOUS;
      this.message = ANONYMOUS.message;
      this.level = ANONYMOUS.level;
      log.debug("Audit event type changed from AUTHENTICATED to ANONYMOUS since no Principal found on request");
    }
  }

  /**
   * Event based on request and AuthorizationContext. Solr will fill in details
   * such as collections, ip, http method etc from the context.
   * @param eventType a predefined or custom EventType
   * @param httpRequest the request to initialize from
   * @param authorizationContext the context to initialize from
   */
  public AuditEvent(EventType eventType, HttpServletRequest httpRequest, AuthorizationContext authorizationContext) {
    this(eventType, httpRequest);
    this.collections = authorizationContext.getCollectionRequests()
        .stream().map(r -> r.collectionName).collect(Collectors.toList());
    setResource(authorizationContext.getResource());
    this.requestType = RequestType.convertType(authorizationContext.getRequestType());
    authorizationContext.getParams().forEach(p -> {
      this.solrParams.put(p.getKey(), Arrays.asList(p.getValue()));
    });
  }

  /**
   * Event to log completed requests. Takes time and status. Solr will fill in details
   * such as collections, ip, http method etc from the HTTP request and context.
   *
   * @param eventType            a predefined or custom EventType
   * @param httpRequest          the request to initialize from
   * @param authorizationContext the context to initialize from
   * @param qTime                query time
   * @param exception            exception from query response, or null if OK
   */
  public AuditEvent(EventType eventType, HttpServletRequest httpRequest, AuthorizationContext authorizationContext, double qTime, Throwable exception) {
    this(eventType, httpRequest, authorizationContext);
    setQTime(qTime);
    setException(exception);
  }

  private HashMap<String, String> getHeadersFromRequest(HttpServletRequest httpRequest) {
    HashMap<String, String> h = new HashMap<>();
    Enumeration<String> headersEnum = httpRequest.getHeaderNames();
    while (headersEnum != null && headersEnum.hasMoreElements()) {
      String name = headersEnum.nextElement();
      h.put(name, httpRequest.getHeader(name));
    }
    return h;
  }

  public enum Level {
    INFO,  // Used for normal successful events
    WARN,  // Used when a user is blocked etc
    ERROR  // Used when there is an exception or error during auth / authz
  }

  public enum RequestType {
    ADMIN, SEARCH, UPDATE, STREAMING, UNKNOWN;
    
    static RequestType convertType(AuthorizationContext.RequestType ctxReqType) {
      switch (ctxReqType) {
        case ADMIN:
          return RequestType.ADMIN;
        case READ:
          return RequestType.SEARCH;
        case WRITE:
          return RequestType.UPDATE;
        default:
          return RequestType.UNKNOWN;
      }
    }
  }
  
  public String getMessage() {
    return message;
  }

  public Level getLevel() {
    return level;
  }

  public Date getDate() {
    return date;
  }

  public String getUsername() {
    return username;
  }

  public String getSession() {
    return session;
  }

  public String getClientIp() {
    return clientIp;
  }
  
  public Map<String, Object> getContext() {
    return context;
  }
  
  public List<String> getCollections() {
    return collections;
  }

  public String getResource() {
    return resource;
  }

  public String getHttpMethod() {
    return httpMethod;
  }

  public String getHttpQueryString() {
    return httpQueryString;
  }

  public EventType getEventType() {
    return eventType;
  }

  public String getSolrHost() {
    return solrHost;
  }

  public String getSolrIp() {
    return solrIp;
  }

  public int getSolrPort() {
    return solrPort;
  }

  public Map<String, String> getHeaders() {
    return headers;
  }

  public Map<String, List<String>> getSolrParams() {
    return solrParams;
  }

  public String getSolrParamAsString(String key) {
    List<String> v = getSolrParams().get(key);
    if (v != null && v.size() > 0) {
      return String.valueOf((v).get(0));
    }
    return null;
  }
  
  public AuthorizationResponse getAutResponse() {
    return autResponse;
  }

  public String getNodeName() {
    return nodeName;
  }

  public RequestType getRequestType() {
    return requestType;
  }

  public int getStatus() {
    return status;
  }

  public double getQTime() {
    return QTime;
  }
  
  public Throwable getException() {
    return exception;
  }

  public StringBuffer getRequestUrl() {
    return requestUrl;
  }

  // Setters, builder style
  
  public AuditEvent setRequestUrl(StringBuffer requestUrl) {
    this.requestUrl = requestUrl;
    return this;
  }
  
  public AuditEvent setSession(String session) {
    this.session = session;
    return this;
  }

  public AuditEvent setClientIp(String clientIp) {
    this.clientIp = clientIp;
    return this;
  }

  public AuditEvent setContext(Map<String, Object> context) {
    this.context = context;
    return this;
  }

  public AuditEvent setContextEntry(String key, Object value) {
    this.context.put(key, value);
    return this;
  }

  public AuditEvent setMessage(String message) {
    this.message = message;
    return this;
  }

  public AuditEvent setLevel(Level level) {
    this.level = level;
    return this;
  }

  public AuditEvent setDate(Date date) {
    this.date = date;
    return this;
  }

  public AuditEvent setUsername(String username) {
    this.username = username;
    return this;
  }

  public AuditEvent setCollections(List<String> collections) {
    this.collections = collections;
    return this;
  }

  public AuditEvent setResource(String resource) {
    this.resource = normalizeResourcePath(resource);
    return this;
  }

  public AuditEvent setHttpMethod(String httpMethod) {
    this.httpMethod = httpMethod;
    return this;
  }

  public AuditEvent setHttpQueryString(String httpQueryString) {
    this.httpQueryString = httpQueryString;
    return this;
  }

  public AuditEvent setSolrHost(String solrHost) {
    this.solrHost = solrHost;
    return this;
  }

  public AuditEvent setSolrPort(int solrPort) {
    this.solrPort = solrPort;
    return this;
  }

  public AuditEvent setSolrIp(String solrIp) {
    this.solrIp = solrIp;
    return this;
  }

  public AuditEvent setHeaders(Map<String, String> headers) {
    this.headers = headers;
    return this;
  }

  public AuditEvent setSolrParams(Map<String, List<String>> solrParams) {
    this.solrParams = solrParams;
    return this;
  }

  public AuditEvent setAutResponse(AuthorizationResponse autResponse) {
    this.autResponse = autResponse;
    return this;
  }

  public AuditEvent setRequestType(RequestType requestType) {
    this.requestType = requestType;
    return this;
  }

  public AuditEvent setQTime(double QTime) {
    this.QTime = QTime;
    return this;
  }

  public AuditEvent setStatus(int status) {
    this.status = status;
    return this;
  }

  public AuditEvent setException(Throwable exception) {
    this.exception = exception;
    if (exception != null) {
      this.eventType = ERROR;
      this.level = ERROR.level;
      this.message = ERROR.message;
      if (exception instanceof SolrException)
        status = ((SolrException)exception).code();
    }
    return this;
  }

  private RequestType findRequestType() {
    if (resource == null) return RequestType.UNKNOWN;
    if (SEARCH_PATH_PATTERNS.stream().anyMatch(p -> p.matcher(resource).matches())) return RequestType.SEARCH;
    if (INDEXING_PATH_PATTERNS.stream().anyMatch(p -> p.matcher(resource).matches())) return RequestType.UPDATE;
    if (STREAMING_PATH_PATTERNS.stream().anyMatch(p -> p.matcher(resource).matches())) return RequestType.STREAMING;
    if (ADMIN_PATH_PATTERNS.stream().anyMatch(p -> p.matcher(resource).matches())) return RequestType.ADMIN;
    return RequestType.UNKNOWN;
  }

  protected String normalizeResourcePath(String resourcePath) {
    if (resourcePath == null) return "";
    return resourcePath.replaceFirst("^/____v2", "/api");
  }

  private static final List<String> ADMIN_PATH_REGEXES = Arrays.asList(
      "^/admin/.*",
      "^/api/(c|collections)$",
      "^/api/(c|collections)/[^/]+/config$",
      "^/api/(c|collections)/[^/]+/schema$",
      "^/api/(c|collections)/[^/]+/shards.*",
      "^/api/cores.*$",
      "^/api/node.*$",
      "^/api/cluster.*$");

  private static final List<String> STREAMING_PATH_REGEXES = Collections.singletonList(".*/stream.*");
  private static final List<String> INDEXING_PATH_REGEXES = Collections.singletonList(".*/update.*");
  private static final List<String> SEARCH_PATH_REGEXES = Arrays.asList(".*/select.*", ".*/query.*");

  private static final List<Pattern> ADMIN_PATH_PATTERNS = ADMIN_PATH_REGEXES.stream().map(Pattern::compile).collect(Collectors.toList());
  private static final List<Pattern> STREAMING_PATH_PATTERNS = STREAMING_PATH_REGEXES.stream().map(Pattern::compile).collect(Collectors.toList());
  private static final List<Pattern> INDEXING_PATH_PATTERNS = INDEXING_PATH_REGEXES.stream().map(Pattern::compile).collect(Collectors.toList());
  private static final List<Pattern> SEARCH_PATH_PATTERNS = SEARCH_PATH_REGEXES.stream().map(Pattern::compile).collect(Collectors.toList());
}
