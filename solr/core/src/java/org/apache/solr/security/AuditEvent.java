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
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.security.AuditEvent.EventType.ANONYMOUS;

/**
 * Audit event that takes request and auth context as input to be able to audit log custom things
 */
public class AuditEvent {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private String message;
  private Level level;
  private Date date;
  private String username;
  private String session;
  private String clientIp;
  private List<String> collections;
  private Map<String, Object> context;
  private HashMap<String, String> headers;
  private Map<String, Object> solrParams;
  private String solrHost;
  private int solrPort;
  private String solrIp;
  private String resource;
  private String httpMethod;
  private String queryString;
  private EventType eventType;
  private AuthorizationResponse autResponse;
  private String requestType;

  /* Predefined event types. Custom types can be made through constructor */
  public enum EventType {
    AUTHENTICATED("Authenticated", "User successfully authenticated", Level.INFO),
    REJECTED("Rejected", "Authentication request rejected", Level.WARN),
    ANONYMOUS("Anonymous", "Request proceeds with unknown user", Level.INFO),
    ANONYMOUS_REJECTED("AnonymousRejected", "Request from unknown user rejected", Level.WARN),
    AUTHORIZED("Authorized", "Authorization succeeded", Level.INFO),
    UNAUTHORIZED("Unauthorized", "Authorization failed", Level.WARN),
    ERROR("Error", "Request failed due to an error", Level.ERROR);
    
    private final String message;
    private String explanation;
    private final Level level;

    EventType(String message, String explanation, Level level) {
      this.message = message;
      this.explanation = explanation;
      this.level = level;
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
    this.level = eventType.level;
    this.message = eventType.message;
  }

  /**
   * Event based on an HttpServletRequest, typically used during authentication. 
   * Solr will fill in details such as ip, http method etc from the request, and
   * username if Principal exists on the request.
   * @param eventType a predefined or custom EventType
   * @param httpRequest the request to initialize from
   */
  public AuditEvent(EventType eventType, HttpServletRequest httpRequest) {
    this(eventType);
    this.solrHost = httpRequest.getLocalName();
    this.solrPort = httpRequest.getLocalPort();
    this.solrIp = httpRequest.getLocalAddr();
    this.clientIp = httpRequest.getRemoteAddr();
    this.resource = httpRequest.getContextPath();
    this.httpMethod = httpRequest.getMethod();
    this.queryString = httpRequest.getQueryString();
    this.headers = getHeadersFromRequest(httpRequest);

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
   * Event based on an AuthorizationContext and reponse. Solr will fill in details
   * such as collections, , ip, http method etc from the context.
   * @param eventType a predefined or custom EventType
   * @param authorizationContext the context to initialize from
   */
  public AuditEvent(EventType eventType, HttpServletRequest httpRequest, AuthorizationContext authorizationContext, AuthorizationResponse authResponse) {
    this(eventType, httpRequest);
    this.collections = authorizationContext.getCollectionRequests()
        .stream().map(r -> r.collectionName).collect(Collectors.toList());
    this.resource = authorizationContext.getResource();
    this.requestType = authorizationContext.getRequestType().toString();
    authorizationContext.getParams().getAll(this.solrParams);
    this.autResponse = authResponse;
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

  public String getQueryString() {
    return queryString;
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

  public HashMap<String, String> getHeaders() {
    return headers;
  }

  public Map<String, Object> getSolrParams() {
    return solrParams;
  }

  public AuthorizationResponse getAutResponse() {
    return autResponse;
  }

  // Setters, builder style
  
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
    this.resource = resource;
    return this;
  }

  public AuditEvent setHttpMethod(String httpMethod) {
    this.httpMethod = httpMethod;
    return this;
  }

  public AuditEvent setQueryString(String queryString) {
    this.queryString = queryString;
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

  public AuditEvent setHeaders(HashMap<String, String> headers) {
    this.headers = headers;
    return this;
  }

  public AuditEvent setSolrParams(Map<String, Object> solrParams) {
    this.solrParams = solrParams;
    return this;
  }

  public AuditEvent setAutResponse(AuthorizationResponse autResponse) {
    this.autResponse = autResponse;
    return this;
  }

  public AuditEvent setRequestType(String requestType) {
    this.requestType = requestType;
    return this;
  }
}
