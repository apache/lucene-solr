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

import javax.servlet.AsyncContext;
import javax.servlet.DispatcherType;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpUpgradeHandler;
import javax.servlet.http.Part;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.Principal;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.collections.iterators.EnumerationIterator;
import org.apache.http.auth.BasicUserPrincipal;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.junit.Test;

import static org.junit.Assert.*;

public class AuditEventTest {
  @Test
  public void fromAuthorizationContext() throws Exception {
    AuditEvent event = new AuditEvent(AuditEvent.EventType.AUTHENTICATED, new HttpServletRequest() {
      @Override
      public String getAuthType() {
        return null;
      }

      @Override
      public Cookie[] getCookies() {
        return new Cookie[0];
      }

      @Override
      public long getDateHeader(String name) {
        return 0;
      }

      @Override
      public String getHeader(String name) {
        return name.equals("foo") ? "fooval" : "barval";
      }

      @Override
      public Enumeration<String> getHeaders(String name) {
        return null;
      }

      @Override
      public Enumeration<String> getHeaderNames() {
        return Collections.enumeration(Arrays.asList("foo", "bar"));
      }

      @Override
      public int getIntHeader(String name) {
        return 0;
      }

      @Override
      public String getMethod() {
        return "GET";
      }

      @Override
      public String getPathInfo() {
        return null;
      }

      @Override
      public String getPathTranslated() {
        return null;
      }

      @Override
      public String getContextPath() {
        return null;
      }

      @Override
      public String getQueryString() {
        return "?foo&bar";
      }

      @Override
      public String getRemoteUser() {
        return null;
      }

      @Override
      public boolean isUserInRole(String role) {
        return false;
      }

      @Override
      public Principal getUserPrincipal() {
        return new BasicUserPrincipal("George");
      }

      @Override
      public String getRequestedSessionId() {
        return null;
      }

      @Override
      public String getRequestURI() {
        return null;
      }

      @Override
      public StringBuffer getRequestURL() {
        return null;
      }

      @Override
      public String getServletPath() {
        return null;
      }

      @Override
      public HttpSession getSession(boolean create) {
        return null;
      }

      @Override
      public HttpSession getSession() {
        return null;
      }

      @Override
      public String changeSessionId() {
        return null;
      }

      @Override
      public boolean isRequestedSessionIdValid() {
        return false;
      }

      @Override
      public boolean isRequestedSessionIdFromCookie() {
        return false;
      }

      @Override
      public boolean isRequestedSessionIdFromURL() {
        return false;
      }

      @Override
      public boolean isRequestedSessionIdFromUrl() {
        return false;
      }

      @Override
      public boolean authenticate(HttpServletResponse response) throws IOException, ServletException {
        return false;
      }

      @Override
      public void login(String username, String password) throws ServletException {

      }

      @Override
      public void logout() throws ServletException {

      }

      @Override
      public Collection<Part> getParts() throws IOException, ServletException {
        return null;
      }

      @Override
      public Part getPart(String name) throws IOException, ServletException {
        return null;
      }

      @Override
      public <T extends HttpUpgradeHandler> T upgrade(Class<T> handlerClass) throws IOException, ServletException {
        return null;
      }

      @Override
      public Object getAttribute(String name) {
        return null;
      }

      @Override
      public Enumeration<String> getAttributeNames() {
        return null;
      }

      @Override
      public String getCharacterEncoding() {
        return null;
      }

      @Override
      public void setCharacterEncoding(String env) throws UnsupportedEncodingException {

      }

      @Override
      public int getContentLength() {
        return 0;
      }

      @Override
      public long getContentLengthLong() {
        return 0;
      }

      @Override
      public String getContentType() {
        return null;
      }

      @Override
      public ServletInputStream getInputStream() throws IOException {
        return null;
      }

      @Override
      public String getParameter(String name) {
        return null;
      }

      @Override
      public Enumeration<String> getParameterNames() {
        return null;
      }

      @Override
      public String[] getParameterValues(String name) {
        return new String[0];
      }

      @Override
      public Map<String, String[]> getParameterMap() {
        return null;
      }

      @Override
      public String getProtocol() {
        return null;
      }

      @Override
      public String getScheme() {
        return null;
      }

      @Override
      public String getServerName() {
        return null;
      }

      @Override
      public int getServerPort() {
        return 0;
      }

      @Override
      public BufferedReader getReader() throws IOException {
        return null;
      }

      @Override
      public String getRemoteAddr() {
        return null;
      }

      @Override
      public String getRemoteHost() {
        return null;
      }

      @Override
      public void setAttribute(String name, Object o) {

      }

      @Override
      public void removeAttribute(String name) {

      }

      @Override
      public Locale getLocale() {
        return null;
      }

      @Override
      public Enumeration<Locale> getLocales() {
        return null;
      }

      @Override
      public boolean isSecure() {
        return false;
      }

      @Override
      public RequestDispatcher getRequestDispatcher(String path) {
        return null;
      }

      @Override
      public String getRealPath(String path) {
        return null;
      }

      @Override
      public int getRemotePort() {
        return 0;
      }

      @Override
      public String getLocalName() {
        return null;
      }

      @Override
      public String getLocalAddr() {
        return null;
      }

      @Override
      public int getLocalPort() {
        return 0;
      }

      @Override
      public ServletContext getServletContext() {
        return null;
      }

      @Override
      public AsyncContext startAsync() throws IllegalStateException {
        return null;
      }

      @Override
      public AsyncContext startAsync(ServletRequest servletRequest, ServletResponse servletResponse) throws IllegalStateException {
        return null;
      }

      @Override
      public boolean isAsyncStarted() {
        return false;
      }

      @Override
      public boolean isAsyncSupported() {
        return false;
      }

      @Override
      public AsyncContext getAsyncContext() {
        return null;
      }

      @Override
      public DispatcherType getDispatcherType() {
        return null;
      }
    }, new AuthorizationContext() {
      @Override
      public SolrParams getParams() {
        return new MapSolrParams(Collections.singletonMap("q", "hello"));
      }

      @Override
      public Principal getUserPrincipal() {
        return new BasicUserPrincipal("George");
      }

      @Override
      public String getHttpHeader(String header) {
        return "MyHeader";
      }

      @Override
      public Enumeration getHeaderNames() {
        return Collections.enumeration(Arrays.asList("Header1", "Header2"));
      }

      @Override
      public String getRemoteAddr() {
        return "127.0.0.1";
      }

      @Override
      public String getRemoteHost() {
        return "localhost";
      }

      @Override
      public List<CollectionRequest> getCollectionRequests() {
        return Arrays.asList(new CollectionRequest("coll1"));
      }

      @Override
      public RequestType getRequestType() {
        return RequestType.READ;
      }

      @Override
      public String getResource() {
        return "/solr/admin/info";
      }

      @Override
      public String getHttpMethod() {
        return "GET";
      }

      @Override
      public Object getHandler() {
        return "/select";
      }
    }, new AuthorizationResponse(200));
    assertEquals("{\"message\":\"Authenticated\",\"level\":\"INFO\",\"date\":" + event.getDate().getTime() + ",\"username\":\"George\",\"session\":null,\"clientIp\":null,\"collections\":[\"coll1\"],\"context\":null,\"headers\":{\"bar\":\"barval\",\"foo\":\"fooval\"},\"solrParams\":null,\"solrHost\":null,\"solrPort\":0,\"solrIp\":null,\"resource\":\"/solr/admin/info\",\"httpMethod\":\"GET\",\"queryString\":\"?foo&bar\",\"eventType\":\"AUTHENTICATED\",\"autResponse\":{\"statusCode\":200,\"message\":null}}",
        new AuditLoggerPlugin.JSONAuditEventFormatter().formatEvent(event));
  }

  @Test
  public void manual() throws Exception {
    AuditEvent event = new AuditEvent(AuditEvent.EventType.REJECTED).setHttpMethod("GET");
    assertTrue(event.getDate() != null);
    assertEquals("GET", event.getHttpMethod());
  }
}