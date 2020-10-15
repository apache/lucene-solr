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
package org.apache.solr.client.solrj.impl;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.invoke.MethodHandles;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import javax.net.ssl.SSLException;

import org.apache.http.HttpRequest;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.RequestWrapper;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrHttpRequestRetryHandler implements HttpRequestRetryHandler {
  
  private static final String GET = "GET";

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  public static final SolrHttpRequestRetryHandler INSTANCE = new SolrHttpRequestRetryHandler();
  
  /** the number of times a method will be retried */
  private final int retryCount;
  
  private final Set<Class<? extends IOException>> nonRetriableClasses;
  
  /**
   * Create the request retry handler using the specified IOException classes
   *
   * @param retryCount
   *          how many times to retry; 0 means no retries
   *          true if it's OK to retry requests that have been sent
   * @param clazzes
   *          the IOException types that should not be retried
   */
  protected SolrHttpRequestRetryHandler(final int retryCount, final Collection<Class<? extends IOException>> clazzes) {
    super();
    this.retryCount = retryCount;
    this.nonRetriableClasses = new HashSet<>();
    for (final Class<? extends IOException> clazz : clazzes) {
      this.nonRetriableClasses.add(clazz);
    }
  }
  
  /**
   * Create the request retry handler using the following list of non-retriable IOException classes: <br>
   * <ul>
   * <li>InterruptedIOException</li>
   * <li>UnknownHostException</li>
   * <li>ConnectException</li>
   * <li>SSLException</li>
   * </ul>
   * 
   * @param retryCount
   *          how many times to retry; 0 means no retries
   *          true if it's OK to retry non-idempotent requests that have been sent
   */
  @SuppressWarnings("unchecked")
  public SolrHttpRequestRetryHandler(final int retryCount) {
    this(retryCount, Arrays.asList(InterruptedIOException.class, UnknownHostException.class,
        ConnectException.class, SSLException.class));
  }
  
  /**
   * Create the request retry handler with a retry count of 3, requestSentRetryEnabled false and using the following
   * list of non-retriable IOException classes: <br>
   * <ul>
   * <li>InterruptedIOException</li>
   * <li>UnknownHostException</li>
   * <li>ConnectException</li>
   * <li>SSLException</li>
   * </ul>
   */
  public SolrHttpRequestRetryHandler() {
    this(3);
  }
  
  @Override
  public boolean retryRequest(final IOException exception, final int executionCount, final HttpContext context) {
    log.debug("Retry http request {} out of {}", executionCount, this.retryCount);
    if (executionCount > this.retryCount) {
      log.debug("Do not retry, over max retry count");
      return false;
    }

    if (!isRetriable(exception)) {
      if (log.isDebugEnabled()) {
        log.debug("Do not retry, non retriable class {}", exception.getClass().getName());
      }
      return false;
    }

    final HttpClientContext clientContext = HttpClientContext.adapt(context);
    final HttpRequest request = clientContext.getRequest();
    
    if (requestIsAborted(request)) {
      log.debug("Do not retry, request was aborted");
      return false;
    }
    
    if (handleAsIdempotent(clientContext)) {
      log.debug("Retry, request should be idempotent");
      return true;
    }

    log.debug("Do not retry, no allow rules matched");
    return false;
  }

  private boolean isRetriable(IOException exception) {
    // Workaround for "recv failed" issue on hard-aborted sockets on Windows
    // (and other operating systems, possibly).
    // https://issues.apache.org/jira/browse/SOLR-13778
    if (exception instanceof SSLException &&
        Arrays.stream(exception.getSuppressed()).anyMatch((t) -> t instanceof SocketException)) {
      return true;
    }

    // Fast check for exact class followed by slow-check with instanceof.
    if (nonRetriableClasses.contains(exception.getClass())
        || nonRetriableClasses.stream().anyMatch(rejectException -> rejectException.isInstance(exception))) {
      return false;
    }

    return true;
  }

  public int getRetryCount() {
    return retryCount;
  }
  
  protected boolean handleAsIdempotent(final HttpClientContext context) {
    String method = context.getRequest().getRequestLine().getMethod();
    // do not retry admin requests, even if they are GET as they are not idempotent
    if (context.getRequest().getRequestLine().getUri().startsWith("/admin/")) {
      log.debug("Do not retry, this is an admin request");
      return false;
    }
    return method.equals(GET);
  }
  
  protected boolean requestIsAborted(final HttpRequest request) {
    HttpRequest req = request;
    if (request instanceof RequestWrapper) { // does not forward request to original
      req = ((RequestWrapper) request).getOriginal();
    }
    return (req instanceof HttpUriRequest && ((HttpUriRequest) req).isAborted());
  }
  
}