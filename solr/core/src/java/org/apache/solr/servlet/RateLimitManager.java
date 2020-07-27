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

package org.apache.solr.servlet;

import javax.servlet.FilterConfig;
import javax.servlet.http.HttpServletRequest;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.client.solrj.SolrRequest;

import static org.apache.solr.common.params.CommonParams.SOLR_REQUEST_CONTEXT_PARAM;
import static org.apache.solr.common.params.CommonParams.SOLR_REQUEST_TYPE_PARAM;

/**
 * This class is responsible for managing rate limiting per request type. Rate limiters
 * can be registered with this class against a corresponding type. There can be only one
 * rate limiter associated with a request type.
 *
 * The actual rate limiting and the limits should be implemented in the corresponding RequestRateLimiter
 * implementation. RateLimitManager is responsible for the orchestration but not the specifics of how the
 * rate limiting is being done for a specific request type.
 */
public class RateLimitManager {
  public final static int DEFAULT_CONCURRENT_REQUESTS= (Runtime.getRuntime().availableProcessors()) * 3;
  public final static long DEFAULT_EXPIRATION_TIME_INMS = 300;
  public final static long DEFAULT_SLOT_ACQUISITION_TIMEOUT_MS = -1;

  private final Map<String, RequestRateLimiter> requestRateLimiterMap;

  public RateLimitManager() {
    this.requestRateLimiterMap = new HashMap<>();
  }

  // Handles an incoming request. The main orchestration code path, this method will
  // identify which (if any) rate limiter can handle this request. Internal requests will not be
  // rate limited
  // Returns true if request is accepted for processing, false if it should be rejected

  // NOTE: It is upto specific rate limiter implementation to handle queuing of rejected requests.
  public boolean handleRequest(HttpServletRequest request) throws InterruptedException {
    String requestContext = request.getHeader(SOLR_REQUEST_CONTEXT_PARAM);
    String typeOfRequest = request.getHeader(SOLR_REQUEST_TYPE_PARAM);

    if (typeOfRequest == null) {
      // Cannot determine if this request should be throttled
      return true;
    }

    // Do not throttle internal requests
    if (requestContext != null && requestContext.equals(SolrRequest.SolrClientContext.SERVER.toString())) {
      return true;
    }

    RequestRateLimiter requestRateLimiter = requestRateLimiterMap.get(typeOfRequest);

    if (requestRateLimiter == null) {
      // No request rate limiter for this request type
      return true;
    }

    return requestRateLimiter.handleRequest(request);
  }

  // Resume a pending request from one of the registered rate limiters.
  // The current model is round robin -- iterate over the list and get a pending request and resume it.

  // TODO: This should be a priority queue based model
  public void resumePendingRequest(HttpServletRequest request) {
    String typeOfRequest = request.getHeader(SOLR_REQUEST_TYPE_PARAM);

    RequestRateLimiter previousRequestRateLimiter = requestRateLimiterMap.get(typeOfRequest);

    if (previousRequestRateLimiter == null) {
      // No rate limiter for this request type
      return;
    }

    // Give preference to the previous request's rate limiter
    if (previousRequestRateLimiter.resumePendingOperation()) {
      return;
    }

    if (previousRequestRateLimiter.getRateLimiterConfig().isWorkStealingEnabled) {
      for (Map.Entry<String, RequestRateLimiter> currentEntry : requestRateLimiterMap.entrySet()) {
        RequestRateLimiter requestRateLimiter = currentEntry.getValue();
        boolean isRequestResumed = requestRateLimiter.resumePendingOperation();

        if (isRequestResumed) {
          return;
        }
      }
    }
  }

  // Decrement the active requests in the rate limiter for the corresponding request type.
  public void decrementActiveRequests(HttpServletRequest request) {
    String typeOfRequest = request.getHeader(SOLR_REQUEST_TYPE_PARAM);

    RequestRateLimiter requestRateLimiter = requestRateLimiterMap.get(typeOfRequest);

    if (requestRateLimiter == null) {
      // No rate limiter for this request type
      return;
    }

    requestRateLimiter.decrementConcurrentRequests();
  }

  public void close() {

    for (RequestRateLimiter requestRateLimiter : requestRateLimiterMap.values()) {
      requestRateLimiter.close();
    }
  }

  public void registerRequestRateLimiter(RequestRateLimiter requestRateLimiter, SolrRequest.SolrRequestType requestType) {
    requestRateLimiterMap.put(requestType.toString(), requestRateLimiter);
  }

  public RequestRateLimiter getRequestRateLimiter(SolrRequest.SolrRequestType requestType) {
    return requestRateLimiterMap.get(requestType.toString());
  }

  public static class Builder {
    protected FilterConfig config;

    public void setConfig(FilterConfig config) {
      this.config = config;
    }

    public RateLimitManager build() {
      RateLimitManager rateLimitManager = new RateLimitManager();

      rateLimitManager.registerRequestRateLimiter(new QueryRateLimiter(config), SolrRequest.SolrRequestType.QUERY);

      return rateLimitManager;
    }
  }
}
