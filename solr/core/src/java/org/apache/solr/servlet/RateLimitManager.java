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

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.annotation.SolrThreadSafe;
import org.apache.solr.common.cloud.ClusterPropertiesListener;
import org.apache.solr.common.cloud.SolrZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
@SolrThreadSafe
public class RateLimitManager implements ClusterPropertiesListener {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public final static int DEFAULT_CONCURRENT_REQUESTS= (Runtime.getRuntime().availableProcessors()) * 3;
  public final static long DEFAULT_SLOT_ACQUISITION_TIMEOUT_MS = -1;
  private final Map<String, RequestRateLimiter> requestRateLimiterMap;

  private final Map<HttpServletRequest, RequestRateLimiter.SlotMetadata> activeRequestsMap;

  public RateLimitManager() {
    this.requestRateLimiterMap = new HashMap<>();
    this.activeRequestsMap = new ConcurrentHashMap<>();
  }

  @Override
  public boolean onChange(Map<String, Object> properties) {

    // Hack: We only support query rate limiting for now
    QueryRateLimiter queryRateLimiter = (QueryRateLimiter) requestRateLimiterMap.get(SolrRequest.SolrRequestType.QUERY);

    if (queryRateLimiter != null) {
      try {
        queryRateLimiter.processConfigChange(properties);
      } catch (IOException e) {
        throw new RuntimeException("Encountered IOException: " + e.getMessage());
      }
    }

    return false;
  }

  // Handles an incoming request. The main orchestration code path, this method will
  // identify which (if any) rate limiter can handle this request. Internal requests will not be
  // rate limited
  // Returns true if request is accepted for processing, false if it should be rejected
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

    RequestRateLimiter.SlotMetadata result = requestRateLimiter.handleRequest();

    if (result != null) {
      // Can be the case if request rate limiter is disabled
      if (result.isReleasable()) {
        activeRequestsMap.put(request, result);
      }
      return true;
    }

    RequestRateLimiter.SlotMetadata slotMetadata = trySlotBorrowing(typeOfRequest);

    if (slotMetadata != null) {
      activeRequestsMap.put(request, slotMetadata);
      return true;
    }

    return false;
  }

  /* For a rejected request type, do the following:
   * For each request rate limiter whose type that is not of the type of the request which got rejected,
   * check if slot borrowing is enabled. If enabled, try to acquire a slot.
   * If allotted, return else try next request type.
   *
   * @lucene.experimental -- Can cause slots to be blocked if a request borrows a slot and is itself long lived.
   */
  private RequestRateLimiter.SlotMetadata trySlotBorrowing(String requestType) {
    for (Map.Entry<String, RequestRateLimiter> currentEntry : requestRateLimiterMap.entrySet()) {
      RequestRateLimiter.SlotMetadata result = null;
      RequestRateLimiter requestRateLimiter = currentEntry.getValue();

      // Cant borrow from ourselves
      if (requestRateLimiter.getRateLimiterConfig().requestType.toString().equals(requestType)) {
        continue;
      }

      if (requestRateLimiter.getRateLimiterConfig().isSlotBorrowingEnabled) {
        if (log.isWarnEnabled()) {
          String msg = "WARN: Experimental feature slots borrowing is enabled for request rate limiter type " +
              requestRateLimiter.getRateLimiterConfig().requestType.toString();

          log.warn(msg);
        }

        try {
          result = requestRateLimiter.allowSlotBorrowing();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }

        if (result == null) {
          throw new IllegalStateException("Returned metadata object is null");
        }

        if (result.isReleasable()) {
          return result;
        }
      }
    }

    return null;
  }

  // Decrement the active requests in the rate limiter for the corresponding request type.
  public void decrementActiveRequests(HttpServletRequest request) {
    RequestRateLimiter.SlotMetadata slotMetadata = activeRequestsMap.get(request);

    if (slotMetadata != null) {
      activeRequestsMap.remove(request);
      slotMetadata.decrementRequest();
    }
  }

  public void registerRequestRateLimiter(RequestRateLimiter requestRateLimiter, SolrRequest.SolrRequestType requestType) {
    requestRateLimiterMap.put(requestType.toString(), requestRateLimiter);
  }

  public RequestRateLimiter getRequestRateLimiter(SolrRequest.SolrRequestType requestType) {
    return requestRateLimiterMap.get(requestType.toString());
  }

  public static class Builder {
    protected SolrZkClient solrZkClient;

    public Builder(SolrZkClient solrZkClient) {
      this.solrZkClient = solrZkClient;
    }

    public RateLimitManager build() {
      RateLimitManager rateLimitManager = new RateLimitManager();

      rateLimitManager.registerRequestRateLimiter(new QueryRateLimiter(solrZkClient), SolrRequest.SolrRequestType.QUERY);

      return rateLimitManager;
    }
  }
}
