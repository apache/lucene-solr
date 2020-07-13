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

public class RateLimitManager {
  public final static int DEFAULT_CONCURRENT_REQUESTS= 10;
  public final static int DEFAULT_SUSPEND_TIME_INMS = 50;
  public final static long DEFAULT_TIMEOUT_MS = -1;

  private final String _suspended;

  private final Map<String, RequestRateLimiter> requestRateLimiterMap;

  public RateLimitManager(String _suspended) {
    this.requestRateLimiterMap = new HashMap();

    this._suspended = _suspended;
  }

  public boolean handleRequest(HttpServletRequest request) throws InterruptedException {
    String requestContext = request.getHeader(SOLR_REQUEST_CONTEXT_PARAM);
    String typeOfRequest = request.getHeader(SOLR_REQUEST_TYPE_PARAM);

    // Do not throttle internal requests
    if (requestContext.equals(SolrRequest.SolrClientContext.SERVER.toString())) {
      return true;
    }

    RequestRateLimiter requestRateLimiter = requestRateLimiterMap.get(typeOfRequest);

    if (requestRateLimiter == null) {
      // No request rate limiter for this request type
      return true;
    }

    Boolean suspended = (Boolean) request.getAttribute(_suspended);
    boolean accepted;

    if (suspended == null) {
      accepted = requestRateLimiter.handleNewRequest(request);
    } else {
      accepted = requestRateLimiter.handleSuspendedRequest(request);
    }

    return accepted;
  }

  // TODO: This should be a priority queue based model
  public void resumePendingRequest() {

    for (Map.Entry<String, RequestRateLimiter> currentEntry : requestRateLimiterMap.entrySet()) {
      RequestRateLimiter requestRateLimiter = currentEntry.getValue();
      boolean isRequestResumed = requestRateLimiter.resumePendingOperation();

      if (isRequestResumed) {
        return;
      }
    }
  }

  public void decrementActiveRequests(HttpServletRequest request) {
    String typeOfRequest = request.getHeader(SOLR_REQUEST_TYPE_PARAM);

    RequestRateLimiter requestRateLimiter = requestRateLimiterMap.get(typeOfRequest);

    if (requestRateLimiter == null) {
      // No rate limiter for this request type
      return;
    }

    requestRateLimiter.decrementConcurrentRequests();
  }

  public void registerRequestRateLimiter(RequestRateLimiter requestRateLimiter, SolrRequest.SolrRequestType requestType) {
    requestRateLimiterMap.put(requestType.toString(), requestRateLimiter);
  }

  public RequestRateLimiter getRequestRateLimiter(SolrRequest.SolrRequestType requestType) {
    return requestRateLimiterMap.get(requestType);
  }

  public static RateLimitManager buildRateLimitManager(FilterConfig config, String _resumed, String _suspended) {
    RateLimitManager rateLimitManager = new RateLimitManager(_suspended);

    rateLimitManager.registerRequestRateLimiter(new IndexRateLimiter(config, _suspended, _resumed), SolrRequest.SolrRequestType.UPDATE);
    rateLimitManager.registerRequestRateLimiter(new QueryRateLimiter(config, _suspended, _resumed), SolrRequest.SolrRequestType.QUERY);

    return rateLimitManager;
  }
}
