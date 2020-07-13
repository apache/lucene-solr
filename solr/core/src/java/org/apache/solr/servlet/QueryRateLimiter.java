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

import static org.apache.solr.servlet.RateLimitManager.DEFAULT_CONCURRENT_REQUESTS;
import static org.apache.solr.servlet.RateLimitManager.DEFAULT_SUSPEND_TIME_INMS;
import static org.apache.solr.servlet.RateLimitManager.DEFAULT_TIMEOUT_MS;

public class QueryRateLimiter extends RequestRateLimiter {
  final static String MAX_QUERY_REQUESTS = "maxQueryRequests";
  final static String QUERY_WAIT_FOR_SLOT_ALLOCATION_INMS = "queryWaitForSlotAllocationInMS";
  final static String QUERY_REQUEST_SUSPEND_TIME_INMS = "queryRequestSuspendTimeInMS";

  public QueryRateLimiter(FilterConfig filterConfig, String _suspended,
                          String _resumed) {
    super(constructIndexRateLimiterConfig(filterConfig, _suspended, _resumed));
  }

  protected static RequestRateLimiter.RateLimiterConfig constructIndexRateLimiterConfig(FilterConfig filterConfig, String _suspended,
                                                                                        String _resumed) {
    RequestRateLimiter.RateLimiterConfig indexRateLimiterConfig = new RequestRateLimiter.RateLimiterConfig();

    indexRateLimiterConfig.requestSuspendTimeInMS = getParamAndParseLong(filterConfig, QUERY_REQUEST_SUSPEND_TIME_INMS,
        DEFAULT_SUSPEND_TIME_INMS);
    indexRateLimiterConfig.waitForSlotAcquisition = getParamAndParseLong(filterConfig, QUERY_WAIT_FOR_SLOT_ALLOCATION_INMS,
        DEFAULT_TIMEOUT_MS);
    indexRateLimiterConfig.allowedRequests = getParamAndParseInt(filterConfig, MAX_QUERY_REQUESTS,
        DEFAULT_CONCURRENT_REQUESTS);
    indexRateLimiterConfig._suspended = _suspended;
    indexRateLimiterConfig._resumed = _resumed;

    return indexRateLimiterConfig;
  }
}
