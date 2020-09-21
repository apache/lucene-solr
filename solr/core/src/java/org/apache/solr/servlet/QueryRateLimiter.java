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

import java.util.Map;

import org.apache.solr.client.solrj.SolrRequest;

import static org.apache.solr.servlet.RateLimitManager.DEFAULT_CONCURRENT_REQUESTS;
import static org.apache.solr.servlet.RateLimitManager.DEFAULT_SLOT_ACQUISITION_TIMEOUT_MS;
import static org.apache.solr.servlet.RateLimiterConfig.RL_CONFIG_KEY;

/** Implementation of RequestRateLimiter specific to query request types. Most of the actual work is delegated
 *  to the parent class but specific configurations and parsing are handled by this class.
 */
public class QueryRateLimiter extends RequestRateLimiter {
  final static String IS_QUERY_RATE_LIMITER_ENABLED = "isQueryRateLimiterEnabled";
  final static String MAX_QUERY_REQUESTS = "maxQueryRequests";
  final static String QUERY_WAIT_FOR_SLOT_ALLOCATION_INMS = "queryWaitForSlotAllocationInMS";
  final static String QUERY_GUARANTEED_SLOTS = "queryGuaranteedSlots";
  final static String QUERY_ALLOW_SLOT_BORROWING = "queryAllowSlotBorrowing";

  public QueryRateLimiter(FilterConfig filterConfig) {
    super(constructQueryRateLimiterConfig(filterConfig));
  }

  public void processConfigChange(Map<String, Object> properties) {
    RateLimiterConfig rateLimiterConfig = getRateLimiterConfig();
    Map<String, Object> propertiesMap = (Map<String, Object>) properties.get(RL_CONFIG_KEY);

    if (propertiesMap.get(RateLimiterConfig.RL_ALLOWED_REQUESTS) != null) {
      rateLimiterConfig.allowedRequests = Integer.parseInt(propertiesMap.get(RateLimiterConfig.RL_ALLOWED_REQUESTS).toString());
    }

    if (propertiesMap.get(RateLimiterConfig.RL_ENABLED) != null) {
      rateLimiterConfig.isEnabled = Boolean.parseBoolean(propertiesMap.get(RateLimiterConfig.RL_ENABLED).toString());
    }

    if (propertiesMap.get(RateLimiterConfig.RL_GUARANTEED_SLOTS) != null) {
      rateLimiterConfig.guaranteedSlotsThreshold = Integer.parseInt(propertiesMap.get(RateLimiterConfig.RL_GUARANTEED_SLOTS).toString());
    }

    if (propertiesMap.get(RateLimiterConfig.RL_SLOT_BORROWING_ENABLED) != null) {
      rateLimiterConfig.isSlotBorrowingEnabled = Boolean.parseBoolean(propertiesMap.get(RateLimiterConfig.RL_SLOT_BORROWING_ENABLED).toString());
    }

    if (propertiesMap.get(RateLimiterConfig.RL_TIME_SLOT_ACQUISITION_INMS) != null) {
      rateLimiterConfig.waitForSlotAcquisition = Long.parseLong(propertiesMap.get(RateLimiterConfig.RL_TIME_SLOT_ACQUISITION_INMS).toString());
    }
  }

  protected static RateLimiterConfig constructQueryRateLimiterConfig(FilterConfig filterConfig) {
    RateLimiterConfig queryRateLimiterConfig = new RateLimiterConfig();

    queryRateLimiterConfig.requestType = SolrRequest.SolrRequestType.QUERY;
    queryRateLimiterConfig.isEnabled = getParamAndParseBoolean(filterConfig, IS_QUERY_RATE_LIMITER_ENABLED, false);
    queryRateLimiterConfig.waitForSlotAcquisition = getParamAndParseLong(filterConfig, QUERY_WAIT_FOR_SLOT_ALLOCATION_INMS,
        DEFAULT_SLOT_ACQUISITION_TIMEOUT_MS);
    queryRateLimiterConfig.allowedRequests = getParamAndParseInt(filterConfig, MAX_QUERY_REQUESTS,
        DEFAULT_CONCURRENT_REQUESTS);
    queryRateLimiterConfig.isSlotBorrowingEnabled = getParamAndParseBoolean(filterConfig, QUERY_ALLOW_SLOT_BORROWING, false);
    queryRateLimiterConfig.guaranteedSlotsThreshold = getParamAndParseInt(filterConfig, QUERY_GUARANTEED_SLOTS, queryRateLimiterConfig.allowedRequests / 2);

    return queryRateLimiterConfig;
  }
}
