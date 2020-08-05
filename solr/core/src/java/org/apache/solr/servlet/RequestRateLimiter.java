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
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.SolrRequest;

/**
 * Handles rate limiting for a specific request type.
 *
 * The control flow is as follows:
 * Handle request -- Check if slot is available -- If available, acquire slot and proceed --
 * else reject the same.
 */
public class RequestRateLimiter {
  private final Semaphore allowedConcurrentRequests;
  private final RateLimiterConfig rateLimiterConfig;

  public RequestRateLimiter(RateLimiterConfig rateLimiterConfig) {
    this.rateLimiterConfig = rateLimiterConfig;
    this.allowedConcurrentRequests = new Semaphore(rateLimiterConfig.allowedRequests);
  }

  /* Handles an incoming request. Returns true if accepted, false if quota for this rate limiter is exceeded */
  public boolean handleRequest() throws InterruptedException {

    if (!rateLimiterConfig.isEnabled) {
      return true;
    }

    return allowedConcurrentRequests.tryAcquire(rateLimiterConfig.waitForSlotAcquisition, TimeUnit.MILLISECONDS);
  }

  /**
   * Whether to allow another request type to borrow a slot from this request rate limiter. Typically works fine
   * if there is a relatively lesser load on this request rate limiter's type compared to the others (think of skew).
   * @return true if allow, false otherwise
   *
   * @lucene.experimental -- Can cause slots to be blocked if a request borrows a slot and is itself long lived.
   */
  public boolean allowSlotBorrowing() {
    synchronized (this) {
      if (allowedConcurrentRequests.availablePermits() > rateLimiterConfig.guaranteedSlotsThreshold) {
        try {
          allowedConcurrentRequests.acquire();
        } catch (InterruptedException e) {
          throw new RuntimeException(e.getMessage());
        }
        return true;
      }
    }

    return false;
  }

  public void decrementConcurrentRequests() {
    allowedConcurrentRequests.release();
  }

  public RateLimiterConfig getRateLimiterConfig() {
    return rateLimiterConfig;
  }

  static long getParamAndParseLong(FilterConfig config, String parameterName, long defaultValue) {
    String tempBuffer = config.getInitParameter(parameterName);

    if (tempBuffer != null) {
      return Long.parseLong(tempBuffer);
    }

    return defaultValue;
  }

  static int getParamAndParseInt(FilterConfig config, String parameterName, int defaultValue) {
    String tempBuffer = config.getInitParameter(parameterName);

    if (tempBuffer != null) {
      return Integer.parseInt(tempBuffer);
    }

    return defaultValue;
  }

  static boolean getParamAndParseBoolean(FilterConfig config, String parameterName, boolean defaultValue) {
    String tempBuffer = config.getInitParameter(parameterName);

    if (tempBuffer != null) {
      return Boolean.parseBoolean(tempBuffer);
    }

    return defaultValue;
  }

  /* Rate limiter config for a specific request rate limiter instance */
  static class RateLimiterConfig {
    public SolrRequest.SolrRequestType requestType;
    public boolean isEnabled;
    public long waitForSlotAcquisition;
    public int allowedRequests;
    public boolean isSlotBorrowingEnabled;
    public int guaranteedSlotsThreshold;

    public RateLimiterConfig() { }

    public RateLimiterConfig(SolrRequest.SolrRequestType requestType, boolean isEnabled, int guaranteedSlotsThreshold,
                             long waitForSlotAcquisition, int allowedRequests, boolean isSlotBorrowingEnabled) {
      this.requestType = requestType;
      this.isEnabled = isEnabled;
      this.guaranteedSlotsThreshold = guaranteedSlotsThreshold;
      this.waitForSlotAcquisition = waitForSlotAcquisition;
      this.allowedRequests = allowedRequests;
      this.isSlotBorrowingEnabled = isSlotBorrowingEnabled;
    }
  }
}
