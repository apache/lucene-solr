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
import org.apache.solr.common.util.Pair;

/**
 * Handles rate limiting for a specific request type.
 *
 * The control flow is as follows:
 * Handle request -- Check if slot is available -- If available, acquire slot and proceed --
 * else reject the same.
 */
public class RequestRateLimiter {
  // Slots that are guaranteed for this request rate limiter.
  private final Semaphore guaranteedSlotsPool;

  // Competitive slots pool that are available for this rate limiter as well as borrowing by other request rate limiters.
  // By competitive, the meaning is that there is no prioritization for the acquisition of these slots -- First Come First Serve,
  // irrespective of whether the request is of this request rate limiter or other.
  private final Semaphore borrowableSlotsPool;

  private final RateLimiterConfig rateLimiterConfig;

  public RequestRateLimiter(RateLimiterConfig rateLimiterConfig) {
    this.rateLimiterConfig = rateLimiterConfig;
    this.guaranteedSlotsPool = new Semaphore(rateLimiterConfig.guaranteedSlotsThreshold);
    this.borrowableSlotsPool = new Semaphore(rateLimiterConfig.allowedRequests - rateLimiterConfig.guaranteedSlotsThreshold);
  }

  /**
   * Handles an incoming request. Returns true if accepted, false if quota for this rate limiter is exceeded. Also returns
   * a metadata object representing the metadata for the acquired slot, if acquired. If a slot is not acquired, returns false
   * with a null metadata object.
   * NOTE: Always check for a null metadata object even if this method returns a true -- this will be the scenario when
   * rate limiters are not enabled.
   * */
  public Pair<Boolean, AcquiredSlotMetadata> handleRequest() throws InterruptedException {

    if (!rateLimiterConfig.isEnabled) {
      return new Pair(true, null);
    }

    if (guaranteedSlotsPool.tryAcquire(rateLimiterConfig.waitForSlotAcquisition, TimeUnit.MILLISECONDS)) {
      return new Pair<>(true, new AcquiredSlotMetadata(this, false));
    }

    if (borrowableSlotsPool.tryAcquire(rateLimiterConfig.waitForSlotAcquisition, TimeUnit.MILLISECONDS)) {
      return new Pair<>(true, new AcquiredSlotMetadata(this, true));
    }

    return new Pair<>(false, null);
  }

  /**
   * Whether to allow another request type to borrow a slot from this request rate limiter. Typically works fine
   * if there is a relatively lesser load on this request rate limiter's type compared to the others (think of skew).
   * @return true if allow, false otherwise. Also returns a metadata object for the acquired slot, if acquired. If the
   * slot was not acquired, returns a null metadata object.
   *
   * @lucene.experimental -- Can cause slots to be blocked if a request borrows a slot and is itself long lived.
   */
  public Pair<Boolean, AcquiredSlotMetadata> allowSlotBorrowing() throws InterruptedException {
    if (borrowableSlotsPool.tryAcquire(rateLimiterConfig.waitForSlotAcquisition, TimeUnit.MILLISECONDS)) {
      return new Pair<>(true, new AcquiredSlotMetadata(this, true));
    }

    return new Pair<>(false, null);
  }

  public void decrementConcurrentRequests(boolean isBorrowedSlot) {
    if (isBorrowedSlot) {
      borrowableSlotsPool.release();
      return;
    }

    guaranteedSlotsPool.release();
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

  // Represents the metadata for an acquired slot
  static class AcquiredSlotMetadata {
    public RequestRateLimiter requestRateLimiter;
    public boolean isBorrowedSlot;

    public AcquiredSlotMetadata(RequestRateLimiter requestRateLimiter, boolean isBorrowedSlot) {
      this.requestRateLimiter = requestRateLimiter;
      this.isBorrowedSlot = isBorrowedSlot;
    }
  }
}
