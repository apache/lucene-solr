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

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.solr.common.annotation.SolrThreadSafe;
import org.apache.solr.core.RateLimiterConfig;

/**
 * Handles rate limiting for a specific request type.
 *
 * The control flow is as follows:
 * Handle request -- Check if slot is available -- If available, acquire slot and proceed --
 * else reject the same.
 */
@SolrThreadSafe
public class RequestRateLimiter {
  // Slots that are guaranteed for this request rate limiter.
  private final Semaphore guaranteedSlotsPool;

  // Competitive slots pool that are available for this rate limiter as well as borrowing by other request rate limiters.
  // By competitive, the meaning is that there is no prioritization for the acquisition of these slots -- First Come First Serve,
  // irrespective of whether the request is of this request rate limiter or other.
  private final Semaphore borrowableSlotsPool;

  private final RateLimiterConfig rateLimiterConfig;
  private final SlotMetadata guaranteedSlotMetadata;
  private final SlotMetadata borrowedSlotMetadata;
  private static final SlotMetadata nullSlotMetadata = new SlotMetadata(null);

  public RequestRateLimiter(RateLimiterConfig rateLimiterConfig) {
    this.rateLimiterConfig = rateLimiterConfig;
    this.guaranteedSlotsPool = new Semaphore(rateLimiterConfig.guaranteedSlotsThreshold);
    this.borrowableSlotsPool = new Semaphore(rateLimiterConfig.allowedRequests - rateLimiterConfig.guaranteedSlotsThreshold);
    this.guaranteedSlotMetadata = new SlotMetadata(guaranteedSlotsPool);
    this.borrowedSlotMetadata = new SlotMetadata(borrowableSlotsPool);
  }

  /**
   * Handles an incoming request. returns a metadata object representing the metadata for the acquired slot, if acquired.
   * If a slot is not acquired, returns a null metadata object.
   * */
  public SlotMetadata handleRequest() throws InterruptedException {

    if (!rateLimiterConfig.isEnabled) {
      return nullSlotMetadata;
    }

    if (guaranteedSlotsPool.tryAcquire(rateLimiterConfig.waitForSlotAcquisition, TimeUnit.MILLISECONDS)) {
      return guaranteedSlotMetadata;
    }

    if (borrowableSlotsPool.tryAcquire(rateLimiterConfig.waitForSlotAcquisition, TimeUnit.MILLISECONDS)) {
      return borrowedSlotMetadata;
    }

    return null;
  }

  /**
   * Whether to allow another request type to borrow a slot from this request rate limiter. Typically works fine
   * if there is a relatively lesser load on this request rate limiter's type compared to the others (think of skew).
   * @return returns a metadata object for the acquired slot, if acquired. If the
   * slot was not acquired, returns a metadata object with a null pool.
   *
   * @lucene.experimental -- Can cause slots to be blocked if a request borrows a slot and is itself long lived.
   */
  public SlotMetadata allowSlotBorrowing() throws InterruptedException {
    if (borrowableSlotsPool.tryAcquire(rateLimiterConfig.waitForSlotAcquisition, TimeUnit.MILLISECONDS)) {
      return borrowedSlotMetadata;
    }

    return nullSlotMetadata;
  }

  public RateLimiterConfig getRateLimiterConfig() {
    return rateLimiterConfig;
  }

  // Represents the metadata for a slot
  static class SlotMetadata {
    private final Semaphore usedPool;

    public SlotMetadata(Semaphore usedPool) {
      this.usedPool = usedPool;
    }

    public void decrementRequest() {
      if (usedPool != null) {
        usedPool.release();
      }
    }

    public boolean isReleasable() {
      return usedPool != null;
    }
  }
}
