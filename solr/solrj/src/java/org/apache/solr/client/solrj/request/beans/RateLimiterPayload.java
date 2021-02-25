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

package org.apache.solr.client.solrj.request.beans;

import java.util.Objects;

import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.util.ReflectMapWriter;

/**
 * POJO for Rate Limiter Metadata Configuration
 */
public class RateLimiterPayload implements ReflectMapWriter {
  @JsonProperty
  public Boolean enabled;

  @JsonProperty
  public Integer guaranteedSlots;

  @JsonProperty
  public Integer allowedRequests;

  @JsonProperty
  public Boolean slotBorrowingEnabled;

  @JsonProperty
  public Integer slotAcquisitionTimeoutInMS;

  public RateLimiterPayload copy() {
    RateLimiterPayload result = new RateLimiterPayload();

    result.enabled = enabled;
    result.guaranteedSlots = guaranteedSlots;
    result.allowedRequests = allowedRequests;
    result.slotBorrowingEnabled = slotBorrowingEnabled;
    result.slotAcquisitionTimeoutInMS = slotAcquisitionTimeoutInMS;

    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof RateLimiterPayload) {
      RateLimiterPayload that = (RateLimiterPayload) obj;
      return Objects.equals(this.enabled, that.enabled) &&
          Objects.equals(this.guaranteedSlots, that.guaranteedSlots) &&
          Objects.equals(this.allowedRequests, that.allowedRequests) &&
          Objects.equals(this.slotBorrowingEnabled, that.slotBorrowingEnabled) &&
          Objects.equals(this.slotAcquisitionTimeoutInMS, that.slotAcquisitionTimeoutInMS);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(enabled, guaranteedSlots, allowedRequests, slotBorrowingEnabled, slotAcquisitionTimeoutInMS);
  }
}
