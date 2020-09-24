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

package org.apache.solr.core;

import org.apache.solr.client.solrj.SolrRequest;

import static org.apache.solr.servlet.RateLimitManager.DEFAULT_CONCURRENT_REQUESTS;
import static org.apache.solr.servlet.RateLimitManager.DEFAULT_SLOT_ACQUISITION_TIMEOUT_MS;

public class RateLimiterConfig {
  public static final String RL_CONFIG_KEY = "rate-limiters";

  public SolrRequest.SolrRequestType requestType;
  public boolean isEnabled;
  public long waitForSlotAcquisition;
  public int allowedRequests;
  public boolean isSlotBorrowingEnabled;
  public int guaranteedSlotsThreshold;

  public RateLimiterConfig(SolrRequest.SolrRequestType requestType) {
    this.requestType = requestType;
    this.isEnabled = false;
    this.allowedRequests = DEFAULT_CONCURRENT_REQUESTS;
    this.isSlotBorrowingEnabled = false;
    this.guaranteedSlotsThreshold = this.allowedRequests / 2;
    this.waitForSlotAcquisition = DEFAULT_SLOT_ACQUISITION_TIMEOUT_MS;
  }

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
