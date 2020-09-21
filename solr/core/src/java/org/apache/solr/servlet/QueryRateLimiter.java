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

import java.util.Map;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import static org.apache.solr.servlet.RateLimiterConfig.RL_CONFIG_KEY;

/** Implementation of RequestRateLimiter specific to query request types. Most of the actual work is delegated
 *  to the parent class but specific configurations and parsing are handled by this class.
 */
public class QueryRateLimiter extends RequestRateLimiter {

  public QueryRateLimiter(SolrZkClient solrZkClient) {
    super(constructQueryRateLimiterConfig(solrZkClient));
  }

  @SuppressWarnings({"unchecked"})
  public void processConfigChange(Map<String, Object> properties) {
    RateLimiterConfig rateLimiterConfig = getRateLimiterConfig();
    Map<String, Object> propertiesMap = (Map<String, Object>) properties.get(RL_CONFIG_KEY);

    constructQueryRateLimiterConfigInternal(propertiesMap, rateLimiterConfig);
  }

  // To be used in initialization
  @SuppressWarnings({"unchecked"})
  private static RateLimiterConfig constructQueryRateLimiterConfig(SolrZkClient zkClient) {
    try {
      Map<String, Object> clusterPropsJson = (Map<String, Object>) Utils.fromJSON(zkClient.getData(ZkStateReader.CLUSTER_PROPS, null, new Stat(), true));
      Map<String, Object> propertiesMap = (Map<String, Object>) clusterPropsJson.get(RL_CONFIG_KEY);
      RateLimiterConfig rateLimiterConfig = new RateLimiterConfig(SolrRequest.SolrRequestType.QUERY);

      constructQueryRateLimiterConfigInternal(propertiesMap, rateLimiterConfig);

      return rateLimiterConfig;
    } catch (KeeperException.NoNodeException e) {
      return new RateLimiterConfig(SolrRequest.SolrRequestType.QUERY);
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException("Error reading cluster property", SolrZkClient.checkInterrupted(e));
    }
  }

  private static void constructQueryRateLimiterConfigInternal(Map<String, Object> propertiesMap, RateLimiterConfig rateLimiterConfig) {

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
}
