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
package org.apache.solr.common.cloud;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.common.util.Utils;

/**
 * Used for routing docs with particular keys into another collection
 */
public class RoutingRule extends ZkNodeProps {
  private final List<DocRouter.Range> routeRanges;
  private final String routeRangesStr;
  private final String targetCollectionName;
  private final Long expireAt;

  public RoutingRule(String routeKey, Map<String, Object> propMap)  {
    super(propMap);
    this.routeRangesStr = (String) propMap.get("routeRanges");
    String[] rangesArr = this.routeRangesStr.split(",");
    if (rangesArr != null && rangesArr.length > 0)  {
      this.routeRanges = new ArrayList<>();
      for (String r : rangesArr) {
        routeRanges.add(DocRouter.DEFAULT.fromString(r));
      }
    } else  {
      this.routeRanges = null;
    }
    this.targetCollectionName = (String) propMap.get("targetCollection");
    this.expireAt = Long.parseLong((String) propMap.get("expireAt"));
  }

  public List<DocRouter.Range> getRouteRanges() {
    return routeRanges;
  }

  public String getTargetCollectionName() {
    return targetCollectionName;
  }

  @SuppressForbidden(reason = "For currentTimeMillis, expiry time depends on external data (should it?)")
  public static String makeExpiryAt(long timeMsFromNow) {
    return String.valueOf(System.currentTimeMillis() + timeMsFromNow);
  }

  @SuppressForbidden(reason = "For currentTimeMillis, expiry time depends on external data (should it?)")
  public boolean isExpired() {
    return (expireAt < System.currentTimeMillis());
  }

  public String getRouteRangesStr() {
    return routeRangesStr;
  }

  @Override
  public String toString() {
    return Utils.toJSONString(propMap);
  }
}
