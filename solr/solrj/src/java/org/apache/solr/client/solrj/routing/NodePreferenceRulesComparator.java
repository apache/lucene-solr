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

package org.apache.solr.client.solrj.routing;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.StringUtils;
import org.apache.solr.common.cloud.NodesSysPropsCacher;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;

/**
   * This comparator makes sure that the given replicas are sorted according to the given list of preferences.
   * E.g. If all nodes prefer local cores then a bad/heavily-loaded node will receive less requests from
   * healthy nodes. This will help prevent a distributed deadlock or timeouts in all the healthy nodes due
   * to one bad node.
   *
   * Optional final preferenceRule is *not* used for pairwise sorting, but instead defines how "equivalent"
   * replicas will be ordered (the base ordering). Defaults to "random"; may specify "stable".
   */
public class NodePreferenceRulesComparator implements Comparator<Object> {

  private final NodesSysPropsCacher sysPropsCache;
  private final String nodeName;
  private final List<PreferenceRule> sortRules;
  private final List<PreferenceRule> preferenceRules;
  private final String localHostAddress;
  private final ReplicaListTransformer baseReplicaListTransformer;

  public NodePreferenceRulesComparator(final List<PreferenceRule> preferenceRules, final SolrParams requestParams,
  final ReplicaListTransformerFactory defaultRltFactory, final ReplicaListTransformerFactory stableRltFactory) {
    this(preferenceRules, requestParams, null, null, null, defaultRltFactory, stableRltFactory);
  }

  public NodePreferenceRulesComparator(final List<PreferenceRule> preferenceRules, final SolrParams requestParams,
      final String nodeName, final String localHostAddress, final NodesSysPropsCacher sysPropsCache,
      final ReplicaListTransformerFactory defaultRltFactory, final ReplicaListTransformerFactory stableRltFactory) {
    this.sysPropsCache = sysPropsCache;
    this.preferenceRules = preferenceRules;
    this.nodeName = nodeName;
    this.localHostAddress = localHostAddress;
    final int maxIdx = preferenceRules.size() - 1;
    final PreferenceRule lastRule = preferenceRules.get(maxIdx);
    if (!ShardParams.SHARDS_PREFERENCE_REPLICA_BASE.equals(lastRule.name)) {
      this.sortRules = preferenceRules;
      this.baseReplicaListTransformer = defaultRltFactory.getInstance(null, requestParams, RequestReplicaListTransformerGenerator.RANDOM_RLTF);
    } else {
      if (maxIdx == 0) {
        this.sortRules = null;
      } else {
        this.sortRules = preferenceRules.subList(0, maxIdx);
      }
      String[] parts = lastRule.value.split(":", 2);
      switch (parts[0]) {
        case ShardParams.REPLICA_RANDOM:
          this.baseReplicaListTransformer = RequestReplicaListTransformerGenerator.RANDOM_RLTF.getInstance(parts.length == 1 ? null : parts[1], requestParams, null);
          break;
        case ShardParams.REPLICA_STABLE:
          this.baseReplicaListTransformer = stableRltFactory.getInstance(parts.length == 1 ? null : parts[1], requestParams, RequestReplicaListTransformerGenerator.RANDOM_RLTF);
          break;
        default:
          throw new IllegalArgumentException("Invalid base replica order spec");
      }
    }
  }
  private static final ReplicaListTransformer NOOP_RLT = (List<?> choices) -> { /* noop */ };
  private static final ReplicaListTransformerFactory NOOP_RLTF =
      (String configSpec, SolrParams requestParams, ReplicaListTransformerFactory fallback) -> NOOP_RLT;
  /**
   * For compatibility with tests, which expect this constructor to have no effect on the *base* order.
   */
  NodePreferenceRulesComparator(final List<PreferenceRule> sortRules, final SolrParams requestParams) {
    this(sortRules, requestParams, NOOP_RLTF, null);
  }

  public ReplicaListTransformer getBaseReplicaListTransformer() {
    return baseReplicaListTransformer;
  }

  @Override
  public int compare(Object left, Object right) {
    if (this.sortRules != null) {
      for (PreferenceRule preferenceRule: this.sortRules) {
        final boolean lhs;
        final boolean rhs;
        switch (preferenceRule.name) {
          case ShardParams.SHARDS_PREFERENCE_REPLICA_TYPE:
            lhs = hasReplicaType(left, preferenceRule.value);
            rhs = hasReplicaType(right, preferenceRule.value);
            break;
          case ShardParams.SHARDS_PREFERENCE_REPLICA_LOCATION:
            lhs = hasCoreUrlPrefix(left, preferenceRule.value);
            rhs = hasCoreUrlPrefix(right, preferenceRule.value);
            break;
          case ShardParams.SHARDS_PREFERENCE_REPLICA_LEADER:
            lhs = hasLeaderStatus(left, preferenceRule.value);
            rhs = hasLeaderStatus(right, preferenceRule.value);
            break;
          case ShardParams.SHARDS_PREFERENCE_NODE_WITH_SAME_SYSPROP:
            if (sysPropsCache == null) {
              throw new IllegalArgumentException("Unable to get the NodesSysPropsCacher on sorting replicas by preference:"+ preferenceRule.value);
            }
            lhs = hasSameMetric(left, preferenceRule.value);
            rhs = hasSameMetric(right, preferenceRule.value);
            break;
          case ShardParams.SHARDS_PREFERENCE_REPLICA_BASE:
            throw new IllegalArgumentException("only one base replica order may be specified in "
                + ShardParams.SHARDS_PREFERENCE + ", and it must be specified last");
          default:
            throw new IllegalArgumentException("Invalid " + ShardParams.SHARDS_PREFERENCE + " type: " + preferenceRule.name);
        }
        if (lhs != rhs) {
          return lhs ? -1 : +1;
        }
      }
    }
    return 0;
  }

  private boolean hasSameMetric(Object o, String metricTag) {
    if (!(o instanceof Replica)) {
      return false;
    }

    Collection<String> tags = Collections.singletonList(metricTag);
    String otherNodeName = ((Replica) o).getNodeName();
    Map<String, Object> currentNodeMetric = sysPropsCache.getSysProps(nodeName, tags);
    Map<String, Object> otherNodeMetric = sysPropsCache.getSysProps(otherNodeName, tags);
    return currentNodeMetric.equals(otherNodeMetric);
  }

  private boolean hasCoreUrlPrefix(Object o, String prefix) {
    final String s;
    if (o instanceof String) {
      s = (String)o;
    }
    else if (o instanceof Replica) {
      s = ((Replica)o).getCoreUrl();
    } else {
      return false;
    }
    if (prefix.equals(ShardParams.REPLICA_LOCAL)) {
      return !StringUtils.isEmpty(localHostAddress) && s.startsWith(localHostAddress);
    } else {
      return s.startsWith(prefix);
    }
  }

  private static boolean hasReplicaType(Object o, String preferred) {
    if (!(o instanceof Replica)) {
      return false;
    }
    final String s = ((Replica)o).getType().toString();
    return s.equalsIgnoreCase(preferred);
  }

  private static boolean hasLeaderStatus(Object o, String status) {
    if (!(o instanceof Replica)) {
      return false;
    }
    final boolean leaderStatus = ((Replica) o).isLeader();
    return leaderStatus == Boolean.parseBoolean(status);
  }

  public List<PreferenceRule> getSortRules() {
    return sortRules;
  }

  public List<PreferenceRule> getPreferenceRules() {
    return preferenceRules;
  }
}