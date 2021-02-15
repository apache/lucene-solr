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

package org.apache.solr.cluster.placement.plugins;

import org.apache.solr.cluster.placement.PlacementPluginConfig;
import org.apache.solr.common.annotation.JsonProperty;

/**
 * Configuration bean for {@link AffinityPlacementFactory}.
 */
public class AffinityPlacementConfig implements PlacementPluginConfig {

  /**
   * Collection property that defines a comma-separate list of required node types.
   */
  public static final String COLLECTION_NODE_TYPE_PROPERTY = COLLECTION_PROPERTY_PREFIX + "affinity.node_type";
  /**
   * Collection property that defines the secondary collection name to be co-located with
   * this collection.
   */
  public static final String WITH_COLLECTION_PROPERTY = COLLECTION_PROPERTY_PREFIX + "affinity.withCollection";
  /**
   * <p>Name of the system property on a node indicating which (public cloud) Availability Zone that node is in. The value
   * is any string, different strings denote different availability zones.
   *
   * <p>Nodes on which this system property is not defined are considered being in the same Availability Zone
   * {@link #UNDEFINED_AVAILABILITY_ZONE} (hopefully the value of this constant is not the name of a real Availability Zone :).
   */
  public static final String AVAILABILITY_ZONE_SYSPROP = "availability_zone";
  /**
   * <p>Name of the system property on a node indicating the type of replicas allowed on that node.
   * The value of that system property is a comma-separated list or a single string of value names of
   * {@link org.apache.solr.cluster.Replica.ReplicaType} (case insensitive). If that property is not defined, that node is
   * considered accepting all replica types (i.e. undefined is equivalent to {@code "NRT,Pull,tlog"}).
   */
  public static final String REPLICA_TYPE_SYSPROP = "replica_type";
  /**
   * Name of the system property on a node indicating the arbitrary "node type" (for example, a node
   * more suitable for the indexing work load could be labeled as <code>node_type: indexing</code>).
   * The value of this system property is a comma-separated list or a single label (labels must not
   * contain commas), which represent a logical OR for the purpose of placement. Collections may
   * indicate the required node types using the {@link AffinityPlacementConfig#COLLECTION_NODE_TYPE_PROPERTY},
   * and replicas will be placed only on the nodes that match one of the types.
   */
  public static final String NODE_TYPE_SYSPROP = "node_type";
  /**
   * This is the "AZ" name for nodes that do not define an AZ. Should not match a real AZ name (I think we're safe)
   */
  public static final String UNDEFINED_AVAILABILITY_ZONE = "uNd3f1NeD";

  public static final long DEFAULT_MINIMAL_FREE_DISK_GB = 20L;
  public static final long DEFAULT_PRIORITIZED_FREE_DISK_GB = 100L;

  public static final AffinityPlacementConfig DEFAULT =
      new AffinityPlacementConfig(DEFAULT_MINIMAL_FREE_DISK_GB, DEFAULT_PRIORITIZED_FREE_DISK_GB);

  /**
   * If a node has strictly less GB of free disk than this value, the node is excluded from assignment decisions.
   * Set to 0 or less to disable.
   */
  @JsonProperty
  public long minimalFreeDiskGB;

  /**
   * Replica allocation will assign replicas to nodes with at least this number of GB of free disk space regardless
   * of the number of cores on these nodes rather than assigning replicas to nodes with less than this amount of free
   * disk space if that's an option (if that's not an option, replicas can still be assigned to nodes with less than this
   * amount of free space).
   */
  @JsonProperty
  public long prioritizedFreeDiskGB;

  /**
   * Zero-arguments public constructor required for deserialization - don't use.
   */
  public AffinityPlacementConfig() {
    this(DEFAULT_MINIMAL_FREE_DISK_GB, DEFAULT_PRIORITIZED_FREE_DISK_GB);
  }

  /**
   * Configuration for the {@link AffinityPlacementFactory}.
   * @param minimalFreeDiskGB minimal free disk GB.
   * @param prioritizedFreeDiskGB prioritized free disk GB.
   */
  public AffinityPlacementConfig(long minimalFreeDiskGB, long prioritizedFreeDiskGB) {
    this.minimalFreeDiskGB = minimalFreeDiskGB;
    this.prioritizedFreeDiskGB = prioritizedFreeDiskGB;
  }
}
