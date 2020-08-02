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

package org.apache.solr.cluster.placement;

/**
 * <p>Placement decision for a single {@link Replica}. Note this placement decision is used as part of a {@link WorkOrder},
 * it does not directly lead to the plugin code getting a corresponding {@link Replica} instance, nor does it require the
 * plugin to provide a {@link Shard} instance (the plugin code gets such instances for existing replicas and shard in the
 * cluster but does not create them directly for adding new replicas for new or existing shards).
 *
 * <p>Captures the {@link Shard} (via the shard name), {@link Node} and {@link Replica.ReplicaType} of a Replica to be created.
 */
public interface ReplicaPlacement {
}
