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
package org.apache.solr.cloud.api.collections.assign;

import java.util.Locale;

/**
 *
 */
public enum ReplicaState {

  /**
   * The replica is ready to receive updates and queries.
   */
  ACTIVE,

  /**
   * The first state before {@link ReplicaState#RECOVERING}. A node in this state
   * should be actively trying to move to {@link ReplicaState#RECOVERING}.
   */
  DOWN,

  /**
   * The node is recovering from the leader. This might involve peer-sync,
   * full replication or finding out things are already in sync.
   */
  RECOVERING,

  /**
   * Recovery attempts have not worked, something is not right.
   * <p>
   * <b>NOTE</b>: This state doesn't matter if the node is not part of
   * {@code /live_nodes} in ZK; in that case the node is not part of the
   * cluster and it's state should be discarded.
   * </p>
   */
  RECOVERY_FAILED;

  @Override
  public String toString() {
    return super.toString().toLowerCase(Locale.ROOT);
  }

  /** Converts the state string to a State instance. */
  public static ReplicaState get(String stateStr) {
    return stateStr == null ? null : ReplicaState.valueOf(stateStr.toUpperCase(Locale.ROOT));
  }
}
