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
package org.apache.solr.cloud.overseer;

import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.PerReplicaStatesOps;

public class ZkWriteCommand {
  /**
   * Single NO_OP instance, can be compared with ==
   */
  static final ZkWriteCommand NO_OP = new ZkWriteCommand(null, null);

  public final String name;
  public final DocCollection collection;
  public final boolean isPerReplicaStateCollection;

  // persist the collection state. If this is false, it means the collection state is not modified
  public final boolean persistJsonState;
  public final PerReplicaStatesOps ops;

  public ZkWriteCommand(String name, DocCollection collection, PerReplicaStatesOps replicaOps, boolean persistJsonState) {
    isPerReplicaStateCollection = collection != null && collection.isPerReplicaState();
    this.name = name;
    this.collection = collection;
    this.ops = replicaOps;
    this.persistJsonState = persistJsonState || !isPerReplicaStateCollection; // Always persist for non "per replica state" collections
  }

  public ZkWriteCommand(String name, DocCollection collection) {
    this(name, collection, null, true);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + ": " + (this == NO_OP ? "no-op" : name + "=" + collection);
  }
}

