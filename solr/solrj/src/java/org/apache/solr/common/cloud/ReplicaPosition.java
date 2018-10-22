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


public class ReplicaPosition implements Comparable<ReplicaPosition> {
  public final String shard;
  public final int index;
  public final Replica.Type type;
  public String node;

  public ReplicaPosition(String shard, int replicaIdx, Replica.Type type) {
    this.shard = shard;
    this.index = replicaIdx;
    this.type = type;
  }
  public ReplicaPosition(String shard, int replicaIdx, Replica.Type type, String node) {
    this.shard = shard;
    this.index = replicaIdx;
    this.type = type;
    this.node = node;
  }

  @Override
  public int compareTo(ReplicaPosition that) {
    //this is to ensure that we try one replica from each shard first instead of
    // all replicas from same shard
    return Integer.compare(index, that.index);
  }

  @Override
  public String toString() {
    return shard + ":" + index + "["+ type +"] @" + node;
  }

  public ReplicaPosition setNode(String node) {
    this.node = node;
    return this;
  }
}
