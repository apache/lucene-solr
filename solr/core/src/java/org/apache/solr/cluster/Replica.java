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

package org.apache.solr.cluster;

/**
 * An instantiation (or one of the copies) of a given {@link Shard} of a given {@link SolrCollection}.
 */
public interface Replica {
  Shard getShard();

  ReplicaType getType();
  ReplicaState getState();

  String getReplicaName();

  /**
   * The core name on disk
   */
  String getCoreName();

  /**
   * {@link Node} on which this {@link Replica} is located.
   */
  Node getNode();

  /**
   * The order of this enum is important from the most to least "important" replica type.
   */
  enum ReplicaType {
    NRT('n'), TLOG('t'), PULL('p');

    private char suffixChar;

    ReplicaType(char suffixChar) {
      this.suffixChar = suffixChar;
    }

    public char getSuffixChar() {
      return suffixChar;
    }
  }

  enum ReplicaState {
    ACTIVE, DOWN, RECOVERING, RECOVERY_FAILED
  }
}
